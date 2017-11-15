//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package n1ql

import (
	"bytes"
	"crypto/tls"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/godbc"
	"github.com/couchbase/query/util"
)

// Common error codes
var (
	ErrNotSupported   = fmt.Errorf("N1QL:Not supported")
	ErrNotImplemented = fmt.Errorf("N1QL: Not implemented")
	ErrUnknownCommand = fmt.Errorf("N1QL: Unknown Command")
	ErrInternalError  = fmt.Errorf("N1QL: Internal Error")
)

// defaults
var (
	N1QL_SERVICE_ENDPOINT  = "/query/service"
	N1QL_DEFAULT_HOST      = "127.0.0.1"
	N1QL_DEFAULT_PORT      = 8093
	N1QL_POOL_SIZE         = 2 ^ 10 // 1 MB
	N1QL_DEFAULT_STATEMENT = "SELECT RAW 1;"
)

// flags

var (
	N1QL_PASSTHROUGH_MODE = false
)

// Rest API query parameters
var QueryParams map[string]string

// Username and password. Used for querying the cluster endpoint,
// which may require authorization.
var username, password string

// Used to decide whether to skip verification of certificates when
// connecting to an ssl port.
var skipVerify = true

func init() {
	QueryParams = make(map[string]string)
}

func SetQueryParams(key string, value string) error {

	if key == "" {
		return fmt.Errorf("N1QL: Key not specified")
	}

	QueryParams[key] = value
	return nil
}

func UnsetQueryParams(key string) error {

	if key == "" {
		return fmt.Errorf("N1QL: Key not specified")
	}

	delete(QueryParams, key)
	return nil
}

func SetPassthroughMode(val bool) {
	N1QL_PASSTHROUGH_MODE = val
}

func SetUsernamePassword(u, p string) {
	username = u
	password = p
}

func hasUsernamePassword() bool {
	return username != "" || password != ""
}

func SetSkipVerify(skip bool) {
	skipVerify = skip
}

// implements driver.Conn interface
type n1qlConn struct {
	clusterAddr string
	queryAPIs   []string
	client      *http.Client
	lock        sync.RWMutex
}

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 10
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

func discoverN1QLService(name string, ps couchbase.PoolServices) string {

	for _, ns := range ps.NodesExt {
		if ns.Services != nil {
			if port, ok := ns.Services["n1ql"]; ok == true {
				var hostname string
				//n1ql service found
				var ipv6 = false
				if ns.Hostname == "" {
					hostnm := strings.TrimSpace(name)
					if strings.HasPrefix(hostnm, "http://") || strings.HasPrefix(hostnm, "https://") {
						hostUrl, _ := url.Parse(name)
						hostnm = hostUrl.Host
					}

					hostname, _, ipv6, _ = HostNameandPort(hostnm)

				} else {
					hostname = ns.Hostname
				}

				if ipv6 {
					return fmt.Sprintf("[%s]:%d", hostname, port)
				} else {
					return fmt.Sprintf("%s:%d", hostname, port)
				}

			}
		}
	}
	return ""
}

func setCBUserAgent(request *http.Request) {
	request.Header.Add("CB-User-Agent", "godbc/"+util.VERSION)
}

func getQueryApi(n1qlEndPoint string) ([]string, error) {
	queryAdmin := "http://" + n1qlEndPoint + "/admin/clusters/default/nodes"
	request, _ := http.NewRequest("GET", queryAdmin, nil)
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	setCBUserAgent(request)
	if hasUsernamePassword() {
		request.SetBasicAuth(username, password)
	}
	queryAPIs := make([]string, 0)

	hostname, _, ipv6, err := HostNameandPort(n1qlEndPoint)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse URL. Error %v", err)
	}

	resp, err := HTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%v", err)
	}

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s", bod)
	}

	var nodesInfo []interface{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
	}

	if err := json.Unmarshal(body, &nodesInfo); err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
	}

	for _, queryNode := range nodesInfo {
		switch queryNode := queryNode.(type) {
		case map[string]interface{}:
			queryAPIs = append(queryAPIs, queryNode["queryEndpoint"].(string))
		}
	}

	localhost := "127.0.0.1"

	if ipv6 {
		hostname = "[" + hostname + "]"
		localhost = "[::1]"
	}

	// if the end-points contain localhost IPv4 or IPv6 then replace them with the actual hostname
	for i, qa := range queryAPIs {
		queryAPIs[i] = strings.Replace(qa, localhost, hostname, -1)
	}

	if len(queryAPIs) == 0 {
		return nil, fmt.Errorf("Query endpoints not found")
	}

	return queryAPIs, nil
}

func OpenN1QLConnection(name string) (*n1qlConn, error) {
	var queryAPIs []string

	if strings.HasPrefix(name, "https") && skipVerify {
		HTTPTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	//First check if the input string is a cluster endpoint
	couchbase.SetSkipVerify(skipVerify)

	var client couchbase.Client
	var err error
	if hasUsernamePassword() {
		client, err = couchbase.ConnectWithAuthCreds(name, username, password)
	} else {
		client, err = couchbase.Connect(name)
	}
	var perr error = nil
	if err != nil {
		perr = fmt.Errorf("N1QL: Unable to connect to cluster endpoint %s. Error %v", name, err)
		// If not cluster endpoint then check if query endpoint
		name = strings.TrimSuffix(name, "/")
		queryAPI := name + N1QL_SERVICE_ENDPOINT
		queryAPIs = make([]string, 1, 1)
		queryAPIs[0] = queryAPI

	} else {
		ps, err := client.GetPoolServices("default")
		if err != nil {
			return nil, fmt.Errorf("N1QL: Failed to get NodeServices list. Error %v", err)
		}

		n1qlEndPoint := discoverN1QLService(name, ps)
		if n1qlEndPoint == "" {
			return nil, fmt.Errorf("N1QL: No query service found on this cluster")
		}

		queryAPIs, err = getQueryApi(n1qlEndPoint)
		if err != nil {
			return nil, err
		}

	}

	conn := &n1qlConn{client: HTTPClient, queryAPIs: queryAPIs}

	request, err := prepareRequest(N1QL_DEFAULT_STATEMENT, queryAPIs[0], nil)
	if err != nil {
		return nil, err
	}

	resp, err := conn.client.Do(request)
	if err != nil {
		final_error := fmt.Errorf("N1QL: Connection failed %v", stripurl(err.Error())).Error()
		if perr != nil {
			final_error = final_error + "\n " + stripurl(perr.Error())
		}
		return nil, fmt.Errorf("%v", final_error)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("N1QL: Connection failure %s", bod)
	}

	return conn, nil
}

func stripurl(inputstring string) string {
	// Detect http* within the string.
	startindex := strings.Index(inputstring, "http")
	endindex := strings.Index(inputstring[startindex:], " ")
	inputurl := inputstring[startindex : startindex+endindex]

	// Parse into a url and detect password
	urlpart, err := url.Parse(inputurl)
	if err != nil {
		return inputstring
	}

	u := urlpart.User
	if u == nil {
		return inputstring
	}

	uname := u.Username()
	pwd, _ := u.Password()

	//Find how many symbols there are in the User string
	num := 0

	for _, letter := range fmt.Sprintf("%v", pwd) {
		if (unicode.IsSymbol(letter) || unicode.IsPunct(letter)) && letter != '*' {
			num = num + 1
		}
	}

	// detect the index on the password
	startindex = strings.Index(inputstring, uname)

	//reform the error message, with * as the password
	inputstring = inputstring[:startindex+len(uname)+1] + "*" + inputstring[startindex+len(uname)+1+len(pwd):]

	//Replace all the special characters encoding
	for num > 0 {
		num = num - 1
		inputstring = stripurl(inputstring)
	}

	return inputstring
}

// do client request with retry
func (conn *n1qlConn) doClientRequest(query string, requestValues *url.Values) (*http.Response, error) {

	ok := false
	for !ok {

		var request *http.Request
		var err error

		// select query API
		rand.Seed(time.Now().Unix())
		numNodes := len(conn.queryAPIs)

		selectedNode := rand.Intn(numNodes)
		conn.lock.RLock()
		queryAPI := conn.queryAPIs[selectedNode]
		conn.lock.RUnlock()

		if query != "" {
			request, err = prepareRequest(query, queryAPI, nil)
			if err != nil {
				return nil, err
			}
		} else {
			if requestValues != nil {
				request, _ = http.NewRequest("POST", queryAPI, bytes.NewBufferString(requestValues.Encode()))
			} else {
				request, _ = http.NewRequest("POST", queryAPI, nil)
			}
			request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			setCBUserAgent(request)
			if hasUsernamePassword() {
				request.SetBasicAuth(username, password)
			}
		}

		resp, err := conn.client.Do(request)
		if err != nil {
			// if this is the last node return with error
			if numNodes == 1 {
				break
			}
			// remove the node that failed from the list of query nodes
			conn.lock.Lock()
			conn.queryAPIs = append(conn.queryAPIs[:selectedNode], conn.queryAPIs[selectedNode+1:]...)
			conn.lock.Unlock()
			continue
		} else {
			return resp, nil
		}
	}

	return nil, fmt.Errorf("N1QL: Query nodes not responding")
}

func serializeErrors(errors interface{}) string {

	var errString string
	switch errors := errors.(type) {
	case []interface{}:
		for _, e := range errors {
			switch e := e.(type) {
			case map[string]interface{}:
				code, _ := e["code"]
				msg, _ := e["msg"]

				if code != 0 && msg != "" {
					if errString != "" {
						errString = fmt.Sprintf("%v Code : %v Message : %v", errString, code, msg)
					} else {
						errString = fmt.Sprintf("Code : %v Message : %v", code, msg)
					}
				}
			}
		}
	}

	if errString != "" {
		return errString
	}
	return fmt.Sprintf(" Error %v %T", errors, errors)
}

func (conn *n1qlConn) Prepare(query string) (*n1qlStmt, error) {
	var argCount int

	query = "PREPARE " + query
	query, argCount = prepareQuery(query)

	resp, err := conn.doClientRequest(query, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s", bod)
	}

	var resultMap map[string]*json.RawMessage
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
	}

	if err := json.Unmarshal(body, &resultMap); err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
	}

	stmt := &n1qlStmt{conn: conn, argCount: argCount}

	errors, ok := resultMap["errors"]
	if ok && errors != nil {
		var errs []interface{}
		_ = json.Unmarshal(*errors, &errs)
		return nil, fmt.Errorf("N1QL: Error preparing statement %v", serializeErrors(errs))
	}

	for name, results := range resultMap {
		switch name {
		case "results":
			var preparedResults []interface{}
			if err := json.Unmarshal(*results, &preparedResults); err != nil {
				return nil, fmt.Errorf("N1QL: Failed to unmarshal results %v", err)
			}
			if len(preparedResults) == 0 {
				return nil, fmt.Errorf("N1QL: Unknown error, no prepared results returned")
			}
			serialized, _ := json.Marshal(preparedResults[0])
			stmt.name = preparedResults[0].(map[string]interface{})["name"].(string)
			stmt.prepared = string(serialized)
		case "signature":
			stmt.signature = string(*results)
		}
	}

	if stmt.prepared == "" {
		return nil, ErrInternalError
	}

	return stmt, nil
}

func (conn *n1qlConn) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

func (conn *n1qlConn) Close() error {
	return nil
}

func decodeSignature(signature *json.RawMessage) interface{} {

	var sign interface{}
	var rows map[string]interface{}

	json.Unmarshal(*signature, &sign)

	switch s := sign.(type) {
	case map[string]interface{}:
		return s
	case string:
		return s
	default:
		fmt.Printf(" Cannot decode signature. Type of this signature is %T", s)
		return map[string]interface{}{"*": "*"}
	}

	return rows
}

func (conn *n1qlConn) performQueryRaw(query string, requestValues *url.Values) (io.ReadCloser, error) {
	resp, err := conn.doClientRequest(query, requestValues)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return resp.Body, fmt.Errorf("Request failed with error code %d.", resp.StatusCode)
	}
	return resp.Body, nil
}

func getDecoder(r io.Reader) (*json.Decoder, error) {
	if r == nil {
		return nil, fmt.Errorf("Failed to decode nil response.")
	}
	return json.NewDecoder(r), nil
}

func (conn *n1qlConn) performQuery(query string, requestValues *url.Values) (godbc.Rows, error) {

	resp, err := conn.doClientRequest(query, requestValues)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s", bod)
	}

	var resultMap map[string]*json.RawMessage
	decoder, err := getDecoder(resp.Body)
	if err != nil {
		return nil, err
	}

	err = decoder.Decode(&resultMap)
	if err != nil {
		return nil, fmt.Errorf(" N1QL: Failed to decode result %v", err)
	}

	var signature interface{}
	var resultRows *json.RawMessage
	var metrics interface{}
	var status interface{}
	var requestId interface{}
	var errs interface{}

	for name, results := range resultMap {
		switch name {
		case "errors":
			_ = json.Unmarshal(*results, &errs)
		case "signature":
			if results != nil {
				signature = decodeSignature(results)
			} else if N1QL_PASSTHROUGH_MODE == true {
				// for certain types of DML queries, the returned signature could be null
				// however in passthrough mode we always return the metrics, status etc as
				// rows therefore we need to ensure that there is a default signature.
				signature = map[string]interface{}{"*": "*"}
			}
		case "results":
			resultRows = results
		case "metrics":
			if N1QL_PASSTHROUGH_MODE == true {
				_ = json.Unmarshal(*results, &metrics)
			}
		case "status":
			if N1QL_PASSTHROUGH_MODE == true {
				_ = json.Unmarshal(*results, &status)
			}
		case "requestID":
			if N1QL_PASSTHROUGH_MODE == true {
				_ = json.Unmarshal(*results, &requestId)
			}
		}
	}

	if N1QL_PASSTHROUGH_MODE == true {
		extraVals := map[string]interface{}{"requestID": requestId,
			"status":    status,
			"signature": signature,
		}

		// in passthrough mode last line will always be en error line
		errors := map[string]interface{}{"errors": errs}
		return resultToRows(bytes.NewReader(*resultRows), resp, signature, metrics, errors, extraVals)
	}

	// we return the errors with the rows because we can have scenarios where there are valid
	// results returned along with the error and this interface doesn't allow for both to be
	// returned and hence this workaround.
	return resultToRows(bytes.NewReader(*resultRows), resp, signature, nil, errs, nil)

}

// Executes a query that returns a set of Rows.
// Select statements should use this interface
func (conn *n1qlConn) Query(query string, args ...interface{}) (godbc.Rows, error) {

	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("Argument count mismatch %d != %d", argCount, len(args))
		}
		query, args = preparePositionalArgs(query, argCount, args)
	}

	return conn.performQuery(query, nil)
}

func (conn *n1qlConn) QueryRaw(query string, args ...interface{}) (io.ReadCloser, error) {
	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("Argument count mismatch %d != %d", argCount, len(args))
		}
		query, args = preparePositionalArgs(query, argCount, args)
	}

	return conn.performQueryRaw(query, nil)
}

func (conn *n1qlConn) performExecRaw(query string, requestValues *url.Values) (io.ReadCloser, error) {
	resp, err := conn.doClientRequest(query, requestValues)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return resp.Body, fmt.Errorf("Request failed with error code %d.", resp.StatusCode)
	}
	return resp.Body, nil
}

func (conn *n1qlConn) performExec(query string, requestValues *url.Values) (godbc.Result, error) {

	resp, err := conn.doClientRequest(query, requestValues)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("%s", bod)
	}

	var resultMap map[string]*json.RawMessage
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
	}

	if err := json.Unmarshal(body, &resultMap); err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
	}

	var execErr error
	res := &n1qlResult{}
	for name, results := range resultMap {
		switch name {
		case "metrics":
			var metrics map[string]interface{}
			err := json.Unmarshal(*results, &metrics)
			if err != nil {
				return nil, fmt.Errorf("N1QL: Failed to unmarshal response. Error %v", err)
			}
			if mc, ok := metrics["mutationCount"]; ok {
				res.affectedRows = int64(mc.(float64))
			}
			break
		case "errors":
			var errs []interface{}
			_ = json.Unmarshal(*results, &errs)
			execErr = fmt.Errorf("N1QL: Error executing query %v", serializeErrors(errs))
		}
	}

	return res, execErr
}

// Execer implementation. To be used for queries that do not return any rows
// such as Create Index, Insert, Upset, Delete etc
func (conn *n1qlConn) Exec(query string, args ...interface{}) (godbc.Result, error) {

	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("Argument count mismatch %d != %d", argCount, len(args))
		}
		query, args = preparePositionalArgs(query, argCount, args)
	}

	return conn.performExec(query, nil)
}

func (conn *n1qlConn) ExecRaw(query string, args ...interface{}) (io.ReadCloser, error) {
	if len(args) > 0 {
		var argCount int
		query, argCount = prepareQuery(query)
		if argCount != len(args) {
			return nil, fmt.Errorf("Argument count mismatch %d != %d", argCount, len(args))
		}
		query, args = preparePositionalArgs(query, argCount, args)
	}

	return conn.performExecRaw(query, nil)
}

func prepareQuery(query string) (string, int) {

	var count int
	re := regexp.MustCompile("\\?")

	f := func(s string) string {
		count++
		return fmt.Sprintf("$%d", count)
	}
	return re.ReplaceAllStringFunc(query, f), count
}

//
// Replace the conditional pqrams in the query and return the list of left-over args
func preparePositionalArgs(query string, argCount int, args []interface{}) (string, []interface{}) {
	subList := make([]string, 0)
	newArgs := make([]interface{}, 0)

	for i, arg := range args {
		if i < argCount {
			var a string
			switch arg := arg.(type) {
			case string:
				a = fmt.Sprintf("\"%v\"", arg)
			case []byte:
				a = string(arg)
			default:
				a = fmt.Sprintf("%v", arg)
			}
			sub := []string{fmt.Sprintf("$%d", i+1), a}
			subList = append(subList, sub...)
		} else {
			newArgs = append(newArgs, arg)
		}
	}
	r := strings.NewReplacer(subList...)
	return r.Replace(query), newArgs
}

// prepare a http request for the query
//
func prepareRequest(query string, queryAPI string, args []interface{}) (*http.Request, error) {

	postData := url.Values{}
	postData.Set("statement", query)

	if len(args) > 0 {
		paStr := buildPositionalArgList(args)
		if len(paStr) > 0 {
			postData.Set("args", paStr)
		}
	}

	setQueryParams(&postData)

	request, err := http.NewRequest("POST", queryAPI, bytes.NewBufferString(postData.Encode()))
	if err != nil {
		return nil, err
	}
	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	setCBUserAgent(request)
	if hasUsernamePassword() {
		request.SetBasicAuth(username, password)
	}

	return request, nil
}

//
// Set query params

func setQueryParams(v *url.Values) {

	for key, value := range QueryParams {
		v.Set(key, value)
	}
}

// Return hostname and port for IPv4 and IPv6
func HostNameandPort(node string) (host, port string, ipv6 bool, err error) {
	tokens := []string{}

	// Set _IPv6 based on input address
	ipv6, err = IsIPv6(node)

	if err != nil {
		return "", "", false, err
	}

	err = nil
	// For IPv6
	if ipv6 {
		// Then the url should be of the form [::1]:8091
		tokens = strings.Split(node, "]:")
		host = strings.Replace(tokens[0], "[", "", 1)

	} else {
		// For IPv4
		tokens = strings.Split(node, ":")
		host = tokens[0]
	}

	if len(tokens) == 2 {
		port = tokens[1]
	} else {
		port = ""
	}

	return
}

func IsIPv6(str string) (bool, error) {

	//ipv6 - can be [::1]:8091
	host, _, err := net.SplitHostPort(str)
	if err != nil {
		host = str
	}

	ip := net.ParseIP(host)
	if ip.To4() == nil {
		//Not an ipv4 address
		// check if ipv6
		if ip.To16() == nil {
			// Not ipv6
			return false, fmt.Errorf("\nThis is an incorrect address %v", str)
		}
		// IPv6
		return true, nil

	}
	// IPv4
	return false, nil
}
