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

	"github.com/couchbase/godbc"
	"github.com/couchbase/query/primitives/couchbase"
	"github.com/couchbase/query/util"
)

// Common error codes
var (
	ErrNotSupported   = fmt.Errorf("N1QL: Not supported")
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
	LOCALHOST              = N1QL_DEFAULT_HOST
)

// flags

var (
	N1QL_PASSTHROUGH_MODE = false
)

// Rest API query parameters
var QueryParams map[string]string
var TxTimeout string

// Username and password. Used for querying the cluster endpoint,
// which may require authorization.
var username, password string

// Used to decide whether to skip verification of certificates when
// connecting to an ssl port.
var skipVerify = true
var certFile = ""
var keyFile = ""
var caFile = ""
var privateKeyPassphrase = []byte{}

var isAnalytics = false
var networkCfg = "default"

const schemestring = "://"

func init() {
	QueryParams = make(map[string]string)
}

func SetIsAnalytics(val bool) {
	isAnalytics = val
}

func SetNetworkType(networkType string) {
	networkCfg = networkType
}

func SetQueryParams(key string, value string) error {

	if key == "" {
		return fmt.Errorf("N1QL: Key not specified")
	} else if key == "txtimeout" {
		TxTimeout = value
	}

	QueryParams[key] = value
	return nil
}

func SetTxTimeout(value string) {
	TxTimeout = value
}

func UnsetQueryParams(key string) error {

	if key == "" {
		return fmt.Errorf("N1QL: Key not specified")
	} else if key == "txtimeout" {
		TxTimeout = ""
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

func SetCertFile(cert string) {
	certFile = cert
}

func SetKeyFile(key string) {
	keyFile = key
}

func SetCaFile(cacert string) {
	caFile = cacert
}

func SetPrivateKeyPassphrase(passphrase []byte) {
	privateKeyPassphrase = passphrase
}

// implements driver.Conn interface
type n1qlConn struct {
	clusterAddr string
	queryAPIs   []string
	txid        string
	txService   string
	client      *http.Client
	lock        sync.RWMutex
}

// HTTPClient to use for REST and view operations.
var MaxIdleConnsPerHost = 10
var HTTPTransport = &http.Transport{MaxIdleConnsPerHost: MaxIdleConnsPerHost}
var HTTPClient = &http.Client{Transport: HTTPTransport}

// Auto discover N1QL and Analytics services depending on input
func discoverN1QLService(name string, ps couchbase.PoolServices, isAnalytics bool, networkType string) ([]string, error) {
	var hostnm string
	var port int
	var ipv6, ok, external bool
	var hostUrl *url.URL

	prefixUrl := "http://"
	serviceType := "n1ql"
	if isAnalytics {
		serviceType = "cbas"
	}

	// Since analytics doesnt have a rest endpoint that lists the cluster nodes
	// We need to populate the list of analytics APIs here itself
	// We might as well do the same for query. This makes getQueryApi() redundant.
	queryAPIs := []string{}

	// If the network type isn't provided, then we need to detect whether to use default address or alternate address
	// by comparing the input hostname with the hostname's under services.
	// If it matches then we know its a default (internal address), else we can think of it as an external address and
	// move on, throwing an error if that doesnt work.
	hostnm = strings.TrimSpace(name)
	if strings.HasPrefix(hostnm, "http://") || strings.HasPrefix(hostnm, "https://") {
		if strings.HasPrefix(hostnm, "https://") {
			prefixUrl = "https://"
			serviceType += "SSL"
		}
		hostUrl, _ = url.Parse(name)
		hostnm = hostUrl.Host
	}
	if networkCfg == "external" {
		external = true
	} else if networkCfg == "auto" {
		for _, ns := range ps.NodesExt {
			if v, found := ns.AlternateNames["external"]; found {
				if strings.Compare(v.Hostname, hostUrl.Hostname()) == 0 {
					external = true
					break
				}
			}
		}
	}

	for _, ns := range ps.NodesExt {
		if ns.Services == nil {
			continue
		}

		port, ok = ns.Services[serviceType]

		if !external {
			if ns.Hostname != "" {
				hostnm = ns.Hostname
			}
		} else {
			v, found := ns.AlternateNames["external"]
			if !found || v.Hostname == "" {
				continue
			}

			hostnm = v.Hostname
			if v.Ports != nil {
				port, ok = v.Ports[serviceType]
			}

		}
		hostnm, _, ipv6, _ = HostNameandPort(hostnm)
		// we have found a port. And we have hostname as well.
		if ok {
			// n1ql or analytics service found
			if ipv6 {
				queryAPIs = append(queryAPIs, fmt.Sprintf("%s[%s]:%d"+N1QL_SERVICE_ENDPOINT, prefixUrl, hostnm, port))
			} else {
				queryAPIs = append(queryAPIs, fmt.Sprintf("%s%s:%d"+N1QL_SERVICE_ENDPOINT, prefixUrl, hostnm, port))
			}
		}
	}
	return queryAPIs, nil
}

var cbUserAgent string = "godbc/" + util.VERSION

func SetCBUserAgentHeader(v string) {
	cbUserAgent = v
}

func setCBUserAgent(request *http.Request) {
	request.Header.Add("CB-User-Agent", cbUserAgent)
}

func getQueryApi(n1qlEndPoint string, isHttps bool) ([]string, error) {

	queryAdmin := n1qlEndPoint + "/admin/clusters/default/nodes"

	if isHttps {
		queryAdmin = "https://" + queryAdmin
	} else {
		queryAdmin = "http://" + queryAdmin
	}

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
		return nil, fmt.Errorf("HTTP client request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bod, _ := ioutil.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("HTTP client response error: %s", bod)
	}

	var nodesInfo []interface{}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("N1QL: Failed to read response body from server: %v", err)
	}

	if err := json.Unmarshal(body, &nodesInfo); err != nil {
		return nil, fmt.Errorf("N1QL: Failed to parse response: %v", err)
	}

	for _, queryNode := range nodesInfo {
		switch queryNode := queryNode.(type) {
		case map[string]interface{}:
			queryAPIs = append(queryAPIs, queryNode["queryEndpoint"].(string))
		}
	}

	if ipv6 {
		hostname = "[" + hostname + "]"
		LOCALHOST = "[::1]"
	}

	// if the end-points contain localhost IPv4 or IPv6 then replace them with the actual hostname
	for i, qa := range queryAPIs {
		queryAPIs[i] = strings.Replace(qa, LOCALHOST, hostname, -1)
	}

	if len(queryAPIs) == 0 {
		return nil, fmt.Errorf("Query endpoints not found")
	}

	return queryAPIs, nil
}

func OpenN1QLConnection(name string) (*n1qlConn, error) {
	var queryAPIs []string = nil

	if name == "" {
		return nil, fmt.Errorf(" N1QL: Invalid query service endpoint.")
	}

	if strings.HasPrefix(name, "https") {
		//First check if the input string is a cluster endpoint
		couchbase.SetSkipVerify(skipVerify)

		if skipVerify {
			HTTPTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		} else {
			if certFile != "" && keyFile != "" {
				couchbase.SetCertFile(certFile)
				couchbase.SetKeyFile(keyFile)
				couchbase.SetPrivateKeyPassphrase(privateKeyPassphrase)
			} else if certFile != "" || keyFile != "" {
				//error need to pass both certfile and keyfile
				return nil, fmt.Errorf("N1QL: Need to pass both certfile and keyfile")
			}

			if caFile != "" {
				couchbase.SetCaFile(caFile)
			}

			// For 18093 connections
			cfg, err := couchbase.ClientConfigForX509(caFile,
				certFile,
				keyFile,
				privateKeyPassphrase)
			if err != nil {
				return nil, err
			}

			HTTPTransport.TLSClientConfig = cfg

		}
	}
	var client couchbase.Client
	var err, perr error

	// Connect to a couchbase cluster
	if hasUsernamePassword() {
		// append these values to the url
		newUrl, er := url.Parse(name)
		if er == nil {
			name = newUrl.Scheme + schemestring + username + ":" + password + "@" + newUrl.Host
		}
	}
	client, perr = couchbase.Connect(name)

	if perr != nil {
		if strings.Contains(perr.Error(), "Unauthorized") {
			return nil, perr
		}
		// Direct query entry (8093 or 8095 for example. So connect to that.)
		// If not cluster endpoint then check if query endpoint
		name = strings.TrimSuffix(name, "/")
		queryAPI := name + N1QL_SERVICE_ENDPOINT
		queryAPIs = make([]string, 1, 1)
		queryAPIs[0] = queryAPI
	} else {
		// Connection was possible - means this is a cluster endpoint.
		// We need to auto detect the query / analytics nodes.
		// Query by default. Analytics if option is set.

		// Get pools/default/nodeServices
		ps, err := client.GetPoolServices("default")
		if err != nil {
			return nil, fmt.Errorf("N1QL: Failed to get NodeServices list: %v", err)
		}

		queryAPIs, err = discoverN1QLService(name, ps, isAnalytics, networkCfg)
		if err != nil {
			return nil, err
		}

		sType := "N1QL"
		if isAnalytics {
			sType = "Analytics"
		}

		if len(queryAPIs) <= 0 {
			return nil, fmt.Errorf("N1QL: No " + sType + " service found on this cluster")
		}
	}

	conn := &n1qlConn{client: HTTPClient, queryAPIs: queryAPIs}

	txParams := map[string]string{"txid": "", "tximplicit": ""}
	request, err := prepareRequest(N1QL_DEFAULT_STATEMENT, queryAPIs[0], nil, txParams)
	if err != nil {
		return nil, err
	}

	resp, err := conn.client.Do(request)

	if err != nil {
		if perr != nil {
			return nil, fmt.Errorf("N1QL: Unable to connect to endpoint %s: %v", name, stripurl(perr.Error()))
		}
		return nil, fmt.Errorf("N1QL: Unable to connect to endpoint %s: %v", name, stripurl(err.Error()))
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("N1QL: Unable to connect to N1QL endpoint: %s \nHTTP ERR: %v", queryAPIs[0], resp.Status)
	}
	if perr != nil {
		// Also check for the case where its auth failure but the http request to query was successful
		var resultMap map[string]*json.RawMessage

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("N1QL: Failed to read response body from server. Error %v", err)
		}

		if err := json.Unmarshal(body, &resultMap); err != nil {
			return nil, fmt.Errorf("N1QL: Failed to parse response. Error %v", err)
		}

		errors, ok := resultMap["errors"]
		if ok && errors != nil {
			var errs []interface{}
			_ = json.Unmarshal(*errors, &errs)
			return nil, fmt.Errorf("N1QL: Connection error. %v", serializeErrors(errs, true))
		}

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

	stmtType := txStatementType(query)
	ok := false
	for !ok {

		var request *http.Request
		var err error
		var selectedNode, numNodes int
		var queryAPI string
		var txParams map[string]string

		// select query API
		if conn.txid != "" && query != N1QL_DEFAULT_STATEMENT {
			txParams = map[string]string{"txid": conn.txid, "tximplicit": ""}
			queryAPI = conn.txService
		} else {
			if stmtType == TX_START && TxTimeout != "" {
				txParams = map[string]string{"txtimeout": TxTimeout}
			}
			rand.Seed(time.Now().Unix())
			numNodes = len(conn.queryAPIs)

			selectedNode = rand.Intn(numNodes)
			conn.lock.RLock()
			queryAPI = conn.queryAPIs[selectedNode]
			conn.lock.RUnlock()
		}

		if query != "" {
			request, err = prepareRequest(query, queryAPI, nil, txParams)
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
			if conn.txService != "" || numNodes == 1 {
				conn.SetTxValues("", "")
				break
			}
			// remove the node that failed from the list of query nodes
			conn.lock.Lock()
			conn.queryAPIs = append(conn.queryAPIs[:selectedNode], conn.queryAPIs[selectedNode+1:]...)
			conn.lock.Unlock()
			continue
		} else {
			if stmtType == TX_START {
				txid := getTxid(resp)
				if txid != "" {
					conn.SetTxValues(txid, queryAPI)
				}
			} else if stmtType == TX_COMMIT || stmtType == TX_ROLLBACK {
				conn.SetTxValues("", "")
			}
			return resp, nil

		}
	}

	return nil, fmt.Errorf("N1QL: Query nodes not responding")
}

func (conn *n1qlConn) SetTxValues(txid, txService string) {
	conn.lock.Lock()
	defer conn.lock.Unlock()
	conn.txid = txid
	conn.txService = txService
}

func (conn *n1qlConn) TxService() bool {
	return conn.txService != ""
}

func serializeErrors(errors interface{}, onlymsg bool) string {

	var errString string
	switch errors := errors.(type) {
	case []interface{}:
		for _, e := range errors {
			switch e := e.(type) {
			case map[string]interface{}:
				tmpErr := ""
				code, _ := e["code"]
				msg, _ := e["msg"]

				if onlymsg && msg != "" {
					tmpErr = fmt.Sprintf("Message : %v", msg)
				} else if code != 0 && msg != "" {
					tmpErr = fmt.Sprintf("Code : %v Message : %v", code, msg)
				}

				if errString != "" {
					errString = fmt.Sprintf("%v ", errString) + tmpErr
				} else {
					errString = tmpErr
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
	defer resp.Body.Close()

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
		return nil, fmt.Errorf("N1QL: Error preparing statement %v", serializeErrors(errs, false))
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
	defer resp.Body.Close()

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
	defer resp.Body.Close()

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
			execErr = fmt.Errorf("N1QL: Error executing query %v", serializeErrors(errs, false))
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
func prepareRequest(query string, queryAPI string, args []interface{}, txParams map[string]string) (*http.Request, error) {

	postData := url.Values{}
	postData.Set("statement", query)

	if len(args) > 0 {
		paStr := buildPositionalArgList(args)
		if len(paStr) > 0 {
			postData.Set("args", paStr)
		}
	}

	setQueryParams(&postData, txParams)

	request, err := http.NewRequest("POST", queryAPI, bytes.NewBufferString(postData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("Error creating HTTP request: %v", err)
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

func setQueryParams(v *url.Values, txParms map[string]string) {

	for key, value := range QueryParams {
		if _, ok := txParms[key]; !ok {
			v.Set(key, value)
		}
	}
	for key, value := range txParms {
		if value != "" {
			v.Set(key, value)
		} else {
			v.Del(key)
		}

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

	if host == "localhost" {
		host = LOCALHOST
	}

	ip := net.ParseIP(host)
	if ip == nil {
		// Essentially this is a FQDN. Golangs ParseIP cannot parse IPs that are non-numerical.
		// It could also be an incorrect address. But that can be handled by split host port.
		// This method is only to check if address is IPv6.
		return false, nil
	}
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

const (
	TX_NONE = iota
	TX_START
	TX_COMMIT
	TX_ROLLBACK
)

func txStatementType(query string) int {
	q := strings.TrimSpace(query)
	if len(q) > 32 {
		q = q[0:32]
	}
	q = strings.ToLower(q)
	qf := strings.Fields(q)
	if len(qf) > 0 {
		switch strings.TrimRight(qf[0], ";") {
		case "start", "begin":
			if len(qf) > 1 {
				return TX_START
			}
		case "commit":
			return TX_COMMIT
		case "rollback":
			if len(qf) < 3 {
				return TX_ROLLBACK
			}
		}
	}
	return TX_NONE
}

func getTxid(resp *http.Response) (txid string) {
	if resp.StatusCode != 200 {
		return
	}

	var resultMap map[string]interface{}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close() //  must close
	resp.Body = ioutil.NopCloser(bytes.NewBuffer(body))

	if err = json.Unmarshal(body, &resultMap); err != nil {
		return
	}

	if resultMap["status"].(string) != "success" {
		return
	}

	results := resultMap["results"].([]interface{})
	if len(results) > 0 {
		txid = results[0].(map[string]interface{})["txid"].(string)
	}
	return
}
