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
	"fmt"
	"io"
	"net/url"

	"github.com/couchbase/godbc"
)

type N1qlStmt interface {
	godbc.Stmt
	QueryRaw(args ...interface{}) (io.ReadCloser, error)
	ExecRaw(args ...interface{}) (io.ReadCloser, error)
}

// Implements N1qlStmt interface.
type n1qlStmt struct {
	conn      *n1qlConn
	prepared  string
	signature string
	argCount  int
	name      string
}

func (stmt *n1qlStmt) Close() error {
	stmt.prepared = ""
	stmt.signature = ""
	stmt.argCount = 0
	stmt = nil
	return nil
}

func (stmt *n1qlStmt) NumInput() int {
	return stmt.argCount
}

func buildPositionalArgList(args []interface{}) string {
	positionalArgs := make([]string, 0)
	for _, arg := range args {
		switch arg := arg.(type) {
		case string:
			// add double quotes since this is a string
			positionalArgs = append(positionalArgs, fmt.Sprintf("\"%v\"", arg))
		case []byte:
			positionalArgs = append(positionalArgs, string(arg))
		default:
			positionalArgs = append(positionalArgs, fmt.Sprintf("%v", arg))
		}
	}

	if len(positionalArgs) > 0 {
		paStr := "["
		for i, param := range positionalArgs {
			if i == len(positionalArgs)-1 {
				paStr = fmt.Sprintf("%s%s]", paStr, param)
			} else {
				paStr = fmt.Sprintf("%s%s,", paStr, param)
			}
		}
		return paStr
	}
	return ""
}

// prepare a http request for the query
//
func (stmt *n1qlStmt) prepareRequest(args []interface{}) (*url.Values, error) {

	postData := url.Values{}

	// use name prepared statement if possible
	if stmt.name != "" {
		postData.Set("prepared", fmt.Sprintf("\"%s\"", stmt.name))
	} else {
		postData.Set("prepared", stmt.prepared)
	}

	if len(args) < stmt.NumInput() {
		return nil, fmt.Errorf("N1QL: Insufficient args. Prepared statement contains positional args")
	}

	if len(args) > 0 {
		paStr := buildPositionalArgList(args)
		if len(paStr) > 0 {
			postData.Set("args", paStr)
		}
	}

	setQueryParams(&postData, nil)

	return &postData, nil
}

func (stmt *n1qlStmt) Query(args ...interface{}) (godbc.Rows, error) {
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}

retry:
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.conn.performQuery("", requestValues)
	if err != nil && stmt.name != "" {
		// retry once if we used a named prepared statement
		stmt.name = ""
		goto retry
	}

	return rows, err
}

func (stmt *n1qlStmt) QueryRaw(args ...interface{}) (io.ReadCloser, error) {
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}

retry:
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}

	body, err := stmt.conn.performQueryRaw("", requestValues)
	if err != nil && stmt.name != "" {
		// retry once if we used a named prepared statement
		stmt.name = ""
		goto retry
	}

	return body, err
}

func (stmt *n1qlStmt) QueryRow(args ...interface{}) godbc.Row {
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil
	}
	hasFirst := rows.Next()
	if !hasFirst {
		return nil
	}
	return rows // Row is a subset of Rows.
}

func (stmt *n1qlStmt) Exec(args ...interface{}) (godbc.Result, error) {
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}

	return stmt.conn.performExec("", requestValues)
}

func (stmt *n1qlStmt) ExecRaw(args ...interface{}) (io.ReadCloser, error) {
	if stmt.prepared == "" {
		return nil, fmt.Errorf("N1QL: Prepared statement not found")
	}
	requestValues, err := stmt.prepareRequest(args)
	if err != nil {
		return nil, err
	}

	return stmt.conn.performExecRaw("", requestValues)
}
