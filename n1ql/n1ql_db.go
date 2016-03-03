//  Copieright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package n1ql

import (
	"errors"
	"io"

	"github.com/couchbase/godbc"
)

type N1qlDB interface {
	godbc.DB
	PrepareExtended(query string) (N1qlStmt, error)

	// Run the query with the given parameters.
	// Returns the raw streaming input from the body of the RESTful request
	// to the database. The returned error contains a short description
	// of what went wrong with the query.
	//
	// Note that under some conditions, where the request was actually sent to the
	// server, both the stream and an error are returned.
	QueryRaw(query string, args ...interface{}) (io.ReadCloser, error)

	// Execute the statement with the given parameters.
	// Returns the raw streaming input from the body of the RESTful request
	// to the database. The returned error contains a short description
	// of what went wrong with the query.
	//
	// Note that under some conditions, where the request was actually sent to the
	// server, both the stream and an error are returned.
	ExecRaw(query string, args ...interface{}) (io.ReadCloser, error)
}

// Implements godbc.DB interface.
type n1qlDB struct {
	conn *n1qlConn
}

var errorNoConnection = errors.New("N1QL connection is already closed.")

func (db *n1qlDB) Begin() (godbc.Tx, error) {
	return nil, errors.New("Transactions are not supported.")
}

func (db *n1qlDB) Close() error {
	if db.conn == nil {
		return errorNoConnection
	}
	err := db.conn.Close()
	if err != nil {
		return err
	}
	db.conn = nil
	return nil
}

func (db *n1qlDB) Exec(query string, args ...interface{}) (godbc.Result, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (db *n1qlDB) ExecRaw(query string, args ...interface{}) (io.ReadCloser, error) {
	if db.conn == nil {
		return nil, errorNoConnection
	}
	return db.conn.ExecRaw(query, args...)
}

func (db *n1qlDB) Ping() error {
	_, error := db.Query(N1QL_DEFAULT_STATEMENT)
	return error
}

func (db *n1qlDB) Prepare(query string) (godbc.Stmt, error) {
	return db.prepare(query)
}

func (db *n1qlDB) PrepareExtended(query string) (N1qlStmt, error) {
	return db.prepare(query)
}

func (db *n1qlDB) prepare(query string) (*n1qlStmt, error) {
	if db.conn == nil {
		return nil, errorNoConnection
	}
	return db.conn.Prepare(query)
}

func (db *n1qlDB) Query(query string, args ...interface{}) (godbc.Rows, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (db *n1qlDB) QueryRaw(query string, args ...interface{}) (io.ReadCloser, error) {
	if db.conn == nil {
		return nil, errorNoConnection
	}
	return db.conn.QueryRaw(query, args...)
}

func (db *n1qlDB) QueryRow(query string, args ...interface{}) godbc.Row {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil
	}
	hasFirst := rows.Next()
	if !hasFirst {
		return nil
	}
	return rows // Row is a subset of Rows.
}

func (db *n1qlDB) SetMaxIdleConns(n int) {
	// Do nothing. We don't keep track of connections.
}

func (db *n1qlDB) SetMaxOpenConns(n int) {
	// Do nothing. We don't keep track of connections.
}

func (db *n1qlDB) Stats() godbc.DBStats {
	return nil
}
