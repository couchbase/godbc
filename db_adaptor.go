//  Copieright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Wrap the sql.DB object in a wrapper that implments the broader godbc.DB interface.
package godbc

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

type dbAdaptor struct {
	sqlDb *sql.DB
}

func (adaptor *dbAdaptor) Begin() (Tx, error) {
	return nil, errors.New("dbAdaptor.Begin() is not implemented.")
}

func (adaptor *dbAdaptor) Close() error {
	return adaptor.sqlDb.Close()
}

func (adaptor *dbAdaptor) Driver() driver.Driver {
	return adaptor.sqlDb.Driver()
}

func (adaptor *dbAdaptor) Exec(query string, args ...interface{}) (Result, error) {
	res, err := adaptor.sqlDb.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return &resultAdaptor{sqlResult: res}, nil
}

func (adaptor *dbAdaptor) Ping() error {
	return adaptor.sqlDb.Ping()
}

func (adaptor *dbAdaptor) Prepare(query string) (Stmt, error) {
	sStmt, err := adaptor.sqlDb.Prepare(query)
	return &stmtAdaptor{sqlStmt: sStmt}, err
}

func (adaptor *dbAdaptor) Query(query string, args ...interface{}) (Rows, error) {
	res, err := adaptor.sqlDb.Query(query, args...)
	return res, err
}

func (adaptor *dbAdaptor) QueryRow(query string, args ...interface{}) Row {
	res := adaptor.sqlDb.QueryRow(query, args...)
	return res
}

func (adaptor *dbAdaptor) SetMaxIdleConns(n int) {
	adaptor.sqlDb.SetMaxIdleConns(n)
}

func (adaptor *dbAdaptor) SetMaxOpenConns(n int) {
	adaptor.sqlDb.SetMaxOpenConns(n)
}

func (adaptor *dbAdaptor) Stats() DBStats {
	return nil
}
