//  Copieright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package godbc

import "errors"

type n1qlDB struct {
	conn *n1qlConn
}

func (db *n1qlDB) Begin() (Tx, error) {
	return nil, errors.New("Transactions are not supported.")
}

func (db *n1qlDB) Close() error {
	if db.conn == nil {
		return errors.New("N1QL connection is already closed.")
	}
	err := db.conn.Close()
	if err != nil {
		return err
	}
	db.conn = nil
	return nil
}

func (db *n1qlDB) Exec(query string, args ...interface{}) (Result, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(args...)
}

func (db *n1qlDB) Ping() error {
	_, error := db.Query("select * from system:keyspaces")
	return error
}

func (db *n1qlDB) Prepare(query string) (Stmt, error) {
	if db.conn == nil {
		return nil, errors.New("N1QL connection is closed.")
	}
	return db.conn.Prepare(query)
}

func (db *n1qlDB) Query(query string, args ...interface{}) (Rows, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(args...)
}

func (db *n1qlDB) QueryRow(query string, args ...interface{}) Row {
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

func (db *n1qlDB) Stats() DBStats {
	return nil
}
