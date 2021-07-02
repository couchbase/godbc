//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package godbc

type DB interface {
	Begin() (Tx, error)
	Close() error
	Exec(query string, args ...interface{}) (Result, error)
	Ping() error
	Prepare(query string) (Stmt, error)
	Query(query string, args ...interface{}) (Rows, error)
	QueryRow(query string, args ...interface{}) Row
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Stats() DBStats
}
