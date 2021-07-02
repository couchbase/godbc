//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package n1ql

import "github.com/couchbase/godbc"

func Open(dataSourceName string) (godbc.DB, error) {
	return open(dataSourceName)
}

func OpenExtended(dataSourceName string) (N1qlDB, error) {
	return open(dataSourceName)
}

func open(dataSourceName string) (*n1qlDB, error) {
	n1qlConn, err := OpenN1QLConnection(dataSourceName)
	if err != nil {
		return nil, err
	}
	return &n1qlDB{conn: n1qlConn}, nil
}
