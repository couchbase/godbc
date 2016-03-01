//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package n1ql

import "github.com/couchbaselabs/godbc"

// Implements godbc.Result interfaces.
type n1qlResult struct {
	affectedRows int64
	insertId     int64
}

func (res *n1qlResult) LastInsertId() (int64, error) {
	return res.insertId, nil
}

func (res *n1qlResult) RowsAffected() (int64, error) {
	return res.affectedRows, nil
}

func (res *n1qlResult) Rows() godbc.Rows {
	return nil
}
