//  Copieright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package n1ql

import "github.com/couchbase/godbc"

func Open(dataSourceName string) (godbc.DB, error) {
	return open(dataSourceName, "")
}

func OpenExtended(dataSourceName string, userAgent string) (N1qlDB, error) {
	return open(dataSourceName, userAgent)
}

func open(dataSourceName string, userAgent string) (*n1qlDB, error) {
	n1qlConn, err := OpenN1QLConnection(dataSourceName, userAgent)
	if err != nil {
		return nil, err
	}
	return &n1qlDB{conn: n1qlConn}, nil
}
