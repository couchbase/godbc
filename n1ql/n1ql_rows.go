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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
)

// Implements godbc.Rows.
type n1qlRows struct {
	resp        *http.Response
	results     io.Reader
	resultChan  chan interface{}
	errChan     chan error
	closed      bool
	signature   interface{}
	extras      interface{}
	metrics     interface{}
	errors      interface{}
	passthrough bool
	columns     []string
	rowsSent    int
	curValues   []interface{}
	iterError   error
}

func resultToRows(results io.Reader, resp *http.Response, signature interface{}, metrics, errors, extraVals interface{}) (*n1qlRows, error) {

	rows := &n1qlRows{results: results,
		resp:       resp,
		signature:  signature,
		extras:     extraVals,
		metrics:    metrics,
		errors:     errors,
		resultChan: make(chan interface{}, 1),
		errChan:    make(chan error),
	}

	// detect if we are in passthrough mode
	if metrics != nil && extraVals != nil {
		rows.passthrough = true
	}

	go rows.populateRows()

	return rows, nil
}

func (rows *n1qlRows) populateRows() {
	var resultRows []interface{}
	defer rows.resp.Body.Close()

	resultsDecoder, err := getDecoder(rows.results)
	if err != nil {
		rows.errChan <- err
	} else {
		err = resultsDecoder.Decode(&resultRows)
		if err != nil {
			rows.errChan <- err
		}
	}

	if rows.extras != nil {
		rows.resultChan <- rows.extras
	}

	// second row will be metrics
	if rows.metrics != nil {
		rows.resultChan <- rows.metrics
	}

	for _, row := range resultRows {
		if rows.closed == true {
			break
		}
		rows.resultChan <- row
	}

	if rows.errors != nil {
		rows.resultChan <- rows.errors
	}

	close(rows.resultChan)

}

func (rows *n1qlRows) Columns() ([]string, error) {
	// TODO: This should be computed once, and stored, particularly since it is used by every
	// call to Next().

	var columns = make([]string, 0)

	switch s := rows.signature.(type) {
	case map[string]interface{}:
		for key, _ := range s {
			columns = append(columns, key)
		}
	case string:
		columns = append(columns, s)
	case nil:
		columns = append(columns, "null")
	}

	sort.Strings(columns)
	rows.columns = columns
	return columns, nil
}

func (rows *n1qlRows) Close() error {
	rows.closed = true
	return nil
}

func (rows *n1qlRows) Err() error {
	return rows.iterError
}

func (rows *n1qlRows) Scan(dest ...interface{}) error {
	if rows.curValues == nil {
		return errors.New("No current row.")
	}
	if len(dest) > len(rows.curValues) {
		return fmt.Errorf("Scan() asked for %d values, but only %d are available.", len(dest), len(rows.curValues))
	}
	for i, d := range dest {
		curVal := rows.curValues[i]
		switch ptr := d.(type) {
		case *float64:
			v, ok := curVal.(float64)
			if !ok {
				return fmt.Errorf("Cannot assign to *float64 at index %d of Scan() from value %v.", i, curVal)
			}
			*ptr = v
		case *string:
			v, ok := curVal.(string)
			if ok {
				*ptr = v
			} else {
				bytes, err := json.Marshal(curVal)
				if err != nil {
					return err
				}
				*ptr = string(bytes)
			}
		case *bool:
			v, ok := curVal.(bool)
			if !ok {
				return fmt.Errorf("Cannot assign to *bool at index %d of Scan() from value %v.", i, curVal)
			}
			*ptr = v
		default:
			return fmt.Errorf("Unsupported destination type at parameter %d of Scan().", i)
		}

	}
	return nil
}

func (rows *n1qlRows) Next() bool {
	select {
	case r, ok := <-rows.resultChan:
		if ok {
			cols, _ := rows.Columns()
			numColumns := len(cols)
			dest := make([]interface{}, numColumns)

			if numColumns == 1 {
				dest[0] = r
			} else if rows.passthrough == true && rows.rowsSent < 2 {
				// first two rows in passthrough mode are status and metrics
				// in passthrough mode if the query being executed has multiple projections
				// then it is highly likely that the number of columns of the metrics/status
				// will not match the number of columns, therefore the following hack
				dest[0] = r
				for i := 1; i < numColumns; i++ {
					dest[i] = ""
				}
			} else {
				switch resultRow := r.(type) {
				case map[string]interface{}:
					if len(resultRow) > numColumns {
						rows.iterError = fmt.Errorf("N1QL: More Colums than expected %d != %d r %v", len(resultRow), numColumns, r)
						return false
					}
					i := 0
					for _, colName := range rows.columns {
						if value, exists := resultRow[colName]; exists == true {
							dest[i] = value
						} else {
							dest[i] = ""
						}
						i++
					}
				case []interface{}:
					i := 0
					for _, value := range resultRow {
						dest[i] = value
						i++
					}

				}
			}
			rows.rowsSent++
			rows.curValues = dest
			return true
		} else {
			rows.curValues = nil
			return false
		}
	case e := <-rows.errChan:
		rows.iterError = e
		return false
	}
}
