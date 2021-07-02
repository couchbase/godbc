/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package main

import (
	"fmt"
	"io"
	"os"

	"github.com/couchbase/godbc/n1ql"
)

// Illustrates the use of the raw result interface of the N1QL module.
func main() {
	db, err := n1ql.OpenExtended("http://localhost:8093")
	if err != nil {
		fmt.Println("Failed to open.", err.Error())
		return
	}
	name := "21st Amendment Brewery Cafe"
	ioSrc, err := db.QueryRaw("select * from `beer-sample` where name = ?", name)

	// Try this query instead, for a much larger (streaming!) output.
	//ioSrc, err := db.QueryRaw("select * from `beer-sample`")

	// For a command rather than a query, try this.
	//ioSrc, err := db.ExecRaw("insert into default(key, value) values ('111', { 'a': 1, 'b': 2 })")

	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
	}
	if ioSrc != nil {
		defer ioSrc.Close()
		io.Copy(os.Stdout, ioSrc)
	} else {
		fmt.Printf("NO OUTPUT.\n")
	}
}
