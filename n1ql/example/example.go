package main

import (
	"fmt"
	"io"
	"os"

	"github.com/couchbase/godbc/n1ql"
)

// Illustrates the use of the raw result interface of the N1QL module.
func main() {
	db, err := n1ql.OpenExtended("http://localhost:8093","example")
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
