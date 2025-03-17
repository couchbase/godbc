module github.com/couchbase/godbc

go 1.23.0

toolchain go1.23.4

replace github.com/couchbase/cbft => ../../../../../../cbft

replace github.com/couchbase/cbgt => ../../../../../../cbgt

replace github.com/couchbase/hebrew => ../../../../../../hebrew

replace github.com/couchbase/eventing => ../eventing

replace github.com/couchbase/eventing-ee => ../eventing-ee

replace github.com/couchbase/go-couchbase => ../go-couchbase

replace github.com/couchbase/go_json => ../go_json

replace github.com/couchbase/gomemcached => ../gomemcached

replace github.com/couchbase/goutils => ../goutils

replace github.com/couchbase/godbc => ../godbc

replace github.com/couchbase/indexing => ../indexing

replace github.com/couchbase/gometa => ../gometa

replace github.com/couchbase/n1fty => ../n1fty

replace github.com/couchbase/plasma => ../plasma

replace github.com/couchbase/query => ../query

replace github.com/couchbase/query-ee => ../query-ee

replace github.com/couchbase/regulator => ../regulator

require github.com/couchbase/query v0.0.0-00010101000000-000000000000

require (
	github.com/couchbase/cbauth v0.1.13 // indirect
	github.com/couchbase/clog v0.1.0 // indirect
	github.com/couchbase/go-couchbase v0.1.1 // indirect
	github.com/couchbase/go_json v0.0.0-20220330123059-4473a21887c8 // indirect
	github.com/couchbase/gomemcached v0.3.3 // indirect
	github.com/couchbase/goutils v0.1.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.36.0 // indirect
)
