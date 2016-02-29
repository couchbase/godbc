package godbc

import "database/sql"

// Wrap an instance of sql.Stmt to present interface godbc.Stmt.

type stmtAdaptor struct {
	sqlStmt *sql.Stmt
}

func (adaptor *stmtAdaptor) Close() error {
	err := adaptor.sqlStmt.Close()
	return err
}

func (adaptor *stmtAdaptor) Exec(args ...interface{}) (Result, error) {
	sResult, err := adaptor.sqlStmt.Exec(args...)
	if err != nil {
		return nil, err
	}
	return &resultAdaptor{sqlResult: sResult}, err
}

func (adaptor *stmtAdaptor) Query(args ...interface{}) (Rows, error) {
	rows, err := adaptor.sqlStmt.Query(args...)
	return rows, err
}

func (adaptor *stmtAdaptor) QueryRow(args ...interface{}) Row {
	row := adaptor.sqlStmt.QueryRow(args...)
	return row
}
