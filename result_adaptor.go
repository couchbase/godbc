package godbc

import "database/sql"

// Wrap an instance of sql.Result to present interace godbc.Result.

type resultAdaptor struct {
	sqlResult sql.Result
}

func (adaptor *resultAdaptor) LastInsertId() (int64, error) {
	return adaptor.sqlResult.LastInsertId()
}

func (adaptor *resultAdaptor) RowsAffected() (int64, error) {
	return adaptor.sqlResult.RowsAffected()
}

func (adaptor *resultAdaptor) Rows() Rows {
	return nil
}
