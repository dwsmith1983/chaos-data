package local

import "database/sql"

// DBForTest returns the underlying *sql.DB for direct test manipulation.
// Only available during testing (this file is compiled only with go test).
func (s *SQLiteState) DBForTest() *sql.DB {
	return s.db
}
