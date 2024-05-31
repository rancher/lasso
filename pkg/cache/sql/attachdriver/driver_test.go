package attachdriver

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestAttachDriver(t *testing.T) {
	Register(":memory:")

	sqlDB, err := sql.Open(Name, ":memory:")
	if err != nil {
		t.Errorf("sql.Open(): %v", err)
	}
	defer sqlDB.Close()

	schemas := []string{
		// default SQLite schema that always exists
		"main",
		// expected to be created by the attached driver
		"db2",
	}

	for _, schema := range schemas {
		// check that schema exists and works by creating a table and inserting a row
		_, err := sqlDB.Exec(fmt.Sprintf("CREATE TABLE %s.t (TEXT t)", schema))
		if err != nil {
			t.Errorf("sql.Exec(): %v", err)
		}

		r, err := sqlDB.Exec(fmt.Sprintf("INSERT INTO %s.t VALUES ('hello world')", schema))
		if err != nil {
			t.Errorf("sql.Exec(): %v", err)
		}

		c, err := r.RowsAffected()
		if err != nil {
			t.Errorf("RowsAffected(): %v", err)
		}
		if c != 1 {
			t.Errorf("INSERT INTO expected 1 row to be inserted into %v.t but got %v", schema, c)
		}
	}
}
