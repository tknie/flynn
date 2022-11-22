package dbsql

import (
	"database/sql"
	"strconv"

	def "github.com/tknie/db/common"
)

func Insert(dbsql DBsql, name string, insert *def.Entries) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()

	insertCmd := "INSERT INTO " + name + " ("
	values := "("

	indexNeed := dbsql.IndexNeeded()
	for i, field := range insert.Fields {
		if i > 0 {
			insertCmd += ","
			values += ","
		}
		insertCmd += field
		if indexNeed {
			values += "$" + strconv.Itoa(i+1)
		} else {
			values += "?"
		}
	}
	values += ")"
	insertCmd += ") VALUES " + values
	for _, v := range insert.Values {
		av := v.([]any)
		_, err = db.Exec(insertCmd, av...)
		if err != nil {
			return err
		}
	}
	return nil
}
