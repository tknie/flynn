package dbsql

import (
	"database/sql"
	"fmt"

	def "github.com/tknie/db/common"
)

type DBsql interface {
	Reference() (string, string)
	IndexNeeded() bool
}

func CreateTable(dbsql DBsql, name string, columns []*def.Column) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	createCmd := `CREATE TABLE ` + name + ` (`
	for i, c := range columns {
		if i > 0 {
			createCmd += ","
		}
		createCmd += c.Name
		switch c.DataType {
		case def.Alpha:
			createCmd += fmt.Sprintf(" %s(%d)\n", c.DataType.SqlType(), c.Length)
		default:
			return def.NewError(50001, "Data type unknown "+c.DataType.SqlType())
		}
	}
	createCmd += ")"
	def.Log.Debugf(url+": Create cmd", createCmd)
	_, err = db.Query(createCmd)
	if err != nil {
		return err
	}
	return nil
}

func DeleteTable(dbsql DBsql, name string) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Println("Drop table " + name)

	_, err = db.Query("DROP TABLE " + name)
	if err != nil {
		return err
	}
	return nil
}
