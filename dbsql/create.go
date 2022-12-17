package dbsql

import (
	"bytes"
	"database/sql"
	"fmt"

	def "github.com/tknie/db/common"
)

type DBsql interface {
	Reference() (string, string)
	IndexNeeded() bool
	ByteArrayAvailable() bool
}

func CreateTable(dbsql DBsql, name string, col any) error {
	//	columns []*def.Column
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	createCmd := `CREATE TABLE ` + name + ` (`
	switch columns := col.(type) {
	case []*def.Column:
		createCmd += createTableByColumns(dbsql, columns)
	default:
		c, err := createTableByStruct(dbsql, col)
		if err != nil {
			return err
		}
		createCmd += c
	}
	createCmd += ")"
	fmt.Println(createCmd)
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

func createTableByColumns(dbsql DBsql, columns []*def.Column) string {
	var buffer bytes.Buffer
	for i, c := range columns {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(c.Name + " ")
		switch c.DataType {
		case def.Alpha, def.Bit:
			buffer.WriteString(c.DataType.SqlType(c.Length))
		case def.Decimal:
			buffer.WriteString(c.DataType.SqlType(c.Length, c.Digits))
		case def.Bytes:
			buffer.WriteString(c.DataType.SqlType(dbsql.ByteArrayAvailable(),
				c.Length))
		default:
			buffer.WriteString(c.DataType.SqlType())
		}
	}
	return buffer.String()
}

func createTableByStruct(dbsql DBsql, columns any) (string, error) {
	fmt.Println("Create table by structs")
	return def.SqlDataType(columns)
}

func BatchSQL(dbsql DBsql, batch string) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	// TODO
	rows, err := db.Query(batch)
	if err != nil {
		return err
	}
	for rows.Next() {
		fmt.Println(rows.Err())
		fmt.Println(rows.ColumnTypes())
	}
	return nil
}
