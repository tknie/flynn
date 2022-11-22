package postgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	def "github.com/tknie/db/common"
)

type PostGres struct {
	def.CommonDatabase
	dbURL        string
	dbTableNames []string
}

func New(id def.RegDbID, url string) (def.Database, error) {
	pg := &PostGres{def.CommonDatabase{RegDbID: id}, url, nil}
	err := pg.check()
	if err != nil {
		return nil, err
	}
	return pg, nil
}

func (pg *PostGres) Reference() (string, string) {
	return "pgx", pg.dbURL
}

func (pg *PostGres) ID() def.RegDbID {
	return pg.RegDbID
}

func (pg *PostGres) URL() string {
	return pg.dbURL
}
func (pg *PostGres) Maps() ([]string, error) {

	return pg.dbTableNames, nil
}

func (pg *PostGres) check() error {

	db, err := sql.Open("pgx", pg.dbURL)
	if err != nil {
		return def.NewError(3, err)
	}
	defer db.Close()

	pg.dbTableNames = make([]string, 0)

	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		return err
	}
	tableName := ""
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			return err
		}
		pg.dbTableNames = append(pg.dbTableNames, tableName)
	}

	return nil
}

func (pg *PostGres) Delete(name string, remove *def.Entries) error {
	return def.NewError(65535)
}

func (pg *PostGres) GetTableColumn(tableName string) ([]string, error) {
	db, err := sql.Open("pgx", pg.dbURL)
	if err != nil {
		return nil, def.NewError(3, err)
	}
	defer db.Close()

	// rows, err := db.Query(`SELECT table_schema, table_name, column_name, data_type
	// FROM INFORMATION_SCHEMA.COLUMNS
	rows, err := db.Query(`SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '` + strings.ToLower(tableName) + `'`)
	if err != nil {
		return nil, err
	}
	// c, err := rows.Columns()
	tableRows := make([]string, 0)
	tableRow := ""
	for rows.Next() {
		err = rows.Scan(&tableRow)
		if err != nil {
			return nil, err
		}
		tableRows = append(tableRows, tableRow)
	}

	return tableRows, nil
}

func (pg *PostGres) Query(search *def.Query, f def.ResultFunction) error {
	db, err := sql.Open("pgx", pg.dbURL)
	if err != nil {
		return err
	}
	selectCmd := ""
	defer db.Close()
	switch {
	case search.DataStruct != nil:
		selectCmd = "select "
		ti := def.CreateInterface(search.DataStruct)
		search.TypeInfo = ti
		selectCmd += ti.CreateQueryFields()
		selectCmd += " from " + search.TableName
	case search.Search == "":
		selectCmd = "select "
		for i, s := range search.Fields {
			if i > 0 {
				s += ","
			}
			selectCmd += s
		}
		selectCmd += " from " + search.TableName
	default:
		selectCmd = search.Search
	}
	rows, err := db.Query(selectCmd)
	if err != nil {
		return err
	}
	return search.QueryRows(rows, f)
}

func (pg *PostGres) CreateTable(name string, columns []*def.Column) error {
	db, err := sql.Open("pgx", pg.dbURL)
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
	def.Log.Debugf(pg.dbURL+": Create cmd", createCmd)
	_, err = db.Query(createCmd)
	if err != nil {
		return err
	}
	return nil
}

func (pg *PostGres) DeleteTable(name string) error {
	db, err := sql.Open("pgx", pg.dbURL)
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

func (pg *PostGres) Insert(name string, insert *def.Entries) error {
	db, err := sql.Open("pgx", pg.dbURL)
	if err != nil {
		return err
	}
	defer db.Close()

	insertCmd := "INSERT INTO " + name + " ("
	values := "("
	for i, field := range insert.Fields {
		if i > 0 {
			insertCmd += ","
			values += ","
		}
		insertCmd += field
		values += "$" + strconv.Itoa(i+1)
	}
	values += ")"
	insertCmd += ") VALUES " + values
	for _, v := range insert.Values {
		fmt.Printf("%s: %#v\n", insertCmd, v)
		av := v.([]any)
		_, err = db.Exec(insertCmd, av...)
		if err != nil {
			return err
		}
	}
	return nil
}
