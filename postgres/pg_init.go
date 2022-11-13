package postgres

import (
	"database/sql"
	"fmt"
	"time"

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
		fmt.Println("Error db open:", err)
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

func (pg *PostGres) Insert(fields []string, values []any) error {
	return fmt.Errorf("not implemented")
}

func (pg *PostGres) Query(search *def.Query, f def.ResultFunction) error {
	db, err := sql.Open("pgx", pg.dbURL)
	if err != nil {
		fmt.Println("Error db open:", err)
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
	// fmt.Println("Query:", selectCmd)
	rows, err := db.Query(selectCmd)
	if err != nil {
		return err
	}
	return queryRows(search, rows, f)
}

func queryRows(search *def.Query, rows *sql.Rows, f def.ResultFunction) (err error) {
	result := &def.Result{}

	result.Data = search.DataStruct
	if search.DataStruct == nil {
		result.Rows, err = generateColumnByValues(rows)
	} else {
		err = result.GenerateColumnByStruct(search, rows)
	}
	if err != nil {
		return err
	}
	for rows.Next() {
		err := rows.Scan(result.Rows...)
		if err != nil {
			return err
		}
		err = f(search, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateColumnByValues(rows *sql.Rows) ([]any, error) {
	colsType, err := rows.ColumnTypes()
	if err != nil {
		fmt.Println("Error cols read:", err)
		return nil, err
	}
	colsValue := make([]any, 0)
	for nr, col := range colsType {
		len, ok := col.Length()
		fmt.Println(nr, "name=", col.Name(), "len=", len, "ok=", ok)
		switch col.DatabaseTypeName() {
		case "VARCHAR2":
			s := ""
			colsValue = append(colsValue, &s)
		case "NUMBER":
			s := int64(0)
			colsValue = append(colsValue, &s)
		case "LONG":
			s := ""
			colsValue = append(colsValue, &s)
		case "DATE":
			n := time.Now()
			colsValue = append(colsValue, &n)
		default:
			s := sql.NullString{}
			colsValue = append(colsValue, &s)
		}
	}
	return colsValue, nil
}
