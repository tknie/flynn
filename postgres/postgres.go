package postgres

import (
	"database/sql"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tknie/db/common"
	def "github.com/tknie/db/common"
	"github.com/tknie/db/dbsql"
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

func (pg *PostGres) IndexNeeded() bool {
	return true
}

func (pg *PostGres) ByteArrayAvailable() bool {
	return true
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

func (pg *PostGres) Query(search *def.Query, f def.ResultFunction) (*common.Result, error) {
	db, err := sql.Open("pgx", pg.dbURL)
	if err != nil {
		return nil, err
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
				selectCmd += ","
			}
			selectCmd += s
		}
		selectCmd += " from " + search.TableName
	default:
		selectCmd = search.Search
	}
	if search.Search != "" {
		selectCmd += " where " + search.Search
	}
	rows, err := db.Query(selectCmd)
	if err != nil {
		return nil, err
	}
	return search.ParseRows(rows, f)
}

func (pg *PostGres) CreateTable(name string, columns any) error {
	return dbsql.CreateTable(pg, name, columns)
}

func (pg *PostGres) DeleteTable(name string) error {
	return dbsql.DeleteTable(pg, name)
}

func (pg *PostGres) Insert(name string, insert *def.Entries) error {
	return dbsql.Insert(pg, name, insert)
}

func (pg *PostGres) BatchSQL(batch string) error {
	return dbsql.BatchSQL(pg, batch)
}
