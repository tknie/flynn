package postgres

import (
	"database/sql"

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

func (pg *PostGres) Insert(insert *def.Entries) error {
	return def.NewError(65535)
}

func (pg *PostGres) Delete(remove *def.Entries) error {
	return def.NewError(65535)
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
