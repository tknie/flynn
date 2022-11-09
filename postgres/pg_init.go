package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	def "github.com/tknie/db/definition"
)

type PostGres struct {
	def.CommonDatabase
	dbURL        string
	dbTableNames []string
}

func New(id def.RegDbID, url string) (*PostGres, error) {
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
	return make([]string, 0), nil
}

func (pg *PostGres) check() error {

	db, err := sql.Open("pgx", pg.dbURL)
	if err != nil {
		fmt.Println("Error db open:", err)
		return err
	}
	defer db.Close()

	pg.dbTableNames = make([]string, 0)

	rows, err := db.Query("SELECT table_name	FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
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
