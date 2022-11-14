package postgres

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	def "github.com/tknie/db/common"
)

type Mysql struct {
	def.CommonDatabase
	dbURL        string
	dbTableNames []string
}

func New(id def.RegDbID, url string) (def.Database, error) {
	mysql := &Mysql{def.CommonDatabase{RegDbID: id}, url, nil}
	err := mysql.check()
	if err != nil {
		return nil, err
	}
	return mysql, nil
}

func (mysql *Mysql) ID() def.RegDbID {
	return mysql.RegDbID
}

func (mysql *Mysql) URL() string {
	return mysql.dbURL
}
func (mysql *Mysql) Maps() ([]string, error) {

	return mysql.dbTableNames, nil
}

func (mysql *Mysql) check() error {
	db, err := sql.Open("mysql", mysql.dbURL)
	if err != nil {
		return def.NewError(3, err)
	}
	defer db.Close()

	mysql.dbTableNames = make([]string, 0)

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	tableName := ""
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			return err
		}
		mysql.dbTableNames = append(mysql.dbTableNames, tableName)
	}

	return nil
}

func (mysql *Mysql) Insert(fields []string, values []any) error {
	return def.NewError(65535)
}

func (mysql *Mysql) Query(search *def.Query, f def.ResultFunction) error {
	db, err := sql.Open("mysql", mysql.dbURL)
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
