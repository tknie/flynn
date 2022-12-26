package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tknie/db/common"
	def "github.com/tknie/db/common"
	"github.com/tknie/db/dbsql"
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

func (mysql *Mysql) IndexNeeded() bool {
	return false
}

func (mysql *Mysql) ByteArrayAvailable() bool {
	return false
}

func (mysql *Mysql) Reference() (string, string) {
	return "mysql", mysql.dbURL
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

func (mysql *Mysql) Insert(name string, insert *def.Entries) error {
	return dbsql.Insert(mysql, name, insert)
}

func (mysql *Mysql) Delete(name string, remove *def.Entries) error {
	return def.NewError(65535)
}

func (mysql *Mysql) GetTableColumn(tableName string) ([]string, error) {
	return nil, def.NewError(65535)
}

func (mysql *Mysql) Query(search *def.Query, f def.ResultFunction) (*common.Result, error) {
	common.Log.Debugf("Query mysql database")
	db, err := sql.Open("mysql", mysql.dbURL+"?parseTime=true")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	selectCmd := search.Select()

	common.Log.Debugf("Query: %s", selectCmd)
	rows, err := db.Query(selectCmd)
	if err != nil {
		return nil, err
	}
	if search.DataStruct == nil {
		return search.ParseRows(rows, f)
	}
	return search.ParseStruct(rows, f)
}

func (mysql *Mysql) CreateTable(name string, columns any) error {
	return dbsql.CreateTable(mysql, name, columns)
}

func (mysql *Mysql) DeleteTable(name string) error {
	return dbsql.DeleteTable(mysql, name)
}

func (mysql *Mysql) BatchSQL(batch string) error {
	return dbsql.BatchSQL(mysql, batch)
}
