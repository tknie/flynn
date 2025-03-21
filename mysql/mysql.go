//go:build !flynn_nomysql
// +build !flynn_nomysql

/*
* Copyright 2022-2024 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/common"
	"github.com/tknie/flynn/dbsql"
	"github.com/tknie/log"
)

const (
	layer             = "mysql"
	userPlaceholder   = "<user>"
	passwdPlaceholder = "<password>"
)

// Mysql instance for MySQL
type Mysql struct {
	common.CommonDatabase
	openDB any
	// dbURL        string
	dbTableNames []string
	password     string
	tx           *sql.Tx
	ctx          context.Context
}

// NewInstance create new mysql reference instance
func NewInstance(id common.RegDbID, reference *common.Reference, password string) (common.Database, error) {
	// o := reference.OptionString()
	// if o == "" {
	// 	o = "?"
	// } else {
	// 	o += "&"
	// }
	// o += "parseTime=true"
	// url := fmt.Sprintf("%s:"+passwdPlaceholder+"@tcp(%s:%d)/%s%s", reference.User, reference.Host,
	// 	reference.Port, reference.Database, o)
	mysql := &Mysql{common.NewCommonDatabase(id, "mysql"),
		nil, nil, password, nil, nil}
	mysql.ConRef = reference
	log.Log.Debugf("%s: create new instance", mysql.ID().String())
	return mysql, nil
}

// New create new mysql reference instance
func New(id common.RegDbID, url string) (common.Database, error) {
	ref, p, err := common.ParseUrl(url)
	if err != nil {
		return nil, err
	}
	mysql := &Mysql{common.NewCommonDatabase(id, "mysql"),
		nil, nil, p, nil, nil}
	mysql.ConRef = ref
	return mysql, nil
}

func (mysql *Mysql) Clone() common.Database {
	newMy := &Mysql{}
	*newMy = *mysql
	return newMy
}

// SetCredentials set credentials to connect to database
func (mysql *Mysql) SetCredentials(user, password string) error {
	mysql.ConRef.User = user
	// mysql.user = user
	mysql.password = password
	return nil
}

func (mysql *Mysql) generateURL() string {
	url := mysql.URL()
	if mysql.ConRef.User != "" {
		url = strings.Replace(url, userPlaceholder, mysql.ConRef.User, -1)
	}
	if mysql.password != "" {
		url = strings.Replace(url, passwdPlaceholder, mysql.password, -1)
	}
	return url
}

func (mysql *Mysql) open() (dbOpen any, err error) {
	if mysql.openDB == nil {
		log.Log.Debugf("%s: Open Mysql database to %s", mysql.ID().String(), mysql.URL())
		mysql.openDB, err = sql.Open(layer, mysql.generateURL())
		if err != nil {
			return
		}
	}
	log.Log.Debugf("Opened Mysql database")
	return mysql.openDB, nil
}

// Open open the database connection
func (mysql *Mysql) Open() (dbOpen any, err error) {
	dbOpen, err = mysql.open()
	if err != nil {
		log.Log.Debugf("%s: error open connection", mysql.ID().String(), err)
		return nil, err
	}
	db := dbOpen.(*sql.DB)

	if mysql.IsTransaction() {
		mysql.ctx = context.Background()
		mysql.tx, err = db.BeginTx(mysql.ctx, nil)
		if err != nil {
			log.Log.Debugf("%s: error begin transaction", mysql.ID().String(), err)
			return nil, err
		}

	}
	log.Log.Debugf("%s: Open MySQL database %s after transaction", mysql.ID().String(), mysql.URL())
	return db, nil
}

// StartTransaction start transaction the database connection
func (mysql *Mysql) BeginTransaction() error {
	if mysql.tx != nil && mysql.ctx != nil {
		return nil
	}
	var err error
	if mysql.openDB == nil {
		_, err = mysql.Open()
		if err != nil {
			log.Log.Debugf("%s: error open during transaction", mysql.ID().String(), err)
			return err
		}
	}
	_, _, err = mysql.StartTransaction()
	if err != nil {
		log.Log.Debugf("%s: error start transaction", mysql.ID().String(), err)
		return err
	}
	mysql.Transaction = true
	return nil
}

// EndTransaction end the transaction and commit if commit parameter is
// true.
func (mysql *Mysql) EndTransaction(commit bool) (err error) {
	if mysql.tx == nil && mysql.ctx == nil {
		return nil
	}
	log.Log.Debugf("%s: End transaction %p", mysql.ID().String(), mysql.tx)
	if mysql.IsTransaction() {
		return nil
	}
	log.Log.Debugf("%s: Commit/Rollback transaction %p commit = %v", mysql.ID().String(), mysql.tx, commit)
	if commit {
		err = mysql.tx.Commit()
	} else {
		err = mysql.tx.Rollback()
	}
	log.Log.Debugf("%s: ET: Reset Tx Transction %p", mysql.ID().String(), mysql.tx)
	mysql.tx = nil
	mysql.ctx = nil
	if err != nil {
		log.Log.Debugf("%s: error end transaction", mysql.ID().String(), err)
	}
	return
}

// Close close the database connection
func (mysql *Mysql) Close() {
	log.Log.Debugf("%s: Close MySQL", mysql.ID().String())
	if mysql.ctx != nil {
		mysql.EndTransaction(false)
	}
	if mysql.openDB != nil {
		mysql.openDB.(*sql.DB).Close()
		mysql.openDB = nil
		log.Log.Debugf("%s: Closed connection and reset transction variables %p", mysql.ID().String(), mysql.tx)
		mysql.tx = nil
		mysql.ctx = nil
	}
}

// FreeHandler don't use the driver anymore
func (mysql *Mysql) FreeHandler() {
	log.Log.Debugf("%s: free handler", mysql.ID().String())
}

// IndexNeeded index needed for the SELECT statement value reference
func (mysql *Mysql) IndexNeeded() bool {
	return false
}

// ByteArrayAvailable byte array available in SQL database
func (mysql *Mysql) ByteArrayAvailable() bool {
	return false
}

// Reference reference to mysql URL
func (mysql *Mysql) Reference() (string, string) {
	return "mysql", mysql.generateURL()
}

// ID current id used
func (mysql *Mysql) ID() common.RegDbID {
	return mysql.RegDbID
}

// URL current URL used
func (mysql *Mysql) URL() string {
	reference := mysql.ConRef
	o := reference.OptionString()
	if o == "" {
		o = "?"
	} else {
		o += "&"
	}
	o += "parseTime=true"
	url := fmt.Sprintf("%s:"+passwdPlaceholder+"@tcp(%s:%d)/%s%s", reference.User, reference.Host,
		reference.Port, reference.Database, o)
	return url
}

// Maps database maps, tables or views
func (mysql *Mysql) Maps() ([]string, error) {
	if mysql.dbTableNames == nil {
		err := mysql.Ping()
		if err != nil {
			log.Log.Debugf("%s: error reading maps", mysql.ID().String(), err)
			return nil, err
		}
	}
	return mysql.dbTableNames, nil
}

// Ping create short test database connection
func (mysql *Mysql) Ping() error {
	dbOpen, err := mysql.Open()
	if err != nil {
		return err
	}
	defer mysql.Close()

	db := dbOpen.(*sql.DB)

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

// Delete Delete database records
func (mysql *Mysql) Delete(name string, remove *common.Entries) (int64, error) {
	return dbsql.Delete(mysql, name, remove)
}

// GetTableColumn get table columne names
func (mysql *Mysql) GetTableColumn(tableName string) ([]string, error) {
	log.Log.Debugf("Get table column ...")
	dbOpen, err := mysql.Open()
	if err != nil {
		return nil, err
	}
	defer mysql.Close()

	db := dbOpen.(*sql.DB)
	// rows, err := db.Query(`SELECT table_schema, table_name, column_name, data_type
	// FROM INFORMATION_SCHEMA.COLUMNS
	rows, err := db.Query(`SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '` + strings.ToUpper(tableName) + `'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// c, err := rows.Columns()
	tableRows := make([]string, 0)
	tableRow := ""
	for rows.Next() {
		err = rows.Scan(&tableRow)
		if err != nil {
			return nil, err
		}
		tableRows = append(tableRows, strings.ToLower(tableRow))
	}

	return tableRows, nil
}

// Query query database records with search or SELECT
func (mysql *Mysql) Query(search *common.Query, f common.ResultFunction) (*common.Result, error) {
	search.Driver = common.MysqlType
	dbOpen, err := mysql.Open()
	if err != nil {
		return nil, err
	}
	defer mysql.Close()

	db := dbOpen.(*sql.DB)
	selectCmd, err := search.Select()
	if err != nil {
		return nil, err
	}
	log.Log.Debugf("Query: %s", selectCmd)
	rows, err := db.Query(selectCmd)
	if err != nil {
		log.Log.Debugf("%s: error query data", mysql.ID().String(), err)
		return nil, err
	}
	if search.DataStruct == nil {
		return search.ParseRows(rows, f)
	}
	return search.ParseStruct(rows, f)
}

// CreateTable create a new table
func (mysql *Mysql) CreateTable(name string, columns any) error {
	return dbsql.CreateTable(mysql, name, columns)
}

// AdaptTable create a new table
func (mysql *Mysql) AdaptTable(name string, newStruct any) error {
	return dbsql.AdaptTable(mysql, name, newStruct)
}

// DeleteTable delete a table
func (mysql *Mysql) DeleteTable(name string) error {
	return dbsql.DeleteTable(mysql, name)
}

// Insert insert record into table
func (mysql *Mysql) Insert(name string, insert *common.Entries) ([][]any, error) {
	return dbsql.Insert(mysql, name, insert)
}

// Update update record in table
func (mysql *Mysql) Update(name string, insert *common.Entries) ([][]any, int64, error) {
	return dbsql.Update(mysql, name, insert)
}

// Batch batch SQL query in table
func (mysql *Mysql) Batch(batch string) error {
	return dbsql.Batch(mysql, batch)
}

// BatchSelect batch SQL query in table with values returned
func (mysql *Mysql) BatchSelect(batch string) ([][]interface{}, error) {
	return dbsql.BatchSelect(mysql, batch)
}

// BatchSelectFct batch SQL query in table with fct called
func (mysql *Mysql) BatchSelectFct(search *common.Query, fct common.ResultFunction) error {
	dbOpen, err := mysql.Open()
	if err != nil {
		return err
	}
	defer mysql.Close()

	db := dbOpen.(*sql.DB)
	selectCmd := search.Search
	log.Log.Debugf("Query: %s", selectCmd)
	rows, err := db.Query(selectCmd, search.Parameters...)
	if err != nil {
		return err
	}
	if search.DataStruct == nil {
		_, err = search.ParseRows(rows, fct)
	} else {
		ti := common.CreateInterface(search.DataStruct, search.Fields)
		search.TypeInfo = ti
		_, err = search.ParseStruct(rows, fct)
	}
	return err
	// return dbsql.BatchSelectFct(mysql, batch, fct)
}

// StartTransaction start transaction
func (mysql *Mysql) StartTransaction() (*sql.Tx, context.Context, error) {
	_, err := mysql.open()
	if err != nil {
		return nil, nil, err
	}
	if mysql.tx != nil && mysql.IsTransaction() {
		return mysql.tx, mysql.ctx, nil
	}
	mysql.ctx = context.Background()
	mysql.tx, err = mysql.openDB.(*sql.DB).BeginTx(mysql.ctx, nil)
	if err != nil {
		mysql.ctx = nil
		mysql.tx = nil
		return nil, nil, err
	}
	log.Log.Debugf("Transaction tx=%p", mysql.tx)
	return mysql.tx, mysql.ctx, nil
}

// Commit commit the transaction
func (mysql *Mysql) Commit() error {
	mysql.Transaction = false
	log.Log.Debugf("Commit transaction %p", mysql.tx)
	return mysql.EndTransaction(true)
}

// Rollback rollback the transaction
func (mysql *Mysql) Rollback() error {
	mysql.Transaction = false
	return mysql.EndTransaction(false)
}

func (mysql *Mysql) Stream(search *common.Query, sf common.StreamFunction) error {
	dbOpen, err := mysql.Open()
	if err != nil {
		return err
	}
	defer mysql.Close()

	db := dbOpen.(*sql.DB)
	offset := int32(1)
	blocksize := search.Blocksize
	dataMaxLen := int32(math.MaxInt32)

	log.Log.Debugf("Start stream for %s for %s", search.Fields[0], search.TableName)
	selectCmd := fmt.Sprintf("SELECT SUBSTRING(%s FROM %d FOR %d),LENGTH(%s) FROM %s WHERE %s",
		search.Fields[0], offset, blocksize, search.Fields[0], search.TableName, search.Search)
	for offset < dataMaxLen {
		log.Log.Debugf("Query: %s", selectCmd)
		rows, err := db.Query(selectCmd)
		if err != nil {
			log.Log.Errorf("Stream query error: %v", err)
			return err
		}
		stream := &common.Stream{}
		stream.Data = make([]byte, 0)
		if !rows.Next() {
			log.Log.Errorf("rows missing")
			return errorrepo.NewError("DB000021")
		}
		if dataMaxLen == int32(math.MaxInt32) {
			err = rows.Scan(&stream.Data, &dataMaxLen)
		} else {
			err = rows.Scan(&stream.Data)
		}
		if err != nil {
			log.Log.Errorf("rows scan error: %s", err)
			return err
		}
		err = sf(search, stream)
		if err != nil {
			log.Log.Errorf("stream function error: %s", err)
			return err
		}
		offset += blocksize
		if offset >= dataMaxLen {
			break
		}
		if offset+blocksize > dataMaxLen {
			blocksize = dataMaxLen - offset + 1
		}

		selectCmd = fmt.Sprintf("SELECT SUBSTRING(%s FROM %d FOR %d) FROM %s WHERE %s",
			search.Fields[0], offset, blocksize, search.TableName, search.Search)
	}
	return nil
}
