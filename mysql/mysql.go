//go:build !flynn_nomysql
// +build !flynn_nomysql

/*
* Copyright 2022-2023 Thorsten A. Knieling
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
	openDB       any
	dbURL        string
	dbTableNames []string
	user         string
	password     string
	tx           *sql.Tx
	ctx          context.Context
}

// NewInstance create new mysql reference instance
func NewInstance(id common.RegDbID, reference *common.Reference, password string) (common.Database, error) {
	o := reference.OptionString()
	if o == "" {
		o = "?"
	} else {
		o += "&"
	}
	o += "parseTime=true"
	url := fmt.Sprintf("%s:"+passwdPlaceholder+"@tcp(%s:%d)/%s%s", reference.User, reference.Host,
		reference.Port, reference.Database, o)
	mysql := &Mysql{common.CommonDatabase{RegDbID: id},
		nil, url, nil, reference.User, password, nil, nil}
	return mysql, nil
}

// New create new mysql reference instance
func New(id common.RegDbID, url string) (common.Database, error) {
	mysql := &Mysql{common.CommonDatabase{RegDbID: id},
		nil, url, nil, "", "", nil, nil}
	return mysql, nil
}

// SetCredentials set credentials to connect to database
func (mysql *Mysql) SetCredentials(user, password string) error {
	mysql.user = user
	mysql.password = password
	return nil
}

func (mysql *Mysql) generateURL() string {
	url := mysql.dbURL
	if mysql.user != "" {
		url = strings.Replace(url, userPlaceholder, mysql.user, -1)
	}
	if mysql.password != "" {
		url = strings.Replace(url, passwdPlaceholder, mysql.password, -1)
	}
	return url
}

func (mysql *Mysql) open() (dbOpen any, err error) {
	if mysql.openDB == nil {
		log.Log.Debugf("Open Mysql database to %s", mysql.dbURL)
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
		return nil, err
	}
	db := dbOpen.(*sql.DB)

	if mysql.IsTransaction() {
		mysql.ctx = context.Background()
		mysql.tx, err = db.BeginTx(mysql.ctx, nil)
		if err != nil {
			return nil, err
		}

	}
	log.Log.Debugf("Open database %s after transaction", mysql.dbURL)
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
			return err
		}
	}
	_, _, err = mysql.StartTransaction()
	if err != nil {
		return err
	}
	mysql.Transaction = true
	return nil
}

func (mysql *Mysql) EndTransaction(commit bool) (err error) {
	if mysql.tx == nil && mysql.ctx == nil {
		return nil
	}
	log.Log.Debugf("End transaction %p", mysql.tx)
	if mysql.IsTransaction() {
		return nil
	}
	log.Log.Debugf("Commit/Rollback transaction %p", mysql.tx)
	if commit {
		err = mysql.tx.Commit()
	} else {
		err = mysql.tx.Rollback()
	}
	log.Log.Debugf("ET: Reset Tx Transction %p", mysql.tx)
	mysql.tx = nil
	mysql.ctx = nil
	return
}

// Close close the database connection
func (mysql *Mysql) Close() {
	log.Log.Debugf("Close MySQL")
	if mysql.ctx != nil {
		mysql.EndTransaction(false)
	}
	if mysql.openDB != nil {
		mysql.openDB.(*sql.DB).Close()
		mysql.openDB = nil
		log.Log.Debugf("Close Tx Transction %p", mysql.tx)
		mysql.tx = nil
		mysql.ctx = nil
	}
}

// FreeHandler don't use the driver anymore
func (mysql *Mysql) FreeHandler() {
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
	return mysql.dbURL
}

// Maps database maps, tables or views
func (mysql *Mysql) Maps() ([]string, error) {
	if mysql.dbTableNames == nil {
		err := mysql.Ping()
		if err != nil {
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
	return nil, errorrepo.NewError("DB065535")
}

// Query query database records with search or SELECT
func (mysql *Mysql) Query(search *common.Query, f common.ResultFunction) (*common.Result, error) {
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

// DeleteTable delete a table
func (mysql *Mysql) DeleteTable(name string) error {
	return dbsql.DeleteTable(mysql, name)
}

// Insert insert record into table
func (mysql *Mysql) Insert(name string, insert *common.Entries) error {
	return dbsql.Insert(mysql, name, insert)
}

// Update update record in table
func (mysql *Mysql) Update(name string, insert *common.Entries) (int64, error) {
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
	rows, err := db.Query(selectCmd)
	if err != nil {
		return err
	}
	if search.DataStruct == nil {
		_, err = search.ParseRows(rows, fct)
	} else {
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
			return fmt.Errorf("rows read missing")
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
