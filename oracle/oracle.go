//go:build !flynn_nooracle
// +build !flynn_nooracle

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

package oracle

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"text/template"

	_ "github.com/godror/godror"
	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/common"
	"github.com/tknie/flynn/dbsql"
	"github.com/tknie/log"
)

const (
	layer             = "oracle"
	userPlaceholder   = "<user>"
	passwdPlaceholder = "<password>"
)

// Oracle instance for MySQL
type Oracle struct {
	common.CommonDatabase
	openDB       any
	Protocol     string
	Host         string
	Port         int
	ServiceName  string
	dbURL        string
	dbTableNames []string
	user         string
	password     string
	tx           *sql.Tx
	ctx          context.Context
}

const templateConnectString = `user="<user>" password="<password>"` +
	` connectString="(DESCRIPTION =(ADDRESS_LIST =` +
	`(ADDRESS =(PROTOCOL = {{ .Protocol}})` +
	`(HOST = {{ .Host}})(PORT = {{ .Port}})))` +
	`(CONNECT_DATA=(SERVICE_NAME = {{ .ServiceName}}))"`

// NewInstance create new oracle reference instance
func NewInstance(id common.RegDbID, reference *common.Reference, password string) (common.Database, error) {
	t, err := template.New("oracle").Parse(templateConnectString)
	if err != nil {
		panic(err)
	}

	oracle := &Oracle{common.CommonDatabase{RegDbID: id},
		nil, "TCP", reference.Host, reference.Port, reference.Database, templateConnectString, nil, reference.User, password, nil, nil}
	var buffer bytes.Buffer
	err = t.Execute(&buffer, oracle)
	if err != nil {
		panic(err)
	}
	oracle.dbURL = buffer.String()
	return oracle, nil
}

// New create new oracle reference instance
func New(id common.RegDbID, url string) (common.Database, error) {
	oracle := &Oracle{common.CommonDatabase{RegDbID: id},
		nil, "TCP", "", 1, "", url, nil, "", "", nil, nil}
	return oracle, nil
}

// SetCredentials set credentials to connect to database
func (oracle *Oracle) SetCredentials(user, password string) error {
	oracle.user = user
	oracle.password = password
	return nil
}

func (oracle *Oracle) generateURL() string {
	url := oracle.dbURL
	if oracle.user != "" {
		url = strings.Replace(url, userPlaceholder, oracle.user, -1)
	}
	if oracle.password != "" {
		url = strings.Replace(url, passwdPlaceholder, oracle.password, -1)
	}
	return url
}

func (oracle *Oracle) open() (dbOpen any, err error) {
	if oracle.openDB == nil {
		log.Log.Debugf("Open Oracle database to %s", oracle.dbURL)
		log.Log.Debugf("Oracle database to %s", oracle.generateURL())
		oracle.openDB, err = sql.Open(layer, oracle.generateURL())
		if err != nil {
			return
		}
	}
	log.Log.Debugf("Opened Oracle database")
	return oracle.openDB, nil
}

// Open open the database connection
func (oracle *Oracle) Open() (dbOpen any, err error) {
	dbOpen, err = oracle.open()
	if err != nil {
		return nil, err
	}
	db := dbOpen.(*sql.DB)

	if oracle.IsTransaction() {
		oracle.ctx = context.Background()
		oracle.tx, err = db.BeginTx(oracle.ctx, nil)
		if err != nil {
			return nil, err
		}

	}
	log.Log.Debugf("Open database %s after transaction", oracle.dbURL)
	return db, nil
}

// FreeHandler don't use the driver anymore
func (oracle *Oracle) FreeHandler() {
}

// StartTransaction start transaction the database connection
func (oracle *Oracle) BeginTransaction() error {
	if oracle.tx != nil && oracle.ctx != nil {
		return nil
	}
	var err error
	if oracle.openDB == nil {
		_, err = oracle.Open()
		if err != nil {
			return err
		}
	}
	_, _, err = oracle.StartTransaction()
	if err != nil {
		return err
	}
	oracle.Transaction = true
	return nil
}

func (oracle *Oracle) EndTransaction(commit bool) (err error) {
	if oracle.tx == nil && oracle.ctx == nil {
		return nil
	}
	if oracle.IsTransaction() {
		return nil
	}
	if commit {
		err = oracle.tx.Commit()
	} else {
		err = oracle.tx.Rollback()
	}
	oracle.tx = nil
	oracle.ctx = nil
	return
}

// Close close the database connection
func (oracle *Oracle) Close() {
	log.Log.Debugf("Close Oracle")
	if oracle.ctx != nil {
		oracle.EndTransaction(false)
	}
	if oracle.openDB != nil {
		oracle.openDB.(*sql.DB).Close()
		oracle.openDB = nil
		oracle.tx = nil
		oracle.ctx = nil
	}
}

// IndexNeeded index needed for the SELECT statement value reference
func (oracle *Oracle) IndexNeeded() bool {
	return false
}

// ByteArrayAvailable byte array available in SQL database
func (oracle *Oracle) ByteArrayAvailable() bool {
	return false
}

// Reference reference to oracle URL
func (oracle *Oracle) Reference() (string, string) {
	return "oracle", oracle.generateURL()
}

// ID current id used
func (oracle *Oracle) ID() common.RegDbID {
	return oracle.RegDbID
}

// URL current URL used
func (oracle *Oracle) URL() string {
	return oracle.dbURL
}

// Maps database maps, tables or views
func (oracle *Oracle) Maps() ([]string, error) {
	if oracle.dbTableNames == nil {
		err := oracle.Ping()
		if err != nil {
			return nil, err
		}
	}
	return oracle.dbTableNames, nil
}

// Ping create short test database connection
func (oracle *Oracle) Ping() error {
	dbOpen, err := oracle.Open()
	if err != nil {
		return err
	}
	defer oracle.Close()

	db := dbOpen.(*sql.DB)

	oracle.dbTableNames = make([]string, 0)

	rows, err := db.Query("SELECT owner, table_name FROM all_tables")
	if err != nil {
		return err
	}
	owner := ""
	tableName := ""
	for rows.Next() {
		err = rows.Scan(&owner, &tableName)
		if err != nil {
			return err
		}
		oracle.dbTableNames = append(oracle.dbTableNames, owner+"."+tableName)
	}

	return nil
}

// Delete Delete database records
func (oracle *Oracle) Delete(name string, remove *common.Entries) (int64, error) {
	return dbsql.Delete(oracle, name, remove)
}

// GetTableColumn get table columne names
func (oracle *Oracle) GetTableColumn(tableName string) ([]string, error) {
	return nil, errorrepo.NewError("DB065535")
}

// Query query database records with search or SELECT
func (oracle *Oracle) Query(search *common.Query, f common.ResultFunction) (*common.Result, error) {
	dbOpen, err := oracle.Open()
	if err != nil {
		return nil, err
	}
	defer oracle.Close()

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
func (oracle *Oracle) CreateTable(name string, columns any) error {
	return dbsql.CreateTable(oracle, name, columns)
}

// DeleteTable delete a table
func (oracle *Oracle) DeleteTable(name string) error {
	return dbsql.DeleteTable(oracle, name)
}

// Insert insert record into table
func (oracle *Oracle) Insert(name string, insert *common.Entries) error {
	return dbsql.Insert(oracle, name, insert)
}

// Update update record in table
func (oracle *Oracle) Update(name string, insert *common.Entries) (int64, error) {
	return dbsql.Update(oracle, name, insert)
}

// Batch batch SQL query in table
func (oracle *Oracle) Batch(batch string) error {
	return dbsql.Batch(oracle, batch)
}

// BatchSelect batch SQL query in table with values returned
func (oracle *Oracle) BatchSelect(batch string) ([][]interface{}, error) {
	return dbsql.BatchSelect(oracle, batch)
}

// BatchSelectFct batch SQL query in table with fct called
func (oracle *Oracle) BatchSelectFct(search *common.Query, fct common.ResultFunction) error {
	dbOpen, err := oracle.Open()
	if err != nil {
		return err
	}
	defer oracle.Close()

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
		_, err = search.ParseStruct(rows, fct)
	}
	return err
	// return dbsql.BatchSelectFct(mysql, batch, fct)	return dbsql.BatchSelectFct(oracle, batch, fct)
}

// StartTransaction start transaction
func (oracle *Oracle) StartTransaction() (*sql.Tx, context.Context, error) {
	_, err := oracle.open()
	if err != nil {
		return nil, nil, err
	}
	oracle.ctx = context.Background()
	oracle.tx, err = oracle.openDB.(*sql.DB).BeginTx(oracle.ctx, nil)
	if err != nil {
		oracle.ctx = nil
		oracle.tx = nil
		return nil, nil, err
	}
	return oracle.tx, oracle.ctx, nil
}

// Commit commit the transaction
func (oracle *Oracle) Commit() error {
	oracle.Transaction = false
	return oracle.EndTransaction(true)
}

// Rollback rollback the transaction
func (oracle *Oracle) Rollback() error {
	oracle.Transaction = false
	return oracle.EndTransaction(false)
}

func (oracle *Oracle) Stream(search *common.Query, sf common.StreamFunction) error {
	dbOpen, err := oracle.Open()
	if err != nil {
		return err
	}
	defer oracle.Close()

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
