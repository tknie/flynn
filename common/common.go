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

package common

import (
	"fmt"
	"os"
	"strings"

	"github.com/tknie/errorrepo"
	"github.com/tknie/log"
)

type CreateStatus byte

const (
	CreateError CreateStatus = iota
	CreateExists
	CreateCreated
	CreateDriver
	CreateConnError
)

type RegDbID uint64

type Result struct {
	Counter uint64
	Fields  []string
	Header  []*Column
	Rows    []any
	Data    any
}

type Stream struct {
	Data []byte
}

type Entries struct {
	Fields     []string
	DataStruct any
	Update     []string
	Values     [][]any
	Criteria   string
}

type Database interface {
	ID() RegDbID
	URL() string
	Ping() error
	SetCredentials(string, string) error
	Maps() ([]string, error)
	GetTableColumn(tableName string) ([]string, error)
	CreateTable(string, any) error
	DeleteTable(string) error
	Open() (any, error)
	Close()
	FreeHandler()
	Insert(name string, insert *Entries) error
	Update(name string, insert *Entries) (int64, error)
	Delete(name string, remove *Entries) (int64, error)
	Batch(batch string) error
	BatchSelect(batch string) ([][]interface{}, error)
	BatchSelectFct(search *Query, f ResultFunction) error
	Query(search *Query, f ResultFunction) (*Result, error)
	BeginTransaction() error
	Commit() error
	Rollback() error
	Stream(search *Query, sf StreamFunction) error
}

type Column struct {
	Name       string
	DataType   DataType
	Length     uint16
	Digits     uint8
	SubColumns []*Column
}

type ResultFunction func(search *Query, result *Result) error

// type ResultDataFunction func(index uint64, header []*Column, result []interface{}) error
type StreamFunction func(search *Query, stream *Stream) error

type CommonDatabase struct {
	RegDbID     RegDbID
	Transaction bool
}

func (cd *CommonDatabase) IsTransaction() bool {
	return cd.Transaction
}

func (id RegDbID) String() string {
	return fmt.Sprintf("ID:%04d", id)
}

// SetCredentials set credentials to connect to database
func (id RegDbID) SetCredentials(user, password string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.SetCredentials(user, password)
}

// Query query database records with search or SELECT
func (id RegDbID) Query(query *Query, f ResultFunction) (*Result, error) {

	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	log.Log.Debugf("Driver %T", driver)
	return driver.Query(query, f)
}

// CreateTable create a new table
func (id RegDbID) CreateTable(tableName string, columns any) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.CreateTable(tableName, columns)
}

// CreateTable create a new table
func (id RegDbID) CreateTableIfNotExists(tableName string, columns any) (CreateStatus, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return CreateDriver, err
	}
	dbTables, err := driver.Maps()
	if err != nil {
		if dbTables == nil {
			return CreateConnError, err
		}
		return CreateError, err
	}
	for _, d := range dbTables {
		if d == tableName {
			return CreateExists, nil
		}
	}

	err = driver.CreateTable(tableName, columns)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return CreateExists, nil
		}
		return CreateError, err
	}
	return CreateCreated, nil
}

// DeleteTable delete a table
func (id RegDbID) DeleteTable(tableName string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.DeleteTable(tableName)
}

// Batch batch SQL with no return data in table
func (id RegDbID) Batch(batch string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Batch(batch)
}

// BatchSelect batch SQL query in table
func (id RegDbID) BatchSelect(batch string) ([][]interface{}, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	return driver.BatchSelect(batch)
}

// BatchSelect batch SQL query in table calling function
func (id RegDbID) BatchSelectFct(batch *Query, f ResultFunction) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.BatchSelectFct(batch, f)
}

// Open open the database connection
func (id RegDbID) Open() (any, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	return driver.Open()
}

// Close close the database connection
func (id RegDbID) Close() {
	log.Log.Debugf("Close regDbId xxxx")
	driver, err := searchDataDriver(id)
	if err != nil {
		return
	}
	driver.Close()
}

// Ping create short test database connection
func (id RegDbID) Ping() error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Ping()
}

// Insert insert record into table
func (id RegDbID) Insert(name string, insert *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Insert(name, insert)
}

// Update update record in table
func (id RegDbID) Update(name string, insert *Entries) (int64, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return 0, err
	}
	return driver.Update(name, insert)
}

// Delete Delete database records
func (id RegDbID) Delete(name string, remove *Entries) (int64, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return 0, err
	}
	return driver.Delete(name, remove)
}

// GetTableColumn get table columne names
func (id RegDbID) GetTableColumn(tableName string) ([]string, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	return driver.GetTableColumn(tableName)
}

func (result *Result) GenerateColumnByStruct(search *Query) (any, []any, error) {
	ti := search.TypeInfo.(*typeInterface)
	copy, values := ti.CreateQueryValues()
	result.Rows = ti.RowValues
	result.Data = ti.DataType
	return copy, values, nil
}

// BeginTransaction begin a transaction
func (id RegDbID) BeginTransaction() error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.BeginTransaction()
}

// Commit transaction commit
func (id RegDbID) Commit() error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Commit()
}

// Rollback transaction rollback
func (id RegDbID) Rollback() error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Rollback()
}

// URL URL string
func (id RegDbID) URL() string {
	driver, err := searchDataDriver(id)
	if err != nil {
		return "Error: " + err.Error()
	}
	return driver.URL()
}

// Stream streaming data from a field
func (id RegDbID) Stream(search *Query, sf StreamFunction) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Stream(search, sf)

}

// RegisterDbClient register database
func RegisterDbClient(db Database) {
	log.Log.Debugf("Lock common")
	handlerLock.Lock()
	defer handlerLock.Unlock()
	defer log.Log.Debugf("Unlock common")

	Databases = append(Databases, db)
}

// FreeHandler unregister registry id for the driver
func (id RegDbID) FreeHandler() error {
	log.Log.Debugf("Lock common (unregister)")
	handlerLock.Lock()
	defer handlerLock.Unlock()
	defer log.Log.Debugf("Unlock common (unregister)")
	log.Log.Debugf("FreeHandler db before state of %s(%d): %v", id, len(Databases), DBHelper())
	for i, d := range Databases {
		if d.ID() == id {
			log.Log.Debugf("FreeHandler db id=%d", d.ID())
			d.FreeHandler()
			newDatabases := make([]Database, 0)
			if i > 0 {
				newDatabases = append(newDatabases, Databases[0:i]...)
			}
			if len(Databases)-1 > i {
				newDatabases = append(newDatabases, Databases[i+1:]...)
			}
			Databases = newDatabases
			log.Log.Debugf("FreeHandler db of id=%d(%d): %v", id, len(Databases), DBHelper())
			return nil
		}
	}
	log.Log.Debugf("FreeHandler db error of id=%d(%d): %v", id, len(Databases), DBHelper())
	return errorrepo.NewError("DB000001")
}

func DBHelper() string {
	if os.Getenv("FLYNN_TRACE_PASSWORD") == "TRUE" {

		dbs := make([]RegDbID, 0)
		for _, d := range Databases {
			dbs = append(dbs, d.ID())
		}
		return fmt.Sprintf("%v", dbs)
	}
	return "-"
}
