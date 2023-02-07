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
	"database/sql"

	"github.com/tknie/log"
)

type RegDbID uint64

type Result struct {
	Fields []string
	Rows   []any
	Data   any
}

type Entries struct {
	Fields []string
	Update []string
	Values [][]any
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
	Insert(name string, insert *Entries) error
	Update(name string, insert *Entries) (int64, error)
	Delete(name string, remove *Entries) (int64, error)
	Batch(batch string) error
	Query(search *Query, f ResultFunction) (*Result, error)
	BeginTransaction() error
	Commit() error
	Rollback() error
}

type Column struct {
	Name       string
	DataType   DataType
	Length     uint16
	Digits     uint8
	SubColumns []*Column
}

type ResultFunction func(search *Query, result *Result) error

type CommonDatabase struct {
	RegDbID     RegDbID
	Transaction bool
}

func (cd *CommonDatabase) IsTransaction() bool {
	return cd.Transaction
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

// DeleteTable delete a table
func (id RegDbID) DeleteTable(tableName string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.DeleteTable(tableName)
}

// Batch batch SQL query in table
func (id RegDbID) Batch(batch string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Batch(batch)
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

func (result *Result) GenerateColumnByStruct(search *Query, rows *sql.Rows) (any, []any, error) {
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
