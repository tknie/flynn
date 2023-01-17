/*
* Copyright 2022 Thorsten A. Knieling
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
	Update(name string, insert *Entries) error
	Delete(name string, remove *Entries) error
	BatchSQL(batch string) error
	Query(search *Query, f ResultFunction) (*Result, error)
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

func (id RegDbID) SetCredentials(user, password string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.SetCredentials(user, password)
}

func (id RegDbID) Query(query *Query, f ResultFunction) (*Result, error) {

	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	Log.Debugf("Driver %T", driver)
	return driver.Query(query, f)
}

func (id RegDbID) CreateTable(tableName string, columns any) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.CreateTable(tableName, columns)
}

func (id RegDbID) DeleteTable(tableName string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.DeleteTable(tableName)
}

func (id RegDbID) BatchSQL(batch string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.BatchSQL(batch)
}

func (id RegDbID) Open() (any, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	return driver.Open()
}

func (id RegDbID) Close() {
	driver, err := searchDataDriver(id)
	if err != nil {
		return
	}
	driver.Close()
}

func (id RegDbID) Insert(name string, insert *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Insert(name, insert)
}

func (id RegDbID) Update(name string, insert *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Update(name, insert)
}

func (id RegDbID) Delete(name string, remove *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Delete(name, remove)
}

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
