//go:build flynn_nomysql
// +build flynn_nomysql

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
	"math"

	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/common"
)

type mysql struct {
}

// NewInstance create new postgres reference instance
func NewInstance(id common.RegDbID, reference *common.Reference, password string) (common.Database, error) {
	return nil, errorrepo.NewError("DB065535")
}

// New create new postgres reference instance
func New(id common.RegDbID, url string) (common.Database, error) {
	return nil, errorrepo.NewError("DB065535")
}

// SetCredentials set credentials to connect to database
func (ada *mysql) SetCredentials(user, password string) error {
	return errorrepo.NewError("DB065535")
}

// ID current id used
func (ada *mysql) ID() common.RegDbID {
	return math.MaxUint64
}

// URL current URL used
func (ada *mysql) URL() string {
	return ""
}

// Maps database maps, tables or views
func (ada *mysql) Maps() ([]string, error) {
	return nil, errorrepo.NewError("DB065535")
}

// Ping create short test database connection
func (ada *mysql) Ping() error {
	return errorrepo.NewError("DB065535")
}

// Open open the database connection
func (ada *mysql) Open() (any, error) {
	return nil, errorrepo.NewError("DB065535")
}

// Close close the database connection
func (ada *mysql) Close() {
}

// Insert insert record into table
func (ada *mysql) Insert(name string, insert *common.Entries) error {
	return errorrepo.NewError("DB065535")
}

// Update update record in table
func (ada *mysql) Update(name string, insert *common.Entries) (int64, error) {
	return 0, errorrepo.NewError("DB065535")
}

// Delete Delete database records
func (ada *mysql) Delete(name string, remove *common.Entries) (int64, error) {
	return 0, errorrepo.NewError("DB065535")
}

// GetTableColumn get table columne names
func (ada *mysql) GetTableColumn(tableName string) ([]string, error) {
	return nil, errorrepo.NewError("DB065535")
}

// Query query database records with search or SELECT
func (ada *mysql) Query(search *common.Query, f common.ResultFunction) (*common.Result, error) {
	return nil, errorrepo.NewError("DB065535")
}

// CreateTable create a new table
func (ada *mysql) CreateTable(string, any) error {
	return errorrepo.NewError("DB065535")
}

// DeleteTable delete a table
func (ada *mysql) DeleteTable(string) error {
	return errorrepo.NewError("DB065535")
}

// Batch batch SQL query in table
func (ada *mysql) Batch(batch string) error {
	return errorrepo.NewError("DB065535")
}

// BatchSelect batch SQL query in table with values returned
func (ada *mysql) BatchSelect(batch string) ([][]interface{}, error) {
	return nil, errorrepo.NewError("DB065535")
}

// BatchSelectFct batch SQL query in table with fct called
func (ada *mysql) BatchSelectFct(*common.Query, common.ResultFunction) error {
	return errorrepo.NewError("DB065535")
}

func (ada *mysql) BeginTransaction() error {
	return errorrepo.NewError("DB065535")
}

func (ada *mysql) Commit() error {
	return errorrepo.NewError("DB065535")
}

func (ada *mysql) Rollback() error {
	return errorrepo.NewError("DB065535")
}

func (ada *mysql) Stream(search *common.Query, sf common.StreamFunction) error {
	return errorrepo.NewError("DB065535")
}
