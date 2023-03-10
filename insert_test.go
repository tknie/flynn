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

package db

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

const testTable = "TestTableData"
const testStructTable = "TestStructTableData"

type TestData struct {
	ID          string    `dbsql:"::10"`
	Name        string    `dbsql:"::200"`
	MiddleName  string    `dbsql:"::50"`
	FirstName   string    `dbsql:"::50"`
	PersonnelNo uint64    `dbsql:"::4"`
	CardNo      [8]byte   `dbsql:"::8"`
	Signature   string    `dbsql:"::20"`
	Sex         string    `dbsql:"::1"`
	MarrieState string    `dbsql:"::1"`
	Street      string    `dbsql:"::200"`
	Address     string    `dbsql:"::200"`
	City        string    `dbsql:"::200"`
	PostCode    string    `dbsql:"::10"`
	Birth       time.Time `dbsql:"::10"`
	Account     float64   `dbsql:"::10, Digits: 2"`
	Description string    `dbsql:"::0"`
	Flags       byte      `dbsql:"::8"`
	AreaCode    int       `dbsql:"::8"`
	Phone       int       `dbsql:"::8"`
	Department  string    `dbsql:"::6"`
	JobTitle    string    `dbsql:"::20"`
	Currency    string    `dbsql:"::2"`
	Salary      uint64    `dbsql:"::8"`
	Bonus       uint64    `dbsql:"::8"`
	LeaveDue    uint64    `dbsql:"::2"`
	LeaveTaken  uint64    `dbsql:"::2"`
	LeaveStart  time.Time
	LeaveEnd    time.Time
	Language    uint64 `dbsql:"::8"`
}

func TestInsertInitTestTable(t *testing.T) {
	for _, target := range getTestTargets(t) {
		if target.layer == "adabas" {
			continue
		}
		if checkTableAvailablefunc(t, target) != nil {
			return
		}
		if checkStructTableAvailablefunc(t, target) != nil {
			return
		}
	}
}

func checkTableAvailablefunc(t *testing.T, target *target) error {
	x, err := Register(target.layer, target.url)
	if !assert.NoError(t, err) {
		return err
	}
	defer Unregister(x)

	q := &common.Query{TableName: testTable,
		Search: "",
		Fields: []string{"Name"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		counter++
		return nil
	})
	if err == nil {
		return nil
	}
	if counter == 0 {
		err = createColumnTestTable(t, target)
		if !assert.NoError(t, err) {
			return err
		}
	}
	return nil
}

func createColumnTestTable(t *testing.T, target *target) error {
	columns := make([]*common.Column, 0)
	columns = append(columns, &common.Column{Name: "ID", DataType: common.Alpha, Length: 10})
	columns = append(columns, &common.Column{Name: "Name", DataType: common.Alpha, Length: 200})
	columns = append(columns, &common.Column{Name: "MiddleName", DataType: common.Alpha, Length: 50})
	columns = append(columns, &common.Column{Name: "FirstName", DataType: common.Alpha, Length: 50})
	columns = append(columns, &common.Column{Name: "PersonnelNo", DataType: common.Number, Length: 4})
	columns = append(columns, &common.Column{Name: "CardNo", DataType: common.Bytes, Length: 8})
	columns = append(columns, &common.Column{Name: "Signature", DataType: common.Alpha, Length: 20})
	columns = append(columns, &common.Column{Name: "Sex", DataType: common.Alpha, Length: 1})
	columns = append(columns, &common.Column{Name: "MarrieState", DataType: common.Alpha, Length: 1})
	columns = append(columns, &common.Column{Name: "Street", DataType: common.Alpha, Length: 200})
	columns = append(columns, &common.Column{Name: "Address", DataType: common.Alpha, Length: 200})
	columns = append(columns, &common.Column{Name: "City", DataType: common.Alpha, Length: 200})
	columns = append(columns, &common.Column{Name: "PostCode", DataType: common.Alpha, Length: 10})
	columns = append(columns, &common.Column{Name: "Birth", DataType: common.Date, Length: 10})
	columns = append(columns, &common.Column{Name: "Account", DataType: common.Decimal, Length: 10, Digits: 2})
	columns = append(columns, &common.Column{Name: "Description", DataType: common.Text, Length: 0})
	columns = append(columns, &common.Column{Name: "Flags", DataType: common.Bit, Length: 8})
	columns = append(columns, &common.Column{Name: "AreaCode", DataType: common.Integer, Length: 8})
	columns = append(columns, &common.Column{Name: "Phone", DataType: common.Integer, Length: 8})
	columns = append(columns, &common.Column{Name: "Department", DataType: common.Alpha, Length: 6})
	columns = append(columns, &common.Column{Name: "JobTitle", DataType: common.Alpha, Length: 20})
	columns = append(columns, &common.Column{Name: "Currency", DataType: common.Alpha, Length: 2})
	columns = append(columns, &common.Column{Name: "Salary", DataType: common.Integer, Length: 8})
	columns = append(columns, &common.Column{Name: "Bonus", DataType: common.Integer, Length: 8})
	columns = append(columns, &common.Column{Name: "LeaveDue", DataType: common.Integer, Length: 2})
	columns = append(columns, &common.Column{Name: "LeaveTaken", DataType: common.Integer, Length: 2})
	columns = append(columns, &common.Column{Name: "LeaveStart", DataType: common.Date})
	columns = append(columns, &common.Column{Name: "LeaveEnd", DataType: common.Date})
	columns = append(columns, &common.Column{Name: "Language", DataType: common.Integer, Length: 8})

	fmt.Println("Create database table")

	id, err := Register(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return err
	}
	defer unregisterDatabase(t, id)
	id.DeleteTable(testTable)
	err = id.CreateTable(testTable, columns)
	if !assert.NoError(t, err, "create test table fail using "+target.layer) {
		return err
	}
	return nil
}

func checkStructTableAvailablefunc(t *testing.T, target *target) error {
	x, err := Register(target.layer, target.url)
	if !assert.NoError(t, err) {
		return err
	}
	defer Unregister(x)

	q := &common.Query{TableName: testStructTable,
		Search: "",
		Fields: []string{"Name"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		counter++
		return nil
	})
	if err == nil {
		return nil
	}
	if counter == 0 {
		err = createStructTestTable(t, target)
		if !assert.NoError(t, err) {
			return err
		}
	}
	return nil
}

func createStructTestTable(t *testing.T, target *target) error {
	log.Log.Debugf("Create database table")

	id, err := Register(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return err
	}
	defer unregisterDatabase(t, id)
	id.DeleteTable(testStructTable)
	err = id.CreateTable(testStructTable, &TestData{})
	if !assert.NoError(t, err, "create test table fail using "+target.layer) {
		return err
	}
	return nil
}
