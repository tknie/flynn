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

package flynn

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

type TestSub struct {
	SubName string
	XCreate time.Time
}

type TestData struct {
	ID          string    `flynn:"::25"`
	Name        string    `flynn:"::200"`
	MiddleName  string    `flynn:"::50"`
	FirstName   string    `flynn:"::50"`
	PersonnelNo uint64    `flynn:"::4"`
	CardNo      [8]byte   `flynn:"::8"`
	Signature   string    `flynn:"::20"`
	Sex         string    `flynn:"::1"`
	MarrieState string    `flynn:"::1"`
	Street      string    `flynn:"::200"`
	Address     string    `flynn:"::200"`
	City        string    `flynn:"::200"`
	PostCode    string    `flynn:"::10"`
	Birth       time.Time `flynn:"::10"`
	Account     float64   `flynn:"::10, Digits: 2"`
	Description string    `flynn:"::0"`
	Flags       byte      `flynn:"::8"`
	AreaCode    int       `flynn:"::8"`
	Phone       int       `flynn:"::8"`
	Department  string    `flynn:"::6"`
	JobTitle    string    `flynn:"::20"`
	Currency    string    `flynn:"::2"`
	Salary      uint64    `flynn:"::8"`
	Bonus       uint64    `flynn:"::8"`
	LeaveDue    uint64    `flynn:"::2"`
	LeaveTaken  uint64    `flynn:"::2"`
	Blob        string    `flynn:"BB:BLOB:2048"`
	LobData     []byte
	LeaveStart  time.Time
	LeaveEnd    time.Time
	Language    uint64 `flynn:"::8"`
	Sub         *TestSub
}

func TestInsertInitTestTable(t *testing.T) {
	InitLog(t)
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
		if fillStructTestTable(t, target) != nil {
			return
		}
	}
	finalCheck(t, 0)
}

func checkTableAvailablefunc(t *testing.T, target *target) error {
	x, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err) {
		return err
	}
	defer x.FreeHandler()

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

	id, err := Handle(target.layer, target.url)
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
	x, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err) {
		return err
	}
	defer x.FreeHandler()
	err = x.DeleteTable(testStructTable)
	log.Log.Debugf("Deleting table ok: %v", err)

	q := &common.Query{TableName: testStructTable,
		Search: "",
		Fields: []string{"Name"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		counter++
		return nil
	})
	if err == nil {
		log.Log.Debugf("Table still available %s", testStructTable)
		return nil
	}
	//	if counter == 0 {
	err = createStructTestTable(t, target)
	if !assert.NoError(t, err) {
		return err
	}
	//	}
	return nil
}

func createStructTestTable(t *testing.T, target *target) error {
	log.Log.Debugf("Create database table -> %s", target.layer)

	id, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return err
	}
	defer unregisterDatabase(t, id)
	err = id.DeleteTable(testStructTable)
	log.Log.Debugf("DELETE TABLE: %v", err)
	err = id.CreateTable(testStructTable, &TestData{Sub: &TestSub{}})
	if !assert.NoError(t, err, "create test table fail using "+target.layer) {
		return err
	}
	return nil
}

func fillStructTestTable(t *testing.T, target *target) error {
	log.Log.Debugf("Create database table -> %s", target.layer)

	id, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return err
	}
	defer unregisterDatabase(t, id)
	data := &TestData{ID: "1", Name: "NAME", LobData: []byte{1, 2, 3, 4, 5},
		Sub: &TestSub{XCreate: time.Now()}}
	input := &common.Entries{Fields: []string{"ID", "Name", "LobData", "Created"},
		DataStruct: data,
		Update:     []string{"ID", "LobData"},
		Values:     [][]any{{data}}}
	_, err = id.Insert(testStructTable, input)
	assert.NoError(t, err)
	return err
}

func TestInsertStruct(t *testing.T) {
	url, _ := postgresTarget(t)
	target := &target{"postgres", url}
	x, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()
	nameValue := time.Now().Format("20060102150405")
	vId1 := "i-" + nameValue + "-1"
	vId2 := "i-" + nameValue + "-2"
	list := [][]any{{vId1, "aaadasfdsnaflksdnf", 1}, {vId2, "dmfklsfgmskdlmgsmgls", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	finalCheck(t, 1)
}

func TestInsertMap(t *testing.T) {
	InitLog(t)

	url, _ := postgresTarget(t)
	target := &target{"postgres", url}
	x, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()
	nameValue := time.Now().Format("20060102150405")
	m1 := make(map[string]interface{})
	m2 := make(map[string]interface{})
	m1["ID"] = "i-" + nameValue + "-1"
	m2["ID"] = "i-" + nameValue + "-2"
	m1["Name"] = "a"
	m2["Name"] = "x"
	list := [][]any{{m1, m2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID", "Name"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	nameValue = time.Now().Format("20060102150405")
	m1 = make(map[string]interface{})
	m2 = make(map[string]interface{})
	m1["ID"] = "o-" + nameValue + "-1"
	m2["ID"] = "o-" + nameValue + "-2"
	m1["Name"] = "adkjfsjf"
	m2["Name"] = "x30ie03i"
	m1["account"] = 123
	m2["account"] = 33
	list = [][]any{{m1, m2}}
	input = &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID", "Name"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	query := &common.Query{Fields: []string{"ID", "Name", "account"},
		TableName: testStructTable,
		Search:    "ID = '" + m1["ID"].(string) + "' OR ID = '" + m2["ID"].(string) + "'"}
	count := 0
	_, err = x.Query(query, func(search *common.Query, result *common.Result) error {
		fmt.Println(result.Rows...)
		count++
		return nil
	})
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 2, count)
	finalCheck(t, 1)
}
