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

package flynn

import (
	"fmt"
	"strconv"
	"testing"

	def "github.com/tknie/flynn/common"
	"github.com/tknie/log"

	"github.com/stretchr/testify/assert"
)

const testCreationTable = "TESTTABLE"
const testCreationTableStruct = "TESTTABLESTRUCT"

type target struct {
	layer string
	url   string
}

func getTestTargets(t *testing.T) (targets []*target) {
	url, err := mysqlTarget(t)
	if !assert.NoError(t, err) {
		return nil
	}
	targets = append(targets, &target{"mysql", url})
	url, err = postgresTarget(t)
	if !assert.NoError(t, err) {
		return nil
	}
	targets = append(targets, &target{"postgres", url})
	url, err = adabasTarget(t)
	if !assert.NoError(t, err) {
		return nil
	}
	targets = append(targets, &target{"adabas", url})
	return
}

func TestCreateStringArray(t *testing.T) {
	InitLog(t)

	columns := make([]*def.Column, 0)
	columns = append(columns, &def.Column{Name: "Id", DataType: def.Alpha, Length: 8})
	columns = append(columns, &def.Column{Name: "Name", DataType: def.Alpha, Length: 10})
	columns = append(columns, &def.Column{Name: "FirstName", DataType: def.Alpha, Length: 20})

	for _, target := range getTestTargets(t) {
		fmt.Println("Working at string creation on target " + target.layer)
		log.Log.Debugf("Working at string creation on target " + target.layer)

		id, err := Register(target.layer, target.url)
		if !assert.NoError(t, err, "register fail using "+target.layer) {
			return
		}
		if target.layer == "adabas" {
			_, err := id.Delete(testCreationTable, &def.Entries{Fields: []string{"%Id"},
				Values: [][]any{{"TEST%"}}})
			if !assert.NoError(t, err, "DELETE") {
				return
			}
		}
		if target.layer != "adabas" {
			id.DeleteTable(testCreationTable)
			err = id.CreateTable(testCreationTable, columns)
			if !assert.NoError(t, err, "create fail using "+target.layer) {
				unregisterDatabase(t, id)
				return
			}
		}
		count := 1
		list := make([][]any, 0)
		list = append(list, []any{"TEST" + strconv.Itoa(count), "Eins", "Ernie"})
		for i := 1; i < 100; i++ {
			count++
			list = append(list, []any{"TEST" + strconv.Itoa(count), strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i)})
		}
		count++
		list = append(list, []any{"TEST" + strconv.Itoa(count), "Letztes", "Anton"})
		err = id.Insert(testCreationTable, &def.Entries{Fields: []string{"Id", "Name", "FirstName"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		log.Log.Debugf("Delete TEST records")
		dr, err := id.Delete(testCreationTable, &def.Entries{Fields: []string{"%Id"},
			Values: [][]any{{"TEST%"}}})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		count++
		log.Log.Debugf("Delete of records done")
		tId := "TEST" + strconv.Itoa(count)
		list = append(list, []any{tId, "Tom", "Terminal"})
		err = id.Insert(testCreationTable, &def.Entries{Fields: []string{"Id", "Name", "FirstName"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		dr, err = id.Delete(testCreationTable, &def.Entries{Criteria: "Id='" + tId + "'"})
		if !assert.NoError(t, err, "delete fail using "+target.layer) {
			return
		}
		assert.Equal(t, int64(1), dr)
		if target.layer != "adabas" {
			deleteTable(t, id, testCreationTable, target.layer)
		}
		unregisterDatabase(t, id)
	}
}

func unregisterDatabase(t *testing.T, id def.RegDbID) {
	log.Log.Debugf("Unregister id=%d", id)
	err := Unregister(id)
	assert.NoError(t, err)
}

func deleteTable(t *testing.T, id def.RegDbID, name, layer string) {
	log.Log.Debugf("Delete table %s", name)
	err := id.DeleteTable(name)
	assert.NoError(t, err, "delete fail using "+layer)
}

func TestCreateStruct(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	for _, target := range getTestTargets(t) {
		log.Log.Debugf("Work on target %#v", target)
		err := createStruct(t, target)
		assert.NoError(t, err)
	}
}

func createStruct(t *testing.T, target *target) error {
	columns := struct {
		XY        uint64 `dbsql:"ID::SERIAL"`
		Name      string
		FirstName string
		LastName  string
		Address   string `dbsql:"Street"`
		Salary    uint64 `dbsql:"Salary"`
		Bonus     int64
	}{Name: "Gellanger",
		FirstName: "Bob"}
	log.Log.Debugf("Working on creating with target " + target.layer)
	if target.layer == "adabas" {
		return nil
	}
	id, err := Register(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return err
	}
	defer unregisterDatabase(t, id)

	log.Log.Debugf("Delete table: {}", testCreationTableStruct)
	err = id.DeleteTable(testCreationTableStruct)
	log.Log.Debugf("Delete table: %s returns with %v", testCreationTableStruct, err)
	err = id.CreateTable(testCreationTableStruct, columns)
	if !assert.NoError(t, err, "create fail using "+target.layer) {
		return err
	}
	list := make([][]any, 0)
	list = append(list, []any{"Eins", "Ernie"})
	for i := 1; i < 100; i++ {
		list = append(list, []any{strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i)})
	}
	list = append(list, []any{"Letztes", "Anton"})
	err = id.Insert(testCreationTableStruct, &def.Entries{Fields: []string{"name", "firstname"},
		Values: list})
	if !assert.NoError(t, err, "insert fail using "+target.layer) {
		return err
	}
	log.Log.Debugf("Inserting into table: %s", testCreationTableStruct)
	err = id.Batch("SELECT NAME FROM " + testCreationTableStruct)
	assert.NoError(t, err, "select fail using "+target.layer)
	log.Log.Debugf("Deleting table: %s", testCreationTableStruct)
	deleteTable(t, id, testCreationTableStruct, target.layer)
	return nil
}
