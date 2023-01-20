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
	"strconv"
	"testing"

	def "github.com/tknie/flynn/common"
	"github.com/tknie/log"

	"github.com/stretchr/testify/assert"
)

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
	initLog()

	columns := make([]*def.Column, 0)
	columns = append(columns, &def.Column{Name: "Id", DataType: def.Alpha, Length: 8})
	columns = append(columns, &def.Column{Name: "Name", DataType: def.Alpha, Length: 10})
	columns = append(columns, &def.Column{Name: "FirstName", DataType: def.Alpha, Length: 20})

	for _, target := range getTestTargets(t) {
		fmt.Println("Work on " + target.layer)
		log.Log.Debugf("Work on " + target.layer)
		// if target.layer == "adabas" {
		// 	continue
		// }
		id, err := Register(target.layer, target.url)
		if !assert.NoError(t, err, "register fail using "+target.layer) {
			return
		}
		if target.layer != "adabas" {
			id.DeleteTable("TESTTABLE")
			err = id.CreateTable("TESTTABLE", columns)
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
		err = id.Insert("TESTTABLE", &def.Entries{Fields: []string{"Id", "Name", "FirstName"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		err = id.Delete("TESTTABLE", &def.Entries{Fields: []string{"Id"},
			Values: [][]any{{"TEST%"}}})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		if target.layer != "adabas" {
			deleteTable(t, id, "TESTTABLE", target.layer)
		}
		unregisterDatabase(t, id)
	}
}

func unregisterDatabase(t *testing.T, id def.RegDbID) {
	err := Unregister(id)
	assert.NoError(t, err)
}

func deleteTable(t *testing.T, id def.RegDbID, name, layer string) {
	err := id.DeleteTable(name)
	assert.NoError(t, err, "delete fail using "+layer)
}

func TestCreateStruct(t *testing.T) {
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
	for _, target := range getTestTargets(t) {
		fmt.Println("Work on " + target.layer)
		if target.layer == "adabas" {
			continue
		}
		id, err := Register(target.layer, target.url)
		if !assert.NoError(t, err, "register fail using "+target.layer) {
			return
		}
		id.DeleteTable("TESTTABLE")
		err = id.CreateTable("TESTTABLE", columns)
		if !assert.NoError(t, err, "create fail using "+target.layer) {
			unregisterDatabase(t, id)
			continue
		}
		list := make([][]any, 0)
		list = append(list, []any{"Eins", "Ernie"})
		for i := 1; i < 100; i++ {
			list = append(list, []any{strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i)})
		}
		list = append(list, []any{"Letztes", "Anton"})
		err = id.Insert("TESTTABLE", &def.Entries{Fields: []string{"name", "firstname"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		err = id.BatchSQL("SELECT NAME FROM TESTTABLE")
		assert.NoError(t, err, "select fail using "+target.layer)
		deleteTable(t, id, "TESTTABLE", target.layer)
		unregisterDatabase(t, id)
	}
}
