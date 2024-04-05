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
)

func TestUpdateInit(t *testing.T) {
	InitLog(t)
	for _, target := range getTestTargets(t) {
		if target.layer == "adabas" {
			continue
		}
		err := updateTest(t, target)
		if err != nil {
			return
		}
	}
}

func updateTest(t *testing.T, target *target) error {
	fmt.Println("Start update test for layer", target.layer)
	x, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err) {
		return err
	}
	defer x.FreeHandler()
	nameValue := time.Now().Format("20060102150405")
	vId1 := "uT-" + nameValue + "-1"
	vId2 := "uT-" + nameValue + "-2"
	list := [][]any{{vId1, "aaadasfdsnaflksdnf", 1}, {vId2, "dmfklsfgmskdlmgsmgls", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return err
	}
	list = [][]any{{vId1, "changeValue", 2323}, {vId2, "mfngkfngkfngk changed", 87766}}
	input.Values = list
	ra, err := x.Update(testStructTable, input)
	if !assert.NoError(t, err) {
		return err
	}
	// 'mysql' does not provide affected rows
	if target.layer != "mysql" {
		if !assert.Equal(t, int64(2), ra) {
			return fmt.Errorf("error updating...")
		}
	}

	list = [][]any{{vId1}, {vId2}}
	input.Fields = []string{"ID"}
	input.Values = list
	dr, err := x.Delete(testStructTable, input)
	if !assert.NoError(t, err) {
		return err
	}
	assert.Equal(t, int64(1), dr)
	return nil
}

func TestPostgresUpdateRollbackTransaction(t *testing.T) {
	InitLog(t)
	url, _ := postgresTarget(t)
	fmt.Println("Start postgres transaction update test for layer")
	x, err := Handle("postgres", url)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	err = x.Batch("TRUNCATE TABLE " + testStructTable)
	assert.NoError(t, err)

	err = x.BeginTransaction()
	if !assert.NoError(t, err) {
		return
	}
	nameValue := time.Now().Format("20060102")
	vId1 := "x-" + nameValue + "-1"
	vId2 := "x-" + nameValue + "-2"
	list := [][]any{{vId1, "xxxxxx", 1}, {vId2, "yyywqwqwqw", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	vId1b := "y-" + nameValue + "-3"
	vId2b := "y-" + nameValue + "-4"
	input.Values = [][]any{{vId1b, "jhhhhmmmmm", 1}, {vId2b, "ppppoiierer", 2}}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	err = x.Rollback()
	if !assert.NoError(t, err) {
		return
	}

	q := &common.Query{TableName: testStructTable,
		Search: "",
		Fields: []string{"ID"}}
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", result.Rows[0].(string))
		return fmt.Errorf("found fail, should not come here, record should be rollbacked")
	})
	assert.NoError(t, err)
}

func TestPostgresTransaction(t *testing.T) {
	InitLog(t)
	url, _ := postgresTarget(t)
	fmt.Println("Start postgres transaction update test for layer")
	x, err := Handle("postgres", url)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	err = x.Batch("TRUNCATE TABLE " + testStructTable)
	assert.NoError(t, err)

	err = x.BeginTransaction()
	if !assert.NoError(t, err) {
		return
	}
	nameValue := time.Now().Format("20060102")
	vId1 := "t-" + nameValue + "-1"
	vId2 := "t-" + nameValue + "-2"
	list := [][]any{{vId1, "xxxxxx", 1}, {vId2, "yyywqwqwqw", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	vId1b := "u-" + nameValue + "-3"
	vId2b := "u-" + nameValue + "-4"
	input.Values = [][]any{{vId1b, "jhhhhmmmmm", 1}, {vId2b, "ppppoiierer", 2}}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	newID := "a-" + nameValue + "-1111"
	input.Values = [][]any{{newID}}
	input.Fields = []string{"ID"}
	input.Update[0] = "ID='" + vId1b + "'"
	_, err = x.Update(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	record := struct {
		ID         string
		Name       string
		MiddleName string
		City       string
	}{ID: "2221111", Name: "Wolfen", MiddleName: "Otto", City: "Hongkong"}
	input = &common.Entries{DataStruct: record, Fields: []string{"*"}}
	input.Update = []string{"ID='" + newID + "'"}
	_, err = x.Update(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	err = x.Commit()
	if !assert.NoError(t, err) {
		return
	}

	q := &common.Query{TableName: testStructTable,
		Search: "ID='" + newID + "'",
		Fields: []string{"ID"}}
	count := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", result.Rows[0].(string))
		count++
		return nil
	})
	assert.Equal(t, count, 0)
	assert.NoError(t, err)

	q = &common.Query{TableName: testStructTable,
		Search: "ID='2221111'",
		Fields: []string{"ID"}}
	count = 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", result.Rows[0].(string))
		count++
		return nil
	})
	assert.Equal(t, count, 1)
	assert.NoError(t, err)

	q = &common.Query{TableName: testStructTable,
		Search: "MiddleName='Otto'",
		Fields: []string{"ID"}}
	count = 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", result.Rows[0].(string))
		count++
		return nil
	})
	assert.Equal(t, count, 1)
	assert.NoError(t, err)

	q = &common.Query{TableName: testStructTable,
		DataStruct: record,
		Search:     "MiddleName='Otto'",
		Fields:     []string{"*"}}
	count = 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", *(result.Rows[2].(*string)), *(result.Rows[3].(*string)))
		assert.Equal(t, "Otto", *(result.Rows[2].(*string)))
		assert.Equal(t, "Hongkong", *(result.Rows[3].(*string)))
		count++
		return nil
	})
	assert.Equal(t, count, 1)
	assert.NoError(t, err)
}

func TestMySQLUpdateTransaction(t *testing.T) {
	InitLog(t)
	url, _ := mysqlTarget(t)
	fmt.Println("Start mySQL transaction update test for layer")
	x, err := Handle("mysql", url)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	err = x.Batch("TRUNCATE TABLE " + testStructTable)
	assert.NoError(t, err)

	err = x.BeginTransaction()
	if !assert.NoError(t, err) {
		return
	}
	nameValue := time.Now().Format("20060102")
	vId1 := "x-" + nameValue + "-1"
	vId2 := "x-" + nameValue + "-2"
	list := [][]any{{vId1, "xxxxxx", 1}, {vId2, "yyywqwqwqw", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	vId1b := "y-" + nameValue + "-3"
	vId2b := "y-" + nameValue + "-4"
	input.Values = [][]any{{vId1b, "jhhhhmmmmm", 1}, {vId2b, "ppppoiierer", 2}}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	err = x.Rollback()
	if !assert.NoError(t, err) {
		return
	}

	q := &common.Query{TableName: testStructTable,
		Search: "",
		Fields: []string{"ID"}}
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		return fmt.Errorf("found fail")
	})
	assert.NoError(t, err)
}

func TestMySQLTransaction(t *testing.T) {
	InitLog(t)
	url, _ := mysqlTarget(t)
	fmt.Println("Start MySQL transaction update test for layer")
	x, err := Handle("mysql", url)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	err = x.Batch("TRUNCATE TABLE " + testStructTable)
	assert.NoError(t, err)

	err = x.BeginTransaction()
	if !assert.NoError(t, err) {
		return
	}
	nameValue := time.Now().Format("20060102")
	vId1 := "t-" + nameValue + "-1"
	vId2 := "t-" + nameValue + "-2"
	list := [][]any{{vId1, "xxxxxx", 1}, {vId2, "yyywqwqwqw", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	vId1b := "u-" + nameValue + "-3"
	vId2b := "u-" + nameValue + "-4"
	input.Values = [][]any{{vId1b, "jhhhhmmmmm", 1}, {vId2b, "ppppoiierer", 2}}
	_, err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	newID := "a-" + nameValue + "-1111"
	input.Values = [][]any{{newID}}
	input.Fields = []string{"ID"}
	input.Update[0] = "ID='" + vId1b + "'"
	_, err = x.Update(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	record := struct {
		ID         string
		Name       string
		MiddleName string
		City       string
	}{ID: "2221111", Name: "Wolfen", MiddleName: "Otto", City: "Hongkong"}
	input = &common.Entries{DataStruct: record, Fields: []string{"*"}}
	input.Update = []string{"ID='" + newID + "'"}
	_, err = x.Update(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	err = x.Commit()
	if !assert.NoError(t, err) {
		return
	}

	q := &common.Query{TableName: testStructTable,
		Search: "ID='" + newID + "'",
		Fields: []string{"ID"}}
	count := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", result.Rows[0].(string))
		count++
		return nil
	})
	assert.Equal(t, count, 0)
	assert.NoError(t, err)

	q = &common.Query{TableName: testStructTable,
		Search: "ID='2221111'",
		Fields: []string{"ID"}}
	count = 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", result.Rows[0].(string))
		count++
		return nil
	})
	assert.Equal(t, count, 1)
	assert.NoError(t, err)

	q = &common.Query{TableName: testStructTable,
		Search: "MiddleName='Otto'",
		Fields: []string{"ID"}}
	count = 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", result.Rows[0].(string))
		count++
		return nil
	})
	assert.Equal(t, count, 1)
	assert.NoError(t, err)

	q = &common.Query{TableName: testStructTable,
		DataStruct: record,
		Search:     "MiddleName='Otto'",
		Fields:     []string{"*"}}
	count = 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		fmt.Println("Result", *(result.Rows[2].(*string)), *(result.Rows[3].(*string)))
		assert.Equal(t, "Otto", *(result.Rows[2].(*string)))
		assert.Equal(t, "Hongkong", *(result.Rows[3].(*string)))
		count++
		return nil
	})
	assert.Equal(t, count, 1)
	assert.NoError(t, err)
}
