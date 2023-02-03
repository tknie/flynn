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
)

func TestUpdateInit(t *testing.T) {
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
	x, err := Register(target.layer, target.url)
	if !assert.NoError(t, err) {
		return err
	}
	defer Unregister(x)
	nameValue := time.Now().Format("20060102150405")
	vId1 := nameValue + "-1"
	vId2 := nameValue + "-2"
	list := [][]any{{vId1, "aaadasfdsnaflksdnf", 1}, {vId2, "dmfklsfgmskdlmgsmgls", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	err = x.Insert(testStructTable, input)
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
		assert.Equal(t, int64(2), ra)
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

func TestUpdateTransaction(t *testing.T) {
	url, _ := postgresTarget(t)
	fmt.Println("Start transaction update test for layer")
	x, err := Register("postgres", url)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	x.BeginTransaction()
	nameValue := time.Now().Format("20060102150405")
	vId1 := "x-" + nameValue + "-1"
	vId2 := "x-" + nameValue + "-2"
	list := [][]any{{vId1, "xxxxxx", 1}, {vId2, "yyywqwqwqw", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update: []string{"ID"},
		Values: list}
	err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	vId1b := "x-" + nameValue + "-1"
	vId2b := "x-" + nameValue + "-2"
	input.Values = [][]any{{vId1b, "jhhhhmmmmm", 1}, {vId2b, "ppppoiierer", 2}}
	err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	x.Rollback()
}
