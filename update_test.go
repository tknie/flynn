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

package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/db/common"
)

func TestUpdateInit(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)
	nameValue := time.Now().Format("20060102150405")
	list := [][]any{{nameValue, "XXX"}}
	input := &common.Entries{Fields: []string{"ID", "Name"},
		Update: []string{"ID"},
		Values: list}
	err = x.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	list = [][]any{{nameValue, "YYY"}}
	input.Values = list
	err = x.Update(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}

	list = [][]any{{nameValue}}
	input.Fields = []string{"ID"}
	input.Values = list
	err = x.Delete(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
}
