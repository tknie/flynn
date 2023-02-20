/*
* Copyright 2023 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package dbsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
)

func TestSQLUpdate(t *testing.T) {
	ui := &common.Entries{
		Fields: []string{"ABC", "BCD", "YYY"},
		Update: []string{"ABC"},
		Values: [][]any{{"abc", 123, 233}},
	}
	sqlCmd, rows := generateUpdate(true, "ABC", ui)
	assert.Equal(t, "UPDATE ABC SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{0}, rows)
	wh := createWhere(0, ui, rows)
	assert.Equal(t, "\"abc\"='abc'", wh)

	ui.Update[0] = "BCD"
	sqlCmd, rows = generateUpdate(true, "DFX", ui)
	assert.Equal(t, "UPDATE DFX SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{1}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "\"bcd\"=123", wh)

	ui.Update[0] = "BCXD=hugo"
	sqlCmd, rows = generateUpdate(true, "Table1", ui)
	assert.Equal(t, "UPDATE Table1 SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "BCXD=hugo", wh)

	ui.Update[0] = "DDD=emil"
	ui.Update = append(ui.Update, "YYY")
	sqlCmd, rows = generateUpdate(true, "Table2", ui)
	assert.Equal(t, "UPDATE Table2 SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{2}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "DDD=emil AND \"yyy\"=233", wh)

	ui.Update[0] = "YYY=emil"
	ui.Update = append(ui.Update, "ABC")
	ui.Update = append(ui.Update, "WWW=abc")
	sqlCmd, rows = generateUpdate(true, "Table3", ui)
	assert.Equal(t, "UPDATE Table3 SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{0, 2}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "YYY=emil AND WWW=abc AND \"abc\"='abc' AND \"yyy\"=233", wh)

	ui.Fields = []string{"AA", "BB", "CC", "DD", "TT"}
	ui.Values = [][]any{{"XXX", "daslkds", 123, 222, 222, time.Now()}, {"XXX2", "aaa2", 51, 522, 5222, time.Now()}}
	ui.Update = []string{"YY=otto", "AA", "CC", "TT"}
	sqlCmd, rows = generateUpdate(true, "Table4", ui)
	assert.Equal(t, "UPDATE Table4 SET \"aa\"=$1,\"bb\"=$2,\"cc\"=$3,\"dd\"=$4,\"tt\"=$5 WHERE ", sqlCmd)
	assert.Equal(t, []int{0, 2, 4}, rows)
	wh = createWhere(0, ui, rows)
	assert.Equal(t, "YY=otto AND \"aa\"='XXX' AND \"cc\"=123 AND \"tt\"=222", wh)
	wh = createWhere(1, ui, rows)
	assert.Equal(t, "YY=otto AND \"aa\"='XXX2' AND \"cc\"=51 AND \"tt\"=5222", wh)
}

func TestSQLDelete(t *testing.T) {
	ui := &common.Entries{
		Fields: []string{"ABC", "BCD", "YYY"},
		Update: []string{"ABC"},
		Values: [][]any{{"abc", 123, 233}},
	}
	sqlCmd, rows := generateDelete(true, "TABLENAME", 0, ui)
	assert.Equal(t, "DELETE FROM TABLENAME WHERE abc IN ($1) AND bcd IN ($2) AND yyy IN ($3)", sqlCmd)
	assert.Equal(t, []interface{}{"abc", 123, 233}, rows)

	sqlCmd, rows = generateDelete(false, "TABLENAME", 0, ui)
	assert.Equal(t, "DELETE FROM TABLENAME WHERE abc IN (?) AND bcd IN (?) AND yyy IN (?)", sqlCmd)
	assert.Equal(t, []interface{}{"abc", 123, 233}, rows)

	ui.Fields = []string{"ABC", "BCD", "%YYY"}
	ui.Values = [][]any{{"abc", 123, "XXX%"}}
	sqlCmd, rows = generateDelete(false, "TABLENAME", 0, ui)
	assert.Equal(t, "DELETE FROM TABLENAME WHERE abc IN (?) AND bcd IN (?) AND (YYY LIKE 'XXX%')", sqlCmd)
	assert.Equal(t, []interface{}{"abc", 123}, rows)
}
