/*
* Copyright 2023-2024 Thorsten A. Knieling
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
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

var logRus = logrus.StandardLogger()
var once = new(sync.Once)

func InitLog(t *testing.T) {
	once.Do(startLog)
	log.Log.Debugf("TEST: %s", t.Name())
}

func startLog() {
	fmt.Println("Init logging")
	fileName := "db.trace.log"
	level := os.Getenv("ENABLE_DB_DEBUG")
	logLevel := logrus.WarnLevel
	switch level {
	case "debug", "1":
		log.SetDebugLevel(true)
		logLevel = logrus.DebugLevel
	case "info", "2":
		log.SetDebugLevel(false)
		logLevel = logrus.InfoLevel
	default:
	}
	logRus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05",
	})
	logRus.SetLevel(logLevel)
	p := os.Getenv("LOGPATH")
	if p == "" {
		p = os.TempDir()
	}
	f, err := os.OpenFile(p+"/"+fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening log:", err)
		return
	}
	logRus.SetOutput(f)
	logRus.Infof("Init logrus")
	log.Log = logRus
	fmt.Println("Logging running")
}

func TestSQLUpdate(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	ui := &common.Entries{
		Fields: []string{"ABC", "BCD", "YYY"},
		Update: []string{"ABC"},
		Values: [][]any{{"abc", 123, 233}},
	}
	sqlCmd, rows := GenerateUpdate(true, "ABC", ui)
	assert.Equal(t, "UPDATE ABC SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{0}, rows)
	wh := CreateWhere(0, ui, rows)
	assert.Equal(t, "\"abc\"='abc'", wh)

	ui.Update[0] = "BCD"
	sqlCmd, rows = GenerateUpdate(true, "DFX", ui)
	assert.Equal(t, "UPDATE DFX SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{1}, rows)
	wh = CreateWhere(0, ui, rows)
	assert.Equal(t, "\"bcd\"=123", wh)

	ui.Update[0] = "BCXD=hugo"
	sqlCmd, rows = GenerateUpdate(true, "Table1", ui)
	assert.Equal(t, "UPDATE Table1 SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{}, rows)
	wh = CreateWhere(0, ui, rows)
	assert.Equal(t, "BCXD=hugo", wh)

	ui.Update[0] = "DDD=emil"
	ui.Update = append(ui.Update, "YYY")
	sqlCmd, rows = GenerateUpdate(true, "Table2", ui)
	assert.Equal(t, "UPDATE Table2 SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{2}, rows)
	wh = CreateWhere(0, ui, rows)
	assert.Equal(t, "DDD=emil AND \"yyy\"=233", wh)

	ui.Update[0] = "YYY=emil"
	ui.Update = append(ui.Update, "ABC")
	ui.Update = append(ui.Update, "WWW=abc")
	sqlCmd, rows = GenerateUpdate(true, "Table3", ui)
	assert.Equal(t, "UPDATE Table3 SET \"abc\"=$1,\"bcd\"=$2,\"yyy\"=$3 WHERE ", sqlCmd)
	assert.Equal(t, []int{0, 2}, rows)
	wh = CreateWhere(0, ui, rows)
	assert.Equal(t, "YYY=emil AND WWW=abc AND \"abc\"='abc' AND \"yyy\"=233", wh)

	ui.Fields = []string{"AA", "BB", "CC", "DD", "TT"}
	ui.Values = [][]any{{"XXX", "daslkds", 123, 222, 222, time.Now()}, {"XXX2", "aaa2", 51, 522, 5222, time.Now()}}
	ui.Update = []string{"YY=otto", "AA", "CC", "TT"}
	sqlCmd, rows = GenerateUpdate(true, "Table4", ui)
	assert.Equal(t, "UPDATE Table4 SET \"aa\"=$1,\"bb\"=$2,\"cc\"=$3,\"dd\"=$4,\"tt\"=$5 WHERE ", sqlCmd)
	assert.Equal(t, []int{0, 2, 4}, rows)
	wh = CreateWhere(0, ui, rows)
	assert.Equal(t, "YY=otto AND \"aa\"='XXX' AND \"cc\"=123 AND \"tt\"=222", wh)
	wh = CreateWhere(1, ui, rows)
	assert.Equal(t, "YY=otto AND \"aa\"='XXX2' AND \"cc\"=51 AND \"tt\"=5222", wh)
}

func TestSQLDelete(t *testing.T) {
	ui := &common.Entries{
		Fields: []string{"ABC", "BCD", "YYY"},
		Update: []string{"ABC"},
		Values: [][]any{{"abc", 123, 233}},
	}
	sqlCmd, rows := GenerateDelete(true, "TABLENAME", 0, ui)
	assert.Equal(t, "DELETE FROM TABLENAME WHERE abc IN ($1) AND bcd IN ($2) AND yyy IN ($3)", sqlCmd)
	assert.Equal(t, []interface{}{"abc", 123, 233}, rows)

	sqlCmd, rows = GenerateDelete(false, "TABLENAME", 0, ui)
	assert.Equal(t, "DELETE FROM TABLENAME WHERE abc IN (?) AND bcd IN (?) AND yyy IN (?)", sqlCmd)
	assert.Equal(t, []interface{}{"abc", 123, 233}, rows)

	ui.Fields = []string{"ABC", "BCD", "%YYY"}
	ui.Values = [][]any{{"abc", 123, "XXX%"}}
	sqlCmd, rows = GenerateDelete(false, "TABLENAME", 0, ui)
	assert.Equal(t, "DELETE FROM TABLENAME WHERE abc IN (?) AND bcd IN (?) AND (YYY LIKE 'XXX%')", sqlCmd)
	assert.Equal(t, []interface{}{"abc", 123}, rows)
}
