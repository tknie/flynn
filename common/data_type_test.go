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

package common

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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

func TestDataType(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	assert.Equal(t, "INTEGER", Number.SqlType())
	assert.Equal(t, "TEXT", Text.SqlType())
	assert.Equal(t, "VARCHAR(19)", Alpha.SqlType(19))
	assert.Equal(t, "INTEGER", Integer.SqlType())
	assert.Equal(t, "DATE", Date.SqlType())
	assert.Equal(t, "BINARY(10)", Bytes.SqlType(false, 10))
	assert.Equal(t, "BYTEA", Bytes.SqlType(true, 10))
}
