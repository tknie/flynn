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

package mysql

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"

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

func mysqlTarget(t *testing.T) (string, error) {
	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlPort := os.Getenv("MYSQL_PORT")
	mysqlPassword := os.Getenv("MYSQL_PWD")
	if !assert.NotEmpty(t, mysqlHost) {
		return "", fmt.Errorf("MySQL Host not set")
	}
	assert.NotEmpty(t, mysqlPort)
	port, err := strconv.Atoi(mysqlPort)
	if !assert.NoError(t, err) {
		return "", fmt.Errorf("MYSQL Port not set")
	}
	mysqlUrl := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", "admin", mysqlPassword, mysqlHost, port, "Bitgarten")

	return mysqlUrl, nil
}

func TestMysqlInit(t *testing.T) {
	url, err := mysqlTarget(t)
	assert.NoError(t, err)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	m, err := pg.Maps()
	sort.Strings(m)
	assert.NoError(t, err)
	assert.Equal(t, []string{"AlbumPictures", "Albums", "PictureLocations",
		"PictureMetadata", "PictureTag",
		"PictureTags", "Pictures", "Tags",
		"TestStructTableData", "TestTableData"}, m)
}

func TestMysqlCall(t *testing.T) {
	InitLog(t)
	url, err := mysqlTarget(t)
	assert.NoError(t, err)
	mSql, err := New(1, url+"?parseTime=true")
	assert.NoError(t, err)
	if !assert.NotNil(t, mSql) {
		return
	}
	err = mSql.BatchSelectFct("select * from Albums", func(index uint64, header []*common.Column, result []interface{}) error {
		assert.Equal(t, 10, len(header))
		assert.Equal(t, 10, len(result))
		return nil
	})
	assert.NoError(t, err)
}
