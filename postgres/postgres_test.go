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

package postgres

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/tknie/log"
)

var logRus = logrus.StandardLogger()
var once = new(sync.Once)

func initLog() {
	once.Do(startLog)
}

func startLog() {
	fmt.Println("Init logging")
	fileName := "db.trace.log"
	level := os.Getenv("ENABLE_DB_DEBUG")
	logLevel := logrus.InfoLevel
	switch level {
	case "debug":
		log.SetDebugLevel(true)
		logLevel = logrus.DebugLevel
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

func PostgresTable(t *testing.T) string {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPort := os.Getenv("POSTGRES_PORT")
	postgresPassword := os.Getenv("POSTGRES_PWD")
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return ""
	}
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", postgresPassword, postgresHost, port, "bitgarten")
	return url
}

func TestPostgresInit(t *testing.T) {
	initLog()
	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	m, err := pg.Maps()
	sort.Strings(m)
	assert.NoError(t, err)
	assert.Equal(t, []string{"albumpictures", "albums", "picturelocations",
		"pictures", "picturetags", "teststructtabledata", "testtabledata"}, m)
}

func TestPostgresTableColumns(t *testing.T) {
	initLog()
	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}

	m, err := pg.GetTableColumn("Albums")
	sort.Strings(m)
	assert.NoError(t, err)
	assert.Equal(t, []string{"albumkey", "albumtype",
		"created", "description", "directory", "id",
		"option", "published", "thumbnail", "title",
		"updated_at"}, m)

}
