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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
)

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
	assert.Equal(t, []string{"AlbumPictures", "Albums", "PictureLocations", "PictureTag",
		"PictureTags", "Pictures", "Tags",
		"TestStructTableData", "TestTableData"}, m)
}

func TestMysqlCall(t *testing.T) {
	url, err := mysqlTarget(t)
	assert.NoError(t, err)
	mSql, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, mSql) {
		return
	}
	err = mSql.BatchSelectFct("select * from Albums", func(index uint64, header []*common.Column, result []interface{}) error {
		fmt.Println("H", len(header))
		return nil
	})
	assert.NoError(t, err)
}
