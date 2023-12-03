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

package dbsql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/log"
)

type SubStruct struct {
	ABC   string
	Nr    uint64
	Value int64
	Doub  float64
	//	AA    complex128
	DoIt bool
}

type SubStruct3 struct {
	ABC   string `flynn:"XYZ"`
	DEF   string `flynn:"UUU"`
	Nr    uint64 `flynn:"ID:IDENTITY(1, 1)"`
	Value int64
	Doub  float64
	//	AA    complex128
	DoIt bool
}

type GlobStruct struct {
	Test string
	Sub  *SubStruct
}

type GlobStruct2 struct {
	Test string
	Sub  SubStruct
}

type GlobStruct3 struct {
	Test string
	Sub  SubStruct3
}

type ArrayStruct struct {
	Test [3]string
	Sub  *SubStruct
}

type SliceStruct struct {
	Test []string
	Sub  *SubStruct
}

type testSQL struct {
}

var tSQL = &testSQL{}

func (t *testSQL) Open() (any, error) {
	return nil, nil
}

func (t *testSQL) StartTransaction() (*sql.Tx, context.Context, error) {
	return nil, nil, nil
}

func (t *testSQL) EndTransaction(bool) error {
	return nil
}

func (t *testSQL) Close() {
	log.Log.Debugf("Close testSQL")

}

func (t *testSQL) IsTransaction() bool {
	return true
}
func (t *testSQL) ByteArrayAvailable() bool {
	return true
}
func (t *testSQL) Reference() (string, string) {
	return "", ""
}
func (t *testSQL) IndexNeeded() bool {
	return true
}

func TestDataTypeStruct(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	x := struct {
		St  string
		Int int
	}{"aaa", 1}

	s, err := SqlDataType(tSQL.ByteArrayAvailable(), &x)
	assert.NoError(t, err)
	assert.Equal(t, "St VARCHAR(255), Int INTEGER", s)
	y := struct {
		XSt  string
		XInt int
		Xstr struct {
			Xii uint64
		}
	}{"aaa", 1, struct{ Xii uint64 }{2}}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), &y)
	assert.NoError(t, err)
	assert.Equal(t, "XSt VARCHAR(255), XInt INTEGER, Xii INTEGER", s)
	global := &GlobStruct{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), global)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), ABC VARCHAR(255), Nr INTEGER, Value INTEGER, Doub DECIMAL(10,5), DoIt BIT(1)", s)
	global2 := &GlobStruct2{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), global2)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), ABC VARCHAR(255), Nr INTEGER, Value INTEGER, Doub DECIMAL(10,5), DoIt BIT(1)", s)
	global3 := &GlobStruct3{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), global3)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), XYZ VARCHAR(255), UUU VARCHAR(255), ID INTEGER IDENTITY(1, 1), Value INTEGER, Doub DECIMAL(10,5), DoIt BIT(1)", s)
	slice := &SliceStruct{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), slice)
	assert.Error(t, err)
	assert.Equal(t, "DB000009: Slice types string are not supported used by field Test", err.Error())
	assert.Equal(t, "", s)
	arr := &ArrayStruct{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), arr)
	assert.Error(t, err)
	assert.Equal(t, "DB000008: Array types are not supported used by field Test", err.Error())
	assert.Equal(t, "", s)
}
