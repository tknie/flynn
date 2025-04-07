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

package dbsql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
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

func TestDataTypeStructBlogs(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	x := struct {
		St  string
		Int int
	}{"aaa", 1}

	s, err := SqlDataType(tSQL.ByteArrayAvailable(), &x, nil)
	assert.NoError(t, err)
	assert.Equal(t, "St VARCHAR(255), Int INTEGER", s)
	y := struct {
		XSt   string
		ZBlob string `flynn:"SBLOB:BLOB:2048"`
		XInt  int
		Xstr  struct {
			Xii uint64
		}
	}{"aaa", "fjrpsgj", 1, struct{ Xii uint64 }{2}}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), &y, nil)
	assert.NoError(t, err)
	assert.Equal(t, "XSt VARCHAR(255), SBLOB BYTEA, XInt INTEGER, Xii NUMERIC(20,0)", s)
	global := &GlobStruct{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), global, nil)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), ABC VARCHAR(255), Nr NUMERIC(20,0), Value INTEGER, Doub DECIMAL(10,5), DoIt BOOL", s)
	global2 := &GlobStruct2{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), global2, nil)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), ABC VARCHAR(255), Nr NUMERIC(20,0), Value INTEGER, Doub DECIMAL(10,5), DoIt BOOL", s)
	global3 := &GlobStruct3{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), global3, nil)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), XYZ VARCHAR(255), UUU VARCHAR(255), ID NUMERIC(20,0) IDENTITY(1, 1), Value INTEGER, Doub DECIMAL(10,5), DoIt BOOL", s)
	slice := &SliceStruct{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), slice, nil)
	assert.Error(t, err)
	assert.Equal(t, "DB000009: Slice types string are not supported used by field Test", err.Error())
	assert.Equal(t, "", s)
	arr := &ArrayStruct{}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), arr, nil)
	assert.Error(t, err)
	assert.Equal(t, "DB000008: Array types are not supported used by field Test", err.Error())
	assert.Equal(t, "", s)

	z := struct {
		ZSt   string `flynn:"KKK::1024"`
		ZInt  string `flynn:"ABC::200"`
		ZBlob []byte
		Zstr0 *SubStruct `flynn:"NNN"`
		Zstr1 *SubStruct `flynn:"YYY:YAML"`
		Zstr2 *SubStruct `flynn:"XXX:XML"`
		Zstr3 *SubStruct `flynn:"JJJ:JSON"`
	}{"aaa", "djfgidjfgi", []byte{1, 9}, nil, nil, nil, nil}
	s, err = SqlDataType(tSQL.ByteArrayAvailable(), &z, nil)
	assert.NoError(t, err)
	assert.Equal(t, "KKK VARCHAR(1024) , ABC VARCHAR(200) , ZBlob BYTEA, ABC VARCHAR(255), Nr NUMERIC(20,0), Value INTEGER, Doub DECIMAL(10,5), DoIt BOOL, YYY VARCHAR(255), XXX VARCHAR(255), JJJ VARCHAR(255)", s)

	ti := common.CreateInterface(&z, []string{"*"})
	assert.Equal(t, []string{"KKK", "ABC", "ZBlob", "ABC", "Nr", "Value", "Doub", "DoIt", "YYY", "XXX", "JJJ"}, ti.RowFields)
	ti = common.CreateInterface(&z, []string{"*"})
	assert.Equal(t, []string{"KKK", "ABC", "ZBlob", "ABC", "Nr", "Value", "Doub", "DoIt", "YYY", "XXX", "JJJ"}, ti.RowFields)

	ti = common.CreateInterface(&GlobStruct{}, []string{"*"})
	v, err := ti.CreateValues(&GlobStruct{Test: "ABCBCC"})
	assert.Equal(t, []interface{}{"ABCBCC", "", uint64(0), int64(0), float64(0), false}, v)
	assert.NoError(t, err)
}

func TestDataTypeStructTag(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	zz := struct {
		St  string
		LOB string `flynn:"AA::6"`
		Int int
		Ba  []int8
		Ca  [4]byte
	}{"bbb", "dfsfspdgjsdpgjspdg",
		1, []int8{1, 2, 3, 4, 5}, [4]byte{'a', 'b', 'c', 'd'}}

	s, err := SqlDataType(tSQL.ByteArrayAvailable(), &zz, nil)
	assert.NoError(t, err)
	assert.Equal(t, "St VARCHAR(255), AA VARCHAR(6) , Int INTEGER, Ba BYTEA, Ca CHAR(4)", s)

}
