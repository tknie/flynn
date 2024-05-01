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

package common

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/log"
)

type SubField struct {
	SubName string
	Number  int
}

type testRecord struct {
	ID        int
	Name      string
	FirstName string
	LastName  string
	Address   string `flynn:"Street"`
	Salary    uint64 `flynn:"Salary"`
	Bonus     int64
	Sub       *SubField `flynn:":sub"`
}

func (s *SubField) Data() []byte {
	if s == nil {
		return []byte("")
	}
	return []byte(fmt.Sprintf("%s:%03d", s.SubName, s.Number))
}

func (s *SubField) ParseData(sub []byte) error {
	if s == nil {
		return fmt.Errorf("test data parse error")
	}
	sp := strings.Split(string(sub), ":")
	s.SubName = sp[0]
	n, err := strconv.Atoi(sp[1])
	if err != nil {
		return err
	}
	s.Number = n
	return nil
}

func TestDynamicQueryFields(t *testing.T) {
	InitLog(t)
	v := &testRecord{ID: 123, Name: "FHUDFD", FirstName: "YYYYY"}
	assert.Nil(t, v.Sub)
	ti := CreateInterface(v, []string{"Name", "FirstName", "Bonus"})
	qf := ti.CreateQueryFields()
	assert.Equal(t, "Name,FirstName,Bonus", qf)
	assert.Equal(t, []string{"Name", "FirstName", "Bonus"}, ti.RowFields)
	assert.Equal(t, []any(nil), ti.ValueRefTo)

	ti = CreateInterface(v, []string{"Name", "FirstName"})
	qf = ti.CreateQueryFields()
	assert.Equal(t, "Name,FirstName", qf)
	assert.Equal(t, []string{"Name", "FirstName"}, ti.RowFields)
	assert.Equal(t, []any(nil), ti.ValueRefTo)

}

func TestDynamicQueryAll(t *testing.T) {
	InitLog(t)

	v := &testRecord{ID: 123, Name: "FHUDFD", FirstName: "YYYYY"}
	ti := CreateInterface(v, []string{"*"})
	assert.Equal(t, []string{"ID", "Name", "FirstName", "LastName", "Street", "Salary", "Bonus", "Sub"}, ti.RowFields)
	log.Log.Debugf("Test: Create query values")
	vd, err := ti.CreateQueryValues()
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, v, ti.DataType)
	assert.Equal(t, 0, *(vd.Values[0].(*int)))
	assert.Equal(t, "", *(vd.Values[2].(*string)))
	assert.Equal(t, "", *(vd.Values[1].(*string)))
	assert.Equal(t, &sql.NullInt32{}, (vd.ScanValues[0].(*sql.NullInt32)))
	assert.Equal(t, &sql.NullString{}, (vd.ScanValues[2].(*sql.NullString)))
	assert.Equal(t, &sql.NullString{}, (vd.ScanValues[1].(*sql.NullString)))
	assert.Equal(t, []string{"ID", "Name", "FirstName", "LastName", "Street",
		"Salary", "Bonus", "Sub"}, ti.RowFields)
	assert.Len(t, vd.Values, 8)
	assert.Len(t, vd.ScanValues, 8)

	ns := vd.ScanValues[1].(*sql.NullString)
	ns.String = "NNNNNNN"
	ns.Valid = true
	ns = vd.ScanValues[2].(*sql.NullString)
	ns.String = "FFFFFFF"
	ns.Valid = true
	ns = vd.ScanValues[5].(*sql.NullString)
	ns.String = "2342323"
	ns.Valid = true
	ni := vd.ScanValues[6].(*sql.NullInt64)
	ni.Int64 = 2333
	ni.Valid = true
	ns = vd.ScanValues[7].(*sql.NullString)
	ns.String = "abc:2342323"
	ns.Valid = true
	newValue := vd.Copy.(*testRecord)
	assert.Equal(t, &testRecord{ID: 0, Name: "",
		FirstName: "", LastName: "", Address: "",
		Salary: 0, Bonus: 0,
		Sub: (*SubField)(newValue.Sub)}, vd.Copy)
	log.Log.Debugf("Test: Shift values")
	err = vd.ShiftValues()
	assert.NoError(t, err)
	assert.Equal(t, &testRecord{ID: 0, Name: "NNNNNNN",
		FirstName: "FFFFFFF", LastName: "", Address: "",
		Salary: 2342323, Bonus: 2333,
		Sub: (*SubField)(newValue.Sub)}, vd.Copy)
	assert.NotEqual(t, (*SubField)(nil), newValue.Sub)

	createValue, err := ti.CreateValues(v)
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{123, "FHUDFD", "YYYYY", "", "",
		uint64(0), int64(0), []uint8{}}, createValue)

}
