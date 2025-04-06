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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
	"golang.org/x/exp/slices"
)

type DBsql interface {
	ID() common.RegDbID
	Open() (any, error)
	StartTransaction() (*sql.Tx, context.Context, error)
	EndTransaction(bool) error
	Close()
	Reference() (string, string)
	IndexNeeded() bool
	ByteArrayAvailable() bool
	IsTransaction() bool
}

func CreateTable(dbsql DBsql, name string, col any) error {
	log.Log.Debugf("%s: Create SQL table", dbsql.ID())
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	createCmd := `CREATE TABLE ` + name + ` (`
	switch columns := col.(type) {
	case []*common.Column:
		createCmd += CreateTableByColumns(dbsql.ByteArrayAvailable(), columns)
	default:
		c, err := CreateTableByStruct(dbsql.ByteArrayAvailable(), col)
		if err != nil {
			log.Log.Errorf("Error parsing structure: %v", err)
			return err
		}
		createCmd += c
	}
	createCmd += ")"
	log.Log.Debugf("Create cmd %s", createCmd)
	_, err = db.Query(createCmd)
	if err != nil {
		log.Log.Errorf("Error returned by SQL: %v", err)
		return err
	}
	log.Log.Debugf("Table created, waiting ....")
	//time.Sleep(60 * time.Second)
	log.Log.Debugf("Table created")
	return nil
}

// AdaptTable adapt table to new struct
func AdaptTable(dbsql DBsql, name string, col any) error {

	log.Log.Debugf("%s: Adapt SQL table", dbsql.ID())
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()

	columnCurrent, err := dbsql.ID().GetTableColumn(name)
	if err != nil {
		return err
	}
	fmt.Println(columnCurrent)
	log.Log.Debugf("Got columns: %v", columnCurrent)
	columStruct, err := SqlDataType(true, col, columnCurrent)
	if err != nil {
		return err
	}
	fmt.Println(columStruct)
	for _, f := range strings.Split(columStruct, ",") {

		adaptCmd := `ALTER TABLE ` + name + ` ADD ` + f
		log.Log.Debugf("Create cmd %s", adaptCmd)
		_, err = db.Query(adaptCmd)
		if err != nil {
			log.Log.Errorf("Error returned by SQL: %v", err)
			return err
		}
	}
	log.Log.Debugf("Table adapted")
	return nil
}

func DeleteTable(dbsql DBsql, name string) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Query("DROP TABLE " + name)
	if err != nil {
		log.Log.Debugf("Drop table error: %v", err)
		return err
	}
	log.Log.Debugf("Drop table " + name)
	return nil
}

func CreateTableByColumns(baAvailable bool, columns []*common.Column) string {
	var buffer bytes.Buffer
	for i, c := range columns {
		if i > 0 {
			buffer.WriteString(", ")
		}
		buffer.WriteString(c.Name + " ")
		switch c.DataType {
		case common.Alpha, common.Bit:
			buffer.WriteString(c.DataType.SqlType(c.Length))
		case common.Decimal:
			buffer.WriteString(c.DataType.SqlType(c.Length, c.Digits))
		case common.Bytes:
			buffer.WriteString(c.DataType.SqlType(baAvailable,
				c.Length))
		default:
			buffer.WriteString(c.DataType.SqlType())
		}
	}
	return buffer.String()
}

func CreateTableByStruct(baAvailable bool, columns any) (string, error) {
	log.Log.Debugf("Create table by structs")
	return SqlDataType(baAvailable, columns, nil)
}

func Batch(dbsql DBsql, batch string) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	// Query batch SQL
	rows, err := db.Query(batch)
	if err != nil {
		return err
	}
	for rows.Next() {
		if rows.Err() != nil {
			fmt.Println("Batch SQL error:", rows.Err())
		}
	}
	return nil
}

// BatchSelect batch SQL query in table with values returned
func BatchSelect(dbsql DBsql, batch string) ([][]interface{}, error) {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	// Query batch SQL
	rows, err := db.Query(batch)
	if err != nil {
		return nil, err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([][]interface{}, 0)
	for rows.Next() {
		if rows.Err() != nil {
			fmt.Println("Batch SQL error:", rows.Err())
			return nil, rows.Err()
		}
		data := common.CreateTypeData(ct)
		err := rows.Scan(data...)
		if err != nil {
			return nil, err
		}
		data = common.Unpointer(data)
		result = append(result, data)
	}
	return result, nil
}

// BatchSelectFct batch SQL query in table with fct called
func BatchSelectFct(dbsql DBsql, batch *common.Query, fct common.ResultFunction) error {
	layer, url := dbsql.Reference()
	log.Log.Debugf("Connect url: %s", url)
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	// Query batch SQL
	rows, err := db.Query(batch.Search)
	if err != nil {
		return err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	count := uint64(0)
	result := &common.Result{}
	for rows.Next() {
		if rows.Err() != nil {
			fmt.Println("Batch SQL error:", rows.Err())
			return rows.Err()
		}
		if result.Header == nil {
			result.Header = common.CreateHeader(ct)
		}
		data := common.CreateTypeData(ct)
		err := rows.Scan(data...)
		if err != nil {
			return err
		}
		result.Data = common.Unpointer(data)
		count++
		fct(nil, result)
	}
	return nil
}

func SqlDataType(baAvailable bool, columns any, ignoreList []string) (string, error) {
	x := reflect.TypeOf(columns)
	if x.Kind() == reflect.Pointer {
		x = x.Elem()
	}
	log.Log.Debugf("Go through data type %s ==>", x.Name())
	defer log.Log.Debugf("Go through data type %s <==", x.Name())

	switch x.Kind() {
	case reflect.Struct:
		var buffer bytes.Buffer
		first := false
		for i := 0; i < x.NumField(); i++ {
			f := x.Field(i)
			s, err := sqlDataTypeStructField(baAvailable, f, ignoreList)
			if err != nil {
				return "", err
			}
			if s != "" {
				if i > 0 && first {
					buffer.WriteString(", ")
				}
				buffer.WriteString(s)
				first = true
			}

		}
		log.Log.Debugf("Got for type %s: %s", x.Name(), buffer.String())
		return buffer.String(), nil
	}
	log.Log.Debugf("Type error, no struct: %T", columns)
	return "", errorrepo.NewError("DB000005", "", fmt.Sprintf("%T", columns))
}

func sqlDataTypeStructField(baAvailable bool, field reflect.StructField,
	ignoreList []string) (string, error) {
	x := field.Type
	if x.Kind() == reflect.Pointer {
		x = x.Elem()
	}
	if log.IsDebugLevel() {
		log.Log.Debugf("Check kind %s/%s %s", x.Kind(), x.Name(), field.Name)
		log.Log.Debugf("Check %v %s %v", ignoreList, field.Name, slices.Contains(ignoreList, strings.ToLower(field.Name)))
	}
	// Check ignore list
	if ignoreList != nil && slices.Contains(ignoreList, strings.ToLower(field.Name)) {
		return "", nil
	}
	switch x.Kind() {
	case reflect.Struct:
		log.Log.Debugf("Check struct")
		sfi := evaluateName(field, x)
		if sfi.skip {
			return "", nil
		}
		if x.Name() == "Time" {
			return sfi.name + " TIMESTAMP " + sfi.additional, nil
		}
		if tagValue, ok := field.Tag.Lookup(common.TagName); ok {
			log.Log.Debugf("Found tag %s for %s", tagValue, field.Name)
			tagName, tagInfo := common.TagInfoParse(tagValue)
			fieldName := field.Name
			if fieldName == "" || !unicode.IsUpper([]rune(fieldName)[0]) {
				log.Log.Debugf("Field skip because lowercase name %c of %s", []rune(fieldName)[0], fieldName)
				return "", nil
			}
			if tagName != "" {
				fieldName = tagName
			}
			switch tagInfo {
			case common.SubTag:
				log.Log.Debugf("Found sub type tag")
				return fieldName + " " + common.Bytes.SqlType(baAvailable, 255), nil
			case common.YAMLTag, common.XMLTag, common.JSONTag:
				log.Log.Debugf("Found conversion tag %s", tagInfo)
				return fieldName + " " + common.Alpha.SqlType(255), nil
			}
		}
		var buffer bytes.Buffer
		for i := 0; i < x.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := x.Field(i)
			s, err := sqlDataTypeStructFieldDataType(baAvailable, f)
			if err != nil {
				return "", err
			}
			buffer.WriteString(s)
		}
		return buffer.String(), nil
	default:
		return sqlDataTypeStructFieldDataType(baAvailable, field)
	}
	// return "", NewError(5, field.Name, x.Kind())
}

func sqlDataTypeStructFieldDataType(baAvailable bool, sf reflect.StructField) (string, error) {
	t := sf.Type
	fieldName := t.Name()
	if fieldName == "" || !unicode.IsUpper([]rune(fieldName)[0]) {
		log.Log.Debugf("Field skip because lowercase name %c of %s", []rune(fieldName)[0], fieldName)
		return "", nil
	}
	sfi := evaluateName(sf, t)
	if sfi.skip {
		return "", nil
	}
	if sfi.info != "" {
		return sfi.info, nil
	}
	log.Log.Debugf("dbsql name %s and kind %s (%s) (sfi kind=%s)",
		sfi.name, t.Kind(), t.Name(), sfi.kind)
	if t.PkgPath() == "time" && t.Name() == "Time" {
		return sfi.name + " TIMESTAMP", nil
	}
	switch t.Kind() {
	case reflect.String:
		switch sfi.kind {
		case "BLOB", "ABYTE":
			if baAvailable {
				return sfi.name + " " + common.Bytes.SqlType(baAvailable, sfi.length), nil
			}
			return sfi.name + " " + common.BLOB.SqlType(sfi.length), nil
		default:
			if sfi.length == 0 {
				sfi.length = 255
			}
			return sfi.name + " " + common.Alpha.SqlType(sfi.length) + sfi.additional, nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		return sfi.name + " " + common.Integer.SqlType() + sfi.additional, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		return sfi.name + " " + common.Integer.SqlType() + sfi.additional, nil
	case reflect.Float32, reflect.Float64:
		if sfi.length == 0 {
			sfi.length = 10
		}
		return sfi.name + " " + common.Decimal.SqlType(sfi.length, 5) + sfi.additional, nil
	case reflect.Bool:
		// if sfi.length == 0 {
		// 	sfi.length = 1
		// }
		// sfi.name + " " + common.Bit.SqlType(sfi.length) + sfi.additional, nil
		return sfi.name + " BOOL" + sfi.additional, nil
	case reflect.Complex64, reflect.Complex128:
		return "", errorrepo.NewError("DB000007")
	case reflect.Struct:
		var buffer bytes.Buffer
		ty := t
		for i := 0; i < ty.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := ty.Field(i)
			log.Log.Debugf("Struct Field: " + f.Name)
			s, err := sqlDataTypeStructFieldDataType(baAvailable, f)
			if err != nil {
				return "", err
			}

			buffer.WriteString(s)
		}
		buffer.WriteString(sfi.additional)
		return buffer.String(), nil
	case reflect.Array:
		log.Log.Debugf("Arrays %d", t.Len())
		if t.Elem().Kind() == reflect.Uint8 {
			return sfi.name + " " + common.Character.SqlType(t.Len()) + sfi.additional, nil
		}
		return "", errorrepo.NewError("DB000008", sf.Name)
	case reflect.Slice:
		return evaluateSlice(baAvailable, sf, t)
	default:
		//		return SqlDataType(t)
		// + " CONSTRAINT " + t.Name +
		// 	" CHECK (" + t.Name + " > 0)"
	}
	return "", errorrepo.NewError("DB000006", sf.Name, t.Kind())
}

type structFieldInfo struct {
	name       string
	additional string
	info       string
	kind       string
	length     int
	skip       bool
}

// evaluateName evaluate name of type given (extract tags and info)
func evaluateName(sf reflect.StructField, tsf reflect.Type) *structFieldInfo {
	sfi := &structFieldInfo{name: sf.Name, skip: false}
	log.Log.Debugf("Found name " + sfi.name)
	if tagName, ok := sf.Tag.Lookup(common.TagName); ok {
		tagField := strings.Split(tagName, ":")
		if tagField[0] != "" {
			sfi.name = tagField[0]
		}
		if len(tagField) > 1 {
			if tagField[1] == "ignore" {
				sfi.skip = true
				return sfi
			}
			sfi.additional = " " + tagField[1]
			sfi.kind = tagField[1]
		}
		log.Log.Debugf("Overwrite to name " + sfi.name)
		if len(tagField) > 2 && tagField[2] != "" {
			if tagField[2] == "SERIAL" {
				sfi.info = sfi.name + " SERIAL UNIQUE"
				return sfi
			}
			x, err := strconv.Atoi(tagField[2])
			if err == nil {
				sfi.length = x
			}
		}
	}
	return sfi
}

func evaluateSlice(baAvailable bool, sf reflect.StructField, t reflect.Type) (string, error) {
	tt := t.Elem()
	if tt.Kind() == reflect.Pointer {
		tt = t.Elem()
	}
	switch tt.Kind() {
	case reflect.Uint8, reflect.Int8:
		sfi := evaluateName(sf, t)
		if sfi.info != "" {
			return sfi.info, nil
		}
		return sfi.name + " " + common.Bytes.SqlType(baAvailable, 8) + sfi.additional, nil
	default:
		log.Log.Debugf("Slice not supported %s (%s)", tt.Kind(), t.Kind())
	}
	return "", errorrepo.NewError("DB000009", t.Elem().Kind(), sf.Name)
}
