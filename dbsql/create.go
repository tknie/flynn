/*
* Copyright 2022 Thorsten A. Knieling
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
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/tknie/db/common"
)

type DBsql interface {
	Open() (any, error)
	Close()
	Reference() (string, string)
	IndexNeeded() bool
	ByteArrayAvailable() bool
	IsTransaction() bool
}

func CreateTable(dbsql DBsql, name string, col any) error {
	//	columns []*def.Column
	common.Log.Debugf("Create SQL table")
	dbOpen, err := dbsql.Open()
	if err != nil {
		return err
	}
	db := dbOpen.(*sql.DB)
	defer dbsql.Close()
	createCmd := `CREATE TABLE ` + name + ` (`
	switch columns := col.(type) {
	case []*common.Column:
		createCmd += createTableByColumns(dbsql, columns)
	default:
		c, err := createTableByStruct(dbsql, col)
		if err != nil {
			common.Log.Errorf("Error parsing structure: %v", err)
			return err
		}
		createCmd += c
	}
	createCmd += ")"
	common.Log.Debugf("Create cmd %s", createCmd)
	_, err = db.Query(createCmd)
	if err != nil {
		common.Log.Errorf("Error returned by SQL: %v", err)
		return err
	}
	common.Log.Debugf("Table created")
	return nil
}

func DeleteTable(dbsql DBsql, name string) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()

	common.Log.Debugf("Drop table " + name)

	_, err = db.Query("DROP TABLE " + name)
	if err != nil {
		return err
	}
	return nil
}

func createTableByColumns(dbsql DBsql, columns []*common.Column) string {
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
			buffer.WriteString(c.DataType.SqlType(dbsql.ByteArrayAvailable(),
				c.Length))
		default:
			buffer.WriteString(c.DataType.SqlType())
		}
	}
	return buffer.String()
}

func createTableByStruct(dbsql DBsql, columns any) (string, error) {
	common.Log.Debugf("Create table by structs")
	return SqlDataType(dbsql, columns)
}

func BatchSQL(dbsql DBsql, batch string) error {
	layer, url := dbsql.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	// TODO
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

func SqlDataType(dbsql DBsql, columns any) (string, error) {
	x := reflect.TypeOf(columns)
	if x.Kind() == reflect.Pointer {
		x = x.Elem()
	}
	common.Log.Debugf("Go through data type %s", x.Name())
	switch x.Kind() {
	case reflect.Struct:
		var buffer bytes.Buffer
		for i := 0; i < x.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := x.Field(i)
			s, err := sqlDataTypeStructField(dbsql, f)
			if err != nil {
				return "", err
			}
			buffer.WriteString(s)
		}
		common.Log.Debugf("Got for type %s: %s", x.Name(), buffer.String())
		return buffer.String(), nil
	}
	common.Log.Debugf("Type error, no struct: %T", columns)
	return "", common.NewError(5, "", fmt.Sprintf("%T", columns))
}

func sqlDataTypeStructField(dbsql DBsql, field reflect.StructField) (string, error) {
	x := field.Type
	if x.Kind() == reflect.Pointer {
		x = x.Elem()
	}
	common.Log.Debugf("Check kind %s/%s %s", x.Kind(), x.Name(), field.Name)
	switch x.Kind() {
	case reflect.Struct:
		name, additional, _ := evaluateName(field, x)
		if x.Name() == "Time" {
			return name + " TIMESTAMP " + additional, nil
		}
		var buffer bytes.Buffer
		for i := 0; i < x.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := x.Field(i)
			s, err := sqlDataTypeStructFieldDataType(dbsql, f)
			if err != nil {
				return "", err
			}
			buffer.WriteString(s)
		}
		return buffer.String(), nil
	default:
		return sqlDataTypeStructFieldDataType(dbsql, field)
	}
	// return "", NewError(5, field.Name, x.Kind())
}

func sqlDataTypeStructFieldDataType(dbsql DBsql, sf reflect.StructField) (string, error) {
	t := sf.Type
	name, additional, info := evaluateName(sf, t)
	if info != "" {
		return info, nil
	}
	common.Log.Debugf("dbsql name %s and kind %s", name, t.Kind())
	switch t.Kind() {
	case reflect.String:
		return name + " " + common.Alpha.SqlType(255) + additional, nil
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		return name + " " + common.Integer.SqlType() + additional, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		return name + " " + common.Integer.SqlType() + additional, nil
	case reflect.Float32, reflect.Float64:
		return name + " " + common.Decimal.SqlType(10, 5) + additional, nil
	case reflect.Bool:
		return name + " " + common.Bit.SqlType(1) + additional, nil
	case reflect.Complex64, reflect.Complex128:
		return "", common.NewError(7)
	case reflect.Struct:
		var buffer bytes.Buffer
		ty := t
		for i := 0; i < ty.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := ty.Field(i)
			fmt.Println("Struct Field: " + f.Name)
			s, err := sqlDataTypeStructFieldDataType(dbsql, f)
			if err != nil {
				return "", err
			}

			buffer.WriteString(s)
		}
		buffer.WriteString(additional)
		return buffer.String(), nil
	case reflect.Array:
		common.Log.Debugf("Arrays %d", t.Len())
		if t.Elem().Kind() == reflect.Uint8 {
			return name + " " + common.Bytes.SqlType(dbsql.ByteArrayAvailable(), 8) + additional, nil
		}
		return "", common.NewError(8, sf.Name)
	case reflect.Slice:
		return "", common.NewError(9, sf.Name)
	default:
		//		return SqlDataType(t)
		// + " CONSTRAINT " + t.Name +
		// 	" CHECK (" + t.Name + " > 0)"
	}
	return "", common.NewError(6, sf.Name, t.Kind())
}

func evaluateName(sf reflect.StructField, tsf reflect.Type) (string, string, string) {
	// t := tsf
	// if t.Kind() == reflect.Pointer {
	// 	t = t.Elem()
	// }
	name := sf.Name
	additional := ""
	common.Log.Debugf("Found name " + name)
	if tagName, ok := sf.Tag.Lookup("dbsql"); ok {
		tagField := strings.Split(tagName, ":")
		if tagField[0] != "" {
			name = tagField[0]
		}
		if len(tagField) > 1 {
			additional = " " + tagField[1]
		}
		common.Log.Debugf("Overwrite to name " + name)
		if len(tagField) > 2 {
			if tagField[2] == "SERIAL" {
				return "", "", name + " SERIAL UNIQUE"
			}
		}
	}
	return name, additional, ""
}
