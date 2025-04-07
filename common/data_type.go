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
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/tknie/log"
)

type DataType byte

const (
	None DataType = iota
	Alpha
	Text
	Unicode
	Integer
	BigInteger
	Decimal
	Number
	Bit
	Bytes
	CurrentTimestamp
	Date
	BLOB
	Character
)

var sqlTypes = []string{"", "VARCHAR(%d)", "TEXT", "UNICODE(%d)", "INTEGER", "bigint",
	"DECIMAL(%d,%d)", "NUMERIC(%d,%d)", "BIT(%d)", "BINARY(%d)",
	"TIMESTAMP(%s)", "DATE", "BLOB(%d)", "CHAR(%d)"}

func (dt DataType) SqlType(arg ...any) string {
	if dt == Bytes {
		if arg[0].(bool) {
			return "BYTEA"
		} else {
			return fmt.Sprintf("BINARY(%d)", arg[1:]...)
		}
	}
	return fmt.Sprintf(sqlTypes[dt], arg...)
}

func SqlDataType(sqlType string) DataType {
	for i, st := range sqlTypes {
		nt := st
		n := strings.IndexByte(st, '(')
		if n != -1 {
			nt = st[:n]
		}
		if nt == sqlType {
			return DataType(i)
		}
	}
	return None
}

func Unpointer(data []interface{}) []interface{} {
	for i, d := range data {
		switch v := d.(type) {
		case *int32:
			data[i] = *v
		case *int64:
			data[i] = *v
		case *uint32:
			data[i] = *v
		case *uint64:
			data[i] = *v
		case *string:
			data[i] = *v
		case *[]byte:
			data[i] = *v
		case *time.Time:
			data[i] = *v
		case *sql.NullString:
			data[i] = *v
		default:
			fmt.Printf("Unpointer error %T\n", d)
		}
	}
	return data
}
func CreateHeader(ct []*sql.ColumnType) []*Column {
	header := make([]*Column, 0)
	for _, t := range ct {
		l, _ := t.Length()
		c := &Column{Name: t.Name(),
			DataType: SqlDataType(t.DatabaseTypeName()),
			Length:   uint16(l)}
		header = append(header, c)
	}
	return header
}

func CreateTypeData(ct []*sql.ColumnType) []interface{} {
	scanData := make([]interface{}, 0)
	for _, t := range ct {
		switch t.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "UNICODE":
			//if nok, _ := t.Nullable(); nok {
			v := sql.NullString{}
			scanData = append(scanData, &v)
			//} else {
			//	s := ""
			//	scanData = append(scanData, &s)
			//}
		case "NUMBER", "INT4", "INTEGER":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &sql.NullInt32{})
			} else {
				i := int32(0)
				scanData = append(scanData, &i)
			}
		case "BIGINT", "INT8":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &sql.NullInt64{})
			} else {
				i := int64(0)
				scanData = append(scanData, &i)
			}
		case "DECIMAL":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &sql.NullFloat64{})
			} else {
				f := float64(0)
				scanData = append(scanData, &f)
			}
		case "BIT":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &sql.NullByte{})
			} else {
				b := byte(0)
				scanData = append(scanData, &b)
			}
		case "BPCHAR":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &sql.NullString{})
			} else {
				b := ""
				scanData = append(scanData, &b)
			}
		case "BLOB", "BINARY", "BYTEA", "DATA":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &NullBytes{})
			} else {
				b := make([]byte, 0)
				scanData = append(scanData, &b)
			}
		case "TIMESTAMP":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &sql.NullTime{})
			} else {
				var t time.Time
				scanData = append(scanData, &t)
			}
		case "UNSIGNED INT":
			if nok, _ := t.Nullable(); nok {
				scanData = append(scanData, &NullUint{})
			} else {
				v := uint64(0)
				scanData = append(scanData, &v)
			}
		default:
			fmt.Println("Type not defined ", t.Name(), t.DatabaseTypeName())
			log.Log.Fatal("Type not defined " + t.Name())
		}
	}
	return scanData
}

type NullBytes struct {
	Bytes []byte
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullBytes) Scan(value any) error {
	if value == nil {
		n.Bytes, n.Valid = nil, false
		return nil
	}
	n.Valid = true
	n.Bytes = value.([]byte)
	return nil
}

// Value implements the driver Valuer interface.
func (n NullBytes) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bytes, nil
}

type NullUint struct {
	Val   uint64
	Valid bool
}

// Scan implements the Scanner interface.
func (n *NullUint) Scan(value any) error {
	if value == nil {
		n.Val, n.Valid = 0, false
		return nil
	}
	n.Valid = true
	n.Val = value.(uint64)
	return nil
}

// Value implements the driver Valuer interface.
func (n NullUint) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Val, nil
}
