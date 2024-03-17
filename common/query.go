/*
* Copyright 2023-2024 Thorsten A. Knieling
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
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/tknie/log"
)

type Query struct {
	TableName    string
	Search       string
	Join         string
	Fields       []string
	Order        []string
	Group        []string
	Parameters   []any
	Limit        uint32
	Blocksize    int32
	Descriptor   bool
	DataStruct   any
	TypeInfo     any
	FctParameter any
}

type sqlInterface interface {
	Value() (driver.Value, error)
}

func (q *Query) Select() (string, error) {
	var selectCmd bytes.Buffer
	switch {
	case q.TableName == "":
		log.Log.Debugf("Table name missing")
		return "", fmt.Errorf("table name missing")
	case q.DataStruct != nil:
		selectCmd.WriteString("SELECT ")
		if q.Descriptor {
			selectCmd.WriteString("DISTINCT ")
		}
		ti := CreateInterface(q.DataStruct, q.Fields)
		q.TypeInfo = ti
		selectCmd.WriteString(ti.CreateQueryFields())
		selectCmd.WriteString(" FROM " + q.TableName + " tn")
	default:
		selectCmd.WriteString("SELECT ")
		if q.Descriptor {
			selectCmd.WriteString("DISTINCT ")
		}
		if len(q.Fields) == 0 {
			selectCmd.WriteString("*")
		} else {
			for i, s := range q.Fields {
				if i > 0 {
					selectCmd.WriteString(",")
				}
				selectCmd.WriteString(s)
			}
		}
		selectCmd.WriteString(" FROM " + q.TableName + " tn")
	}
	if q.Search != "" {
		selectCmd.WriteString(" WHERE " + q.Search)
	}
	if q.Join != "" {
		selectCmd.WriteString(" LIKE " + q.Join)
	}
	if len(q.Order) > 0 {
		selectCmd.WriteString(" GROUP BY ")
		for x, s := range q.Order {
			if x > 0 {
				selectCmd.WriteString(",")
			}
			selectCmd.WriteString(s)
		}
	}
	if len(q.Order) > 0 {
		selectCmd.WriteString(" ORDER BY ")
		for x, s := range q.Order {
			if x > 0 {
				selectCmd.WriteString(",")
			}
			entry := strings.Split(s, ":")
			if len(entry) != 2 {
				log.Log.Debugf("Split order incorect")
				return "", fmt.Errorf("order by syntax error (':' separator missing)")
			}
			x := strings.ToUpper(entry[1])
			switch x {
			case "ASC", "DESC":
				selectCmd.WriteString(entry[0] + " " + x)
			default:
				selectCmd.WriteString(entry[0] + " ASC")
			}
		}
	}
	if q.Limit > 0 {
		selectCmd.WriteString(fmt.Sprintf(" LIMIT %d", q.Limit))
	}
	log.Log.Debugf("Final select: %s", selectCmd.String())
	return selectCmd.String(), nil
}

func (search *Query) ParseRows(rows *sql.Rows, f ResultFunction) (result *Result, err error) {
	result = &Result{}

	result.Data = search.DataStruct
	ct, err := rows.ColumnTypes()
	if err != nil {
		log.Log.Debugf("Error generating column header: %v", err)
		return nil, err
	}
	result.Header = CreateHeader(ct)

	// rows := make([]any, len(result.Rows))
	var scanRows []any
	if search.DataStruct == nil {
		log.Log.Debugf("Generate data struct values")
		scanRows, err = generateColumnByValues(rows)
	} else {
		log.Log.Debugf("Generate data values")
		_, scanRows, _, err = result.GenerateColumnByStruct(search)
	}
	if err != nil {
		log.Log.Debugf("Error generating column: %v", err)
		return nil, err
	}
	result.Fields, err = rows.Columns()
	if err != nil {
		return nil, err
	}
	log.Log.Debugf("Parse columns rows: %d fields: %v", len(scanRows), result.Fields)
	for rows.Next() {
		result.Counter++
		log.Log.Debugf("Found record")
		err := rows.Scan(scanRows...)
		if err != nil {
			fmt.Println("Error scanning rows", scanRows)
			log.Log.Debugf("Error during scan rows: %v", err)
			return nil, err
		}
		result.Rows = make([]any, len(scanRows))
		for i, r := range scanRows {
			log.Log.Debugf("Parse Row %T", r)
			switch n := r.(type) {
			case *sql.NullByte:
				if n.Valid {
					result.Rows[i] = n.Byte
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullBool:
				if n.Valid {
					result.Rows[i] = n.Bool
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullString:
				if n.Valid {
					result.Rows[i] = n.String
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullInt32:
				if n.Valid {
					result.Rows[i] = n.Int32
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullInt64:
				if n.Valid {
					result.Rows[i] = n.Int64
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullInt16:
				if n.Valid {
					result.Rows[i] = n.Int16
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullTime:
				if n.Valid {
					result.Rows[i] = n.Time
				} else {
					result.Rows[i] = nil
				}
			default:
				result.Rows[i] = r
			}
		}
		err = f(search, result)
		if err != nil {
			return nil, err
		}
	}
	log.Log.Debugf("Rows procession ended")
	return
}

func (search *Query) ParseStruct(rows *sql.Rows, f ResultFunction) (result *Result, err error) {
	if search.DataStruct == nil {
		return search.ParseRows(rows, f)
	}
	result = &Result{}
	log.Log.Debugf("Parse using struct...")
	result.Data = search.DataStruct
	copy, values, scanValues, err := result.GenerateColumnByStruct(search)
	if err != nil {
		log.Log.Debugf("Error generating column: %v", err)
		return nil, err
	}
	log.Log.Debugf("Parse columns rows")
	result.Fields, err = rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(scanValues...)
		if err != nil {
			fmt.Println("Error scanning structs", values, err)
			log.Log.Debugf("Error during scan of struct: %v/%v", err, copy)
			return nil, err
		}
		err = ShiftValues(scanValues, values)
		if err != nil {
			return nil, err
		}

		result.Data = copy
		err = f(search, result)
		if err != nil {
			return nil, err
		}
	}
	return
}

func generateColumnByValues(rows *sql.Rows) ([]any, error) {
	colsType, err := rows.ColumnTypes()
	if err != nil {
		if log.IsDebugLevel() {
			log.Log.Debugf("Error cols read: %v", err)
		}
		return nil, err
	}
	log.Log.Debugf("Create columns values")
	colsValue := make([]any, 0)
	for nr, col := range colsType {
		len, ok := col.Length()
		_, nullOk := col.Nullable()
		if log.IsDebugLevel() {
			log.Log.Debugf("Colnr=%d name=%s len=%d ok=%v null=%v typeName=%s",
				nr, col.Name(), len, ok, nullOk, col.DatabaseTypeName())
		}
		switch col.DatabaseTypeName() {
		case "VARCHAR2", "VARCHAR":
			if nullOk {
				s := sql.NullString{}
				colsValue = append(colsValue, &s)
				log.Log.Debugf("Create null string value")
			} else {
				s := ""
				colsValue = append(colsValue, &s)
				log.Log.Debugf("Create non-null string value")
			}
		case "NUMBER":
			s := int64(0)
			colsValue = append(colsValue, &s)
		case "BYTEA", "BLOB":
			s := make([]byte, 0)
			colsValue = append(colsValue, &s)
		case "LONG":
			s := ""
			colsValue = append(colsValue, &s)
		case "DATE", "TIMESTAMP":
			n := time.Now()
			colsValue = append(colsValue, &n)
		default:
			s := sql.NullString{}
			colsValue = append(colsValue, &s)
		}
	}
	return colsValue, nil
}
