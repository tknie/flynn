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

	"github.com/tknie/errorrepo"
	"github.com/tknie/log"
)

type Query struct {
	Driver       ReferenceType
	TableName    string
	Search       string
	Join         string
	Fields       []string
	Order        []string
	Group        []string
	Parameters   []any
	Limit        string
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
	log.Log.Debugf("Query select with type %s", q.Driver)
	var selectCmd bytes.Buffer
	switch {
	case q.TableName == "":
		log.Log.Debugf("Table name missing")
		return "", errorrepo.NewError("DB000016")
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
	if len(q.Group) > 0 {
		selectCmd.WriteString(" GROUP BY ")
		for x, s := range q.Group {
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
			x := "ASC"
			entry := strings.Split(s, ":")
			switch {
			case len(entry) == 1:
			case len(entry) == 2:
				x = strings.ToUpper(entry[1])
			default:
				log.Log.Debugf("Split order incorect")
				return "", errorrepo.NewError("DB000017")
			}
			log.Log.Debugf("Order by: " + x)
			switch x {
			case "ASC", "DESC":
				selectCmd.WriteString(entry[0] + " " + x)
			default:
				selectCmd.WriteString(entry[0] + " ASC")
			}
		}
	}
	sqlCmd := selectCmd.String()
	if q.Limit != "" {
		switch q.Driver {
		case OracleType:
			log.Log.Debugf("Got Oracle limit")
			sqlCmd = fmt.Sprintf("SELECT * FROM (%s) WHERE rownum < %s", sqlCmd, q.Limit)
		default:
			sqlCmd += fmt.Sprintf(" LIMIT %s", q.Limit)
		}
	}
	log.Log.Debugf("Final select: %s", selectCmd.String())
	return sqlCmd, nil
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
		log.Log.Debugf("Generate data row values")
		scanRows, err = generateColumnByValues(rows)
	} else {
		log.Log.Debugf("Generate data struct values")
		vd, verr := result.GenerateColumnByStruct(search)
		scanRows = vd.Values
		err = verr
	}
	if err != nil {
		log.Log.Debugf("Error generating column: %v", err)
		return nil, err
	}
	result.Fields, err = rows.Columns()
	if err != nil {
		log.Log.Debugf("Error generating field columns: %v", err)
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
			case *sql.NullFloat64:
				if n.Valid {
					result.Rows[i] = n.Float64
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
	vd, err := result.GenerateColumnByStruct(search)
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
		err := rows.Scan(vd.ScanValues...)
		if err != nil {
			fmt.Println("Error scanning structs", vd.Values, err)
			log.Log.Debugf("Error during scan of struct: %v/%v", err, vd.Copy)
			return nil, err
		}
		err = vd.ShiftValues()
		if err != nil {
			return nil, err
		}

		result.Data = vd.Copy
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
			if nullOk {
				s := sql.NullFloat64{}
				colsValue = append(colsValue, &s)
				log.Log.Debugf("Create null number value")
			} else {
				s := float64(0)
				colsValue = append(colsValue, &s)
				log.Log.Debugf("Create non-null number value")
			}
			// s := int64(0)
			// colsValue = append(colsValue, &s)
		case "BYTEA", "BLOB":
			s := make([]byte, 0)
			colsValue = append(colsValue, &s)
		case "LONG":
			if nullOk {
				s := sql.NullString{}
				colsValue = append(colsValue, &s)
				log.Log.Debugf("Create null long value")
			} else {
				s := ""
				colsValue = append(colsValue, &s)
				log.Log.Debugf("Create non-null long value")
			}
		case "DATE", "TIMESTAMP":
			if nullOk {
				s := sql.NullTime{}
				colsValue = append(colsValue, &s)
				log.Log.Debugf("Create null time value")
			} else {
				n := time.Now()
				colsValue = append(colsValue, &n)
				log.Log.Debugf("Create non-null time value")
			}
		default:
			s := sql.NullString{}
			colsValue = append(colsValue, &s)
		}
	}
	return colsValue, nil
}
