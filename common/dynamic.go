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
	"bytes"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"reflect"
	"strings"
	"time"

	"github.com/tknie/log"
	"gopkg.in/yaml.v3"
)

type SetType byte

// TagName name to be used for tagging structure field
const TagName = "flynn"

const (
	EmptySet SetType = iota
	AllSet
	GivenSet
)

type void struct{}

var member void

type typeInterface struct {
	DataType   interface{}
	RowNames   map[string][]string
	RowFields  []string
	SetType    SetType
	FieldSet   map[string]void
	ValueRefTo []any
	ScanValues []any
}

func CreateInterface(i interface{}, createFields []string) *typeInterface {
	fields := createFields
	if fields == nil {
		fields = []string{"*"}
	}
	ri := reflect.TypeOf(i)
	if ri.Kind() == reflect.Ptr {
		ri = ri.Elem()
	}
	log.Log.Debugf("Create dynamic interface with fields %#v", fields)
	set := make(map[string]void) // New empty set
	dynamic := &typeInterface{DataType: i, RowNames: make(map[string][]string),
		RowFields: make([]string, 0), FieldSet: set}
	for _, f := range fields {
		switch f {
		case "*":
			dynamic.SetType = AllSet
		case "":
			dynamic.SetType = EmptySet
			return dynamic
		default:
			dynamic.SetType = GivenSet
			dynamic.FieldSet[strings.ToLower(f)] = member
		}
	}
	log.Log.Debugf("FieldSet defined: %#v", dynamic.FieldSet)
	dynamic.generateFieldNames(ri)
	log.Log.Debugf("Final created field list generated %#v", dynamic.RowFields)
	return dynamic
}

func (dynamic *typeInterface) CreateQueryFields() string {
	if dynamic.SetType == EmptySet {
		return ""
	}
	var buffer bytes.Buffer
	for _, fieldName := range dynamic.RowFields {
		if buffer.Len() > 0 {
			buffer.WriteRune(',')
		}
		buffer.WriteString(fieldName)
	}
	return buffer.String()
}

// CreateQueryValues create query value copy of struct
func (dynamic *typeInterface) CreateQueryValues() (any, []any, []any) {
	if dynamic.SetType == EmptySet {
		log.Log.Debugf("Empty set defined")
		return nil, nil, nil
	}
	log.Log.Debugf("Create query values")
	value := reflect.ValueOf(dynamic.DataType)
	if value.Type().Kind() == reflect.Pointer {
		value = value.Elem()
	}
	copyValue := reflect.New(value.Type())
	if log.IsDebugLevel() {
		log.Log.Debugf("Value %s %T", value.Type().Name(), value.Interface())
		log.Log.Debugf("Main1: %T", copyValue.Interface())
	}
	elemValue := copyValue
	rt := elemValue.Type()
	if rt.Kind() == reflect.Pointer {
		elemValue = elemValue.Elem()
		log.Log.Debugf("Sub: %T", elemValue.Interface())
	}
	log.Log.Debugf("Final: %T", elemValue.Interface())
	dynamic.generateField(elemValue, true)
	return copyValue.Interface(), dynamic.ValueRefTo, dynamic.ScanValues
}

// CreateQueryValues create query value copy of struct
func (dynamic *typeInterface) CreateInsertValues() []any {
	if dynamic.SetType == EmptySet {
		log.Log.Debugf("Empty set defined")
		return nil
	}
	log.Log.Debugf("Create insert values")
	value := reflect.ValueOf(dynamic.DataType)
	if value.Type().Kind() == reflect.Pointer {
		//		fmt.Println(fieldType.Kind(), value.Kind())
		value = value.Elem()
	}
	dynamic.generateField(value, false)
	return dynamic.ValueRefTo
}

// generateField generate field values for dynamic query.
// 'scan' is used to consider case for read (field creation out of database) or
// write (no creation, data is used by application)
func (dynamic *typeInterface) generateField(elemValue reflect.Value, scan bool) {
	log.Log.Debugf("Generate field of Struct: %T %s -> scan=%v",
		elemValue.Interface(), elemValue.Type().Name(), scan)
	for fi := 0; fi < elemValue.NumField(); fi++ {
		fieldType := elemValue.Type().Field(fi)
		tag := fieldType.Tag
		cv := elemValue.Field(fi)
		d := tag.Get(TagName)
		tags := strings.Split(d, ":")
		fieldName := fieldType.Name
		log.Log.Debugf("%s: kind %v", fieldName, cv.Kind())
		if len(tags) > 1 {
			log.Log.Debugf("Tag for %s = %s", fieldType.Name, tag)
			if tags[1] == "ignore" {
				continue
			}
		}
		if len(tags) > 0 {
			if tags[0] != "" {
				fieldName = tags[0]
			}
		}
		if cv.Kind() == reflect.Pointer {
			if !scan && cv.IsNil() {
				log.Log.Debugf("IsNil pointer = %v -> %s", cv.IsNil(), cv.Type().String())
				if len(tags) > 2 {
					switch tags[2] {
					case "YAML", "XML", "JSON":
						dynamic.ValueRefTo = append(dynamic.ValueRefTo, "")
						continue
					}
				}

				// x := reflect.New(cv.Type().Elem())
				x := reflect.Indirect(reflect.New(cv.Type().Elem()))

				dynamic.generateField(x, scan)
				// dynamic.ValueRefTo = append(dynamic.ValueRefTo, nil)
				continue
			}
			if scan {
				x := reflect.New(cv.Type().Elem())
				log.Log.Debugf("Work on pointer %v %s", x, cv.Type().String())
				cv.Set(x)
				cv = x.Elem()
			} else {
				cv = cv.Elem()
				log.Log.Debugf("Go on pointer %s: kind %v", fieldName, cv.Kind())
			}
		}
		if cv.Kind() == reflect.Struct {
			log.Log.Debugf("Work on struct %s", fieldType.Name)
			switch cv.Interface().(type) {
			case time.Time:
				checkField := dynamic.checkFieldSet(fieldType.Name)
				if checkField {
					ptr := cv.Addr()
					t := reflect.TypeOf(cv)
					log.Log.Debugf("Add Time %T %s %s", ptr.Interface(), cv.Type().Name(), t.Name())
					dynamic.ValueRefTo = append(dynamic.ValueRefTo, ptr.Interface())
					dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullTime{})
				}
				continue
			default:
				if len(tags) > 2 {
					switch tags[2] {
					case "YAML":
						out, err := yaml.Marshal(cv.Interface())
						if err != nil {
							return
						}
						dynamic.ValueRefTo = append(dynamic.ValueRefTo, string(out))
						continue
					case "XML":
						out, err := xml.Marshal(cv.Interface())
						if err != nil {
							return
						}
						dynamic.ValueRefTo = append(dynamic.ValueRefTo, string(out))
						continue
					case "JSON":
						out, err := json.Marshal(cv.Interface())
						if err != nil {
							return
						}
						dynamic.ValueRefTo = append(dynamic.ValueRefTo, string(out))
						continue
					default:
						dynamic.ValueRefTo = append(dynamic.ValueRefTo, "")
						continue
					}
				}
				dynamic.generateField(cv, scan)
			}
		} else {
			log.Log.Debugf("Work on field %s -> %v", fieldName, scan)
			checkField := dynamic.checkFieldSet(fieldName)
			if checkField {
				if scan {
					var ptr reflect.Value
					if cv.CanAddr() {
						log.Log.Debugf("Use Addr")
						ptr = cv.Addr()
					} else {
						ptr = reflect.New(cv.Type())
						log.Log.Debugf("Got Addr pointer %#v", ptr)
						ptr.Elem().Set(cv)
					}
					ptrInt := ptr.Interface()
					log.Log.Debugf("Add value %T pointer=%p %s %s", ptrInt, ptrInt, fieldName, elemValue.Type().Name())
					dynamic.ValueRefTo = append(dynamic.ValueRefTo, ptrInt)
					switch cv.Kind() {
					case reflect.String:
						dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullString{})
					case reflect.Bool:
						dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullBool{})
					case reflect.Int8:
						dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullByte{})
					case reflect.Int16:
						dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullInt16{})
					case reflect.Int32:
						dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullInt32{})
					case reflect.Int64:
						dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullInt64{})
					case reflect.Float32, reflect.Float64:
						dynamic.ScanValues = append(dynamic.ScanValues, &sql.NullFloat64{})
					default:
						log.Log.Debugf("'%s' dynamic Kind not defined for SQL %s", fieldType.Name, cv.Kind().String())
						dynamic.ScanValues = append(dynamic.ScanValues, ptrInt)
					}
				} else {
					switch cv.Kind() {
					case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
						reflect.UnsafePointer, reflect.Interface, reflect.Slice:
						if cv.IsNil() {
							dynamic.ValueRefTo = append(dynamic.ValueRefTo, nil)
						} else {
							dynamic.ValueRefTo = append(dynamic.ValueRefTo, cv.Interface())
						}
					default:
						if cv.IsValid() {
							log.Log.Debugf("Add no-scan value type=%T field=%s elemValueName=%s: value=%#v",
								cv.Interface(), fieldName, elemValue.Type().Name(), cv.Interface())
							dynamic.ValueRefTo = append(dynamic.ValueRefTo, cv.Interface())
						} else {
							dynamic.ValueRefTo = append(dynamic.ValueRefTo, nil)
						}
					}
				}
			} else {
				log.Log.Debugf("Skip field not in field set")
			}
		}
		log.Log.Debugf("Row values len=%d", len(dynamic.ValueRefTo))
	}
}

func (dynamic *typeInterface) checkFieldSet(fieldName string) bool {
	ok := true
	log.Log.Debugf("Check %s in %#v", strings.ToLower(fieldName), dynamic.FieldSet)
	if dynamic.SetType == GivenSet {
		_, ok = dynamic.FieldSet[strings.ToLower(fieldName)]
		log.Log.Debugf("Restrict to %v", ok)
	}

	return ok
}

// generateFieldNames examine all structure-tags in the given structure and build up
// field names map pointing to corresponding path with names of structures
func (dynamic *typeInterface) generateFieldNames(ri reflect.Type) {
	if log.IsDebugLevel() {
		log.Log.Debugf("Generate field names...")
	}
	if ri.Kind() != reflect.Struct {
		return
	}
	for fi := 0; fi < ri.NumField(); fi++ {
		ct := ri.Field(fi)
		fieldName := ct.Name
		log.Log.Debugf("Work on fieldname %s", fieldName)
		tag := ct.Tag.Get(TagName)

		// If tag is given
		if tag != "" {
			log.Log.Debugf("Field tag %s", tag)
			s := strings.Split(tag, ":")
			if len(s) > 0 && s[0] != "" {
				fieldName = s[0]
			}
			if len(s) > 1 {
				log.Log.Debugf("Field tag option %s", s[1])
				switch s[1] {
				case "key":
					dynamic.RowNames["#key"] = []string{fieldName}
				case "isn":
					dynamic.RowNames["#index"] = []string{fieldName}
					continue
				case "ignore":
					continue
				default:
				}
			}
			if len(s) > 2 {
				log.Log.Debugf("Field tag option %s", s[1])
				switch s[2] {
				case "YAML", "XML", "JSON":
					dynamic.RowFields = append(dynamic.RowFields, fieldName)
					continue
				}
			}
		}
		log.Log.Debugf("Work on final fieldname %s", fieldName)
		log.Log.Debugf("Add field %s", ct.Name)
		st := ct.Type
		if st.Kind() == reflect.Pointer {
			log.Log.Debugf("Pointer-Kind of %s", st.Name())
			st = st.Elem()
			log.Log.Debugf("Pointer-Struct-Kind of %s -> %s", st.Name(), st.Kind())
		}
		if st.Kind() == reflect.Struct {
			log.Log.Debugf("Struct-Kind of %s", st.Name())
			//continue generate field names
			if st.Name() != "Time" {
				dynamic.generateFieldNames(st)
			} else {
				ok := dynamic.checkFieldSet(fieldName)
				if ok {
					dynamic.RowFields = append(dynamic.RowFields, fieldName)
					log.Log.Debugf("RowFields: Add field name %s", fieldName)
				}
			}
		} else {
			log.Log.Debugf("Kind of %s: %s", fieldName, ct.Type.Kind())
			// copy of subfields
			// copy(subFields, fields)
			ok := dynamic.checkFieldSet(fieldName)
			if ok {
				dynamic.RowFields = append(dynamic.RowFields, fieldName)
				log.Log.Debugf("RowFields: Add field name %s", fieldName)
			}
		}
		// Handle special case for pointer and slices
		switch ct.Type.Kind() {
		case reflect.Ptr:
			// dynamic.generateFieldNames(ct.Type.Elem())
		case reflect.Slice:
			sliceT := ct.Type.Elem()
			if sliceT.Kind() == reflect.Ptr {
				sliceT = sliceT.Elem()
			}
			dynamic.generateFieldNames(sliceT)
		}
	}
	log.Log.Debugf("Field list generated %#v", dynamic.RowFields)
}

func ShiftValues(scanValues, values []any) (err error) {
	for d, v := range scanValues {
		if _, ok := v.(sqlInterface); ok {
			vv, err := v.(sqlInterface).Value()
			if err != nil {
				return err
			}
			if vv != nil {
				log.Log.Debugf("(%d) Found value %T pointer=%p", d, values[d], values[d])
				switch vt := values[d].(type) {
				case *int:
					*vt = vv.(int)
				case *string:
					*vt = vv.(string)
				case *time.Time:
					*vt = vv.(time.Time)
				default:
					log.Log.Fatalf("%d: Unknown type for shifting value %T -> %T", d, values[d], vv)
				}
			}
		}
	}
	return nil
}
