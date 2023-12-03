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

package common

import (
	"bytes"
	"reflect"
	"strings"
	"time"

	"github.com/tknie/log"
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
	DataType  interface{}
	RowNames  map[string][]string
	RowFields []string
	SetType   SetType
	FieldSet  map[string]void
	RowValues []any
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
func (dynamic *typeInterface) CreateQueryValues() (any, []any) {
	if dynamic.SetType == EmptySet {
		log.Log.Debugf("Empty set defined")
		return nil, nil
	}
	log.Log.Debugf("Create query values")
	//	fieldType := reflect.TypeOf(dynamic.DataType)
	value := reflect.ValueOf(dynamic.DataType)
	if value.Type().Kind() == reflect.Pointer {
		//		fmt.Println(fieldType.Kind(), value.Kind())
		value = value.Elem()
	}
	copyValue := reflect.New(value.Type())
	if log.IsDebugLevel() {
		log.Log.Debugf("Value %s %T", value.Type().Name(), value.Interface())
		log.Log.Debugf("Main1: %T", copyValue.Interface())
	}
	elemValue := copyValue
	rt := elemValue.Type()
	// fmt.Println(rt.Name(), rt.Kind(), rt.Kind() == reflect.Pointer, elemValue)
	if rt.Kind() == reflect.Pointer {
		elemValue = elemValue.Elem()
		log.Log.Debugf("Sub: %T", elemValue.Interface())
	}
	log.Log.Debugf("Final: %T", elemValue.Interface())
	dynamic.generateField(elemValue, true)
	return copyValue.Interface(), dynamic.RowValues
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
	return dynamic.RowValues
}

func (dynamic *typeInterface) generateField(elemValue reflect.Value, scan bool) {
	log.Log.Debugf("Generate field of Struct: %T %s -> %v",
		elemValue.Interface(), elemValue.Type().Name(), scan)
	for fi := 0; fi < elemValue.NumField(); fi++ {
		fieldType := elemValue.Type().Field(fi)
		tag := fieldType.Tag
		cv := elemValue.Field(fi)
		d := tag.Get(TagName)
		fieldName := fieldType.Name
		if d != "" {
			log.Log.Debugf("Tag for %s = %s", fieldType.Name, tag)
			if d == ":ignore" {
				continue
			}
			options := strings.Split(d, ":")
			if options[0] != "" {
				fieldName = options[0]
			}
		}
		if cv.Kind() == reflect.Pointer {
			x := reflect.New(cv.Type().Elem())
			log.Log.Debugf("Work on pointer %v %s", x, cv.Type().String())
			cv.Set(x)
			cv = x.Elem()
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
					dynamic.RowValues = append(dynamic.RowValues, ptr.Interface())
				}
				continue
			default:
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
					log.Log.Debugf("Add value %T %s %s", ptr.Interface(), fieldName, elemValue.Type().Name())
					dynamic.RowValues = append(dynamic.RowValues, ptr.Interface())
				} else {
					log.Log.Debugf("Add no-scan value %T %s %s", cv.Interface(), fieldName, elemValue.Type().Name())
					dynamic.RowValues = append(dynamic.RowValues, cv.Interface())
				}
			} else {
				log.Log.Debugf("Skip field not in field set")
			}
		}
		log.Log.Debugf("Row values len=%d", len(dynamic.RowNames))
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
				case "":
					if s[0] != "" {
						// this is if the inmap repository-less map is used
						log.Log.Debugf("Field name %s", s[0])
						fieldName = s[0]
					}
				default:
					continue
				}
			} else {
				fieldName = s[0]
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
