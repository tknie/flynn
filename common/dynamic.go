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
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/tknie/log"
)

type SetType byte

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

func CreateInterface(i interface{}, fields []string) *typeInterface {
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
	dynamic.generateFieldNames(ri)
	log.Log.Debugf("Final created field list generated %#v", dynamic.RowFields)
	return dynamic
}

func (dynamic *typeInterface) CreateQueryFields() string {
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
		return nil, nil
	}
	fieldType := reflect.TypeOf(dynamic.DataType)
	value := reflect.ValueOf(dynamic.DataType)
	if value.Type().Kind() == reflect.Pointer {
		fmt.Println(fieldType.Kind(), value.Kind())
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
	dynamic.generateField(elemValue)
	return copyValue.Interface(), dynamic.RowValues
}

func (dynamic *typeInterface) generateField(elemValue reflect.Value) {
	log.Log.Debugf("Generate field of Struct: %T %s", elemValue.Interface(), elemValue.Type().Name())
	for fi := 0; fi < elemValue.NumField(); fi++ {
		fieldType := elemValue.Type().Field(fi)
		tag := fieldType.Tag
		cv := elemValue.Field(fi)
		d := tag.Get("db")
		if d == ":ignore" {
			continue
		}
		log.Log.Debugf("Work on field %s", fieldType.Name)
		if cv.Kind() == reflect.Struct {
			switch elemValue.Interface().(type) {
			case time.Time:
				checkField := dynamic.checkFieldSet(fieldType.Name)
				if checkField {
					ptr := elemValue.Addr()
					t := reflect.TypeOf(elemValue)
					log.Log.Debugf("Add value %T %s %s", ptr.Interface(), elemValue.Type().Name(), t.Name())
					dynamic.RowValues = append(dynamic.RowValues, ptr.Interface())
				}
				continue
			}
			dynamic.generateField(cv)
		} else {
			checkField := dynamic.checkFieldSet(fieldType.Name)
			if checkField {
				var ptr reflect.Value
				if cv.CanAddr() {
					log.Log.Debugf("Use Addr")
					ptr = cv.Addr()
				} else {
					ptr = reflect.New(cv.Type())
					log.Log.Debugf("Got Addr pointer %#v", ptr)
					ptr.Elem().Set(cv)
				}
				log.Log.Debugf("Add value %T %s %s", ptr.Interface(), fieldType.Name, elemValue.Type().Name())
				dynamic.RowValues = append(dynamic.RowValues, ptr.Interface())
			}
		}
	}
}

func (dynamic *typeInterface) checkFieldSet(fieldName string) bool {
	ok := true
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
		tag := ct.Tag.Get("db")

		// If tag is given
		if tag != "" {
			s := strings.Split(tag, ":")

			if len(s) > 1 {
				switch s[1] {
				case "key":
					dynamic.RowNames["#key"] = []string{fieldName}
				case "isn":
					dynamic.RowNames["#index"] = []string{fieldName}
					continue
				case "ignore":
					continue
				case "":
					// this is if the inmap repository-less map is used
				default:
					continue
				}
			}
		}

		if ct.Type.Kind() == reflect.Struct {
			log.Log.Debugf("Struct-Kind of %s", ct.Type.Name())
			//continue generate field names
			if ct.Type.Name() != "Time" {
				dynamic.generateFieldNames(ct.Type)
			} else {
				ok := dynamic.checkFieldSet(fieldName)
				if ok {
					dynamic.RowFields = append(dynamic.RowFields, fieldName)
				}
			}
		} else {
			log.Log.Debugf("Kind of %s: %s", fieldName, ct.Type.Kind())
			// copy of subfields
			// copy(subFields, fields)
			ok := dynamic.checkFieldSet(fieldName)
			if ok {
				dynamic.RowFields = append(dynamic.RowFields, fieldName)
			}
		}
		// Handle special case for pointer and slices
		switch ct.Type.Kind() {
		case reflect.Ptr:
			dynamic.generateFieldNames(ct.Type.Elem())
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
