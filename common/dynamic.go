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

type typeInterface struct {
	DataType  interface{}
	RowNames  map[string][]string
	RowFields []string
	RowValues []any
}

func CreateInterface(i interface{}) *typeInterface {
	ri := reflect.TypeOf(i)
	if ri.Kind() == reflect.Ptr {
		ri = ri.Elem()
	}
	dynamic := &typeInterface{DataType: i, RowNames: make(map[string][]string),
		RowFields: make([]string, 0)}
	generateFieldNames(ri, dynamic.RowNames, dynamic.RowFields)
	return dynamic
}

func (dynamic *typeInterface) CreateQueryFields() string {
	var buffer bytes.Buffer
	for fieldName := range dynamic.RowNames {
		if buffer.Len() > 0 {
			buffer.WriteRune(',')
		}
		buffer.WriteString(fieldName)
	}
	return buffer.String()
}

// CreateQueryValues create query value copy of struct
func (dynamic *typeInterface) CreateQueryValues() (any, []any) {
	value := reflect.ValueOf(dynamic.DataType)
	copyValue := reflect.New(value.Elem().Type())
	if log.IsDebugLevel() {
		log.Log.Debugf("Value %s %T", value.Elem().Type().Name(), value.Interface())
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
	switch elemValue.Interface().(type) {
	case time.Time:
		ptr := elemValue.Addr()
		log.Log.Debugf("Add value %T %s", ptr.Interface(), elemValue.Type().Name())
		dynamic.RowValues = append(dynamic.RowValues, ptr.Interface())
		return
	}
	for fi := 0; fi < elemValue.NumField(); fi++ {
		fieldType := elemValue.Type().Field(fi)
		tag := fieldType.Tag
		cv := elemValue.Field(fi)
		d := tag.Get("db")
		if d == ":ignore" {
			continue
		}
		if cv.Kind() == reflect.Struct {
			dynamic.generateField(cv)
		} else {
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

func (dynamic *typeInterface) CreateQueryValues2() (any, []any) {
	log.Log.Debugf("Create query values")
	rt := reflect.TypeOf(dynamic.DataType)
	if rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	ptr2 := reflect.New(rt)
	dynamic.DataType = ptr2.Interface()
	dynamic.createQueryValues(dynamic.DataType)
	log.Log.Debugf("Number values created %d out of %s", len(dynamic.RowValues), dynamic.RowNames)
	log.Log.Debugf("Values %#v", dynamic.RowValues)
	return ptr2.Interface(), dynamic.RowValues
}

func (dynamic *typeInterface) createQueryValues(dataType interface{}) {
	rv := reflect.ValueOf(dataType)
	rt := reflect.TypeOf(dataType)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
		rt = rt.Elem()
	}
	if log.IsDebugLevel() {
		rt := reflect.TypeOf(dataType)
		log.Log.Debugf("Scan query values in struct %v %d %s", rv, rv.NumField(), rt.Name())
	}
	for fi := 0; fi < rv.NumField(); fi++ {
		f := rv.Field(fi)
		fmt.Println(f.Kind(), f.Type().Name())
		if f.Kind() == reflect.Pointer {
			f = f.Elem()
		}
		if f.Kind() == reflect.Struct {
			dynamic.createQueryValues(f.Interface())
		} else {
			if f.IsValid() {
				if f.CanSet() {
					log.Log.Debugf("-> %s %T", dataType, dataType)
					if f.Kind() == reflect.String {
						f.SetString(dataType.(string))
					} else if f.Kind() == reflect.Struct {
						f.Set(reflect.ValueOf(dataType))
					}
				}
			}
		}
		// subRt := rt.Field(fi)
		// if log.IsDebugLevel() {
		// 	log.Log.Debugf("Field %d:%s %v canAddr: %v %s %s", fi, cv.Type().Name(), cv.CanInterface(), cv.CanAddr(), rt.Name(), subRt.Name)
		// }
		// if cv.Kind() == reflect.Struct {
		// 	dynamic.createQueryValues(cv.Interface())
		// } else {
		// 	var ptr reflect.Value
		// 	if cv.CanAddr() {
		// 		log.Log.Debugf("Use Addr")
		// 		ptr = cv.Addr()
		// 	} else {
		// 		log.Log.Debugf("New Addr pointer")
		// 		ptr = reflect.New(cv.Type())
		// 		log.Log.Debugf("Got Addr pointer %#v", ptr)
		// 		ptr.Elem().Set(cv)
		// 	}
		// 	if log.IsDebugLevel() {
		// 		log.Log.Debugf("FieldPTR: %T / %T / %v\n", ptr.Type().Name(), ptr.Interface(), ptr.Interface())
		// 	}
		// 	// x := ptr.Pointer()
		// 	// xv := reflect.ValueOf(x)
		// 	// fmt.Println("PTR Kind:", xv.Kind(), xv.Kind() == reflect.Pointer)
		// 	// if xv.Kind() != reflect.Pointer {
		// 	// 	log.Fatalf("FATAL ERROR not a pointer ..... exiting FieldPTR: %T / %T / %v\n", ptr.Type().Name(), x, x)
		// 	// }
		// 	//rf := reflect.NewAt(cv.Type(), unsafe.Pointer(ptr.Pointer())) // .Elem()
		// 	//dynamic.RowValues = append(dynamic.RowValues, rf.Interface())
		// 	dynamic.RowValues = append(dynamic.RowValues, ptr.Interface())
		// }
	}
	if log.IsDebugLevel() {
		log.Log.Debugf("Len row values: %d", len(dynamic.RowValues))
	}
}

// generateFieldNames examine all structure-tags in the given structure and build up
// field names map pointing to corresponding path with names of structures
func generateFieldNames(ri reflect.Type, f map[string][]string, fields []string) {
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
					f["#key"] = []string{fieldName}
				case "isn":
					f["#index"] = []string{fieldName}
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

		subFields := make([]string, len(fields))
		if ct.Type.Kind() == reflect.Struct {
			log.Log.Debugf("Struct-Kind of %s", ct.Type.Name())
			//continue generate field names
			if ct.Type.Name() != "Time" {
				generateFieldNames(ct.Type, f, fields)
			} else {
				fields = append(fields, fieldName)
				copy(subFields, fields)
				subFields = append(subFields, fieldName)
				f[fieldName] = subFields
			}
		} else {
			fields = append(fields, fieldName)
			log.Log.Debugf("Kind of %s: %s", fieldName, ct.Type.Kind())
			// copy of subfields
			copy(subFields, fields)
			subFields = append(subFields, fieldName)
			f[fieldName] = subFields
			log.Log.Debugf("%s -> SubFields %#v", fieldName, subFields)
		}
		// Handle special case for pointer and slices
		switch ct.Type.Kind() {
		case reflect.Ptr:
			generateFieldNames(ct.Type.Elem(), f, subFields)
		case reflect.Slice:
			sliceT := ct.Type.Elem()
			if sliceT.Kind() == reflect.Ptr {
				sliceT = sliceT.Elem()
			}
			generateFieldNames(sliceT, f, subFields)
		}
		log.Log.Debugf("Sub field list %#v", subFields)
		log.Log.Debugf("Field list %#v", fields)
	}
}
