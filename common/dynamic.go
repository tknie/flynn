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

	"github.com/tknie/log"
)

type typeInterface struct {
	DataType  interface{}
	RowNames  map[string][]string
	RowValues []any
}

func CreateInterface(i interface{}) *typeInterface {
	ri := reflect.TypeOf(i)
	if ri.Kind() == reflect.Ptr {
		ri = ri.Elem()
	}
	dynamic := &typeInterface{DataType: i, RowNames: make(map[string][]string)}
	generateFieldNames(ri, dynamic.RowNames, make([]string, 0))
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

func (dynamic *typeInterface) CreateQueryValues() (any, []any) {
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
		cv := rv.Field(fi)
		subRt := rt.Field(fi)
		if log.IsDebugLevel() {
			log.Log.Debugf("Field %d:%s %v canAddr: %v %s %s", fi, cv.Type().Name(), cv.CanInterface(), cv.CanAddr(), rt.Name(), subRt.Name)
		}
		if cv.Kind() == reflect.Struct {
			dynamic.createQueryValues(cv.Interface())
		} else {
			if log.IsDebugLevel() {
				log.Log.Debugf("FieldCV: %s %T %T %v %v indirect=%v\n",
					cv.Type().Name(), cv, cv.Interface(), cv.CanAddr(),
					cv.CanInterface(), reflect.Indirect(cv))
			}
			var ptr reflect.Value
			if cv.CanAddr() {
				log.Log.Debugf("Use Addr")
				ptr = cv.Addr()
			} else {
				log.Log.Debugf("New Addr pointer")
				ptr = reflect.New(cv.Type())
				ptr.Elem().Set(cv)
			}
			if log.IsDebugLevel() {
				log.Log.Debugf("FieldPTR: %T / %T / %v\n", ptr.Type().Name(), ptr.Interface(), ptr.Interface())
			}
			// x := ptr.Pointer()
			// xv := reflect.ValueOf(x)
			// fmt.Println("PTR Kind:", xv.Kind(), xv.Kind() == reflect.Pointer)
			// if xv.Kind() != reflect.Pointer {
			// 	log.Fatalf("FATAL ERROR not a pointer ..... exiting FieldPTR: %T / %T / %v\n", ptr.Type().Name(), x, x)
			// }
			//rf := reflect.NewAt(cv.Type(), unsafe.Pointer(ptr.Pointer())) // .Elem()
			//dynamic.RowValues = append(dynamic.RowValues, rf.Interface())
			dynamic.RowValues = append(dynamic.RowValues, ptr.Interface())
		}
	}
	if log.IsDebugLevel() {
		log.Log.Debugf("Len row values: %d", len(dynamic.RowValues))
	}
}

// generateFieldNames examine all structure-tags in the given structure and build up
// field names map pointing to corresponding path with names of structures
func generateFieldNames(ri reflect.Type, f map[string][]string, fields []string) {
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

		// copy of subfields
		subFields := make([]string, len(fields))
		copy(subFields, fields)
		subFields = append(subFields, fieldName)
		f[fieldName] = subFields

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
	}
}
