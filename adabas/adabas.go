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

package adabas

import (
	"bytes"
	"strconv"

	"github.com/tknie/adabas-go-api/adabas"
	"github.com/tknie/adabas-go-api/adatypes"
	"github.com/tknie/flynn/common"
	def "github.com/tknie/flynn/common"
)

type Adabas struct {
	def.CommonDatabase
	dbURL        string
	conn         *adabas.Connection
	dbTableNames []string
	user         string
	password     string
}

func init() {
	adatypes.Central.Log = def.Log
}

func New(id def.RegDbID, url string) (def.Database, error) {
	ada := &Adabas{def.CommonDatabase{RegDbID: id}, url,
		nil, nil, "", ""}
	return ada, nil
}

func (ada *Adabas) SetCredentials(user, password string) error {
	ada.user = user
	ada.password = password
	return nil
}

func (ada *Adabas) ID() def.RegDbID {
	return ada.RegDbID
}

func (ada *Adabas) URL() string {
	return ada.dbURL
}
func (ada *Adabas) Maps() ([]string, error) {
	if ada.dbTableNames == nil {
		err := ada.Ping()
		if err != nil {
			return nil, err
		}
	}
	return ada.dbTableNames, nil
}

func (ada *Adabas) Ping() error {
	c, err := ada.Open()
	if err != nil {
		return err
	}
	con := c.(*adabas.Connection)
	defer con.Close()
	listMaps, err := con.GetMaps()
	if err != nil {
		return err
	}
	ada.dbTableNames = listMaps
	return nil
}

func (ada *Adabas) Open() (any, error) {
	db, err := adabas.NewConnection(ada.URL())
	if err != nil {
		return nil, err
	}
	err = db.Open()
	if err != nil {
		return nil, err
	}
	ada.conn = db
	return db, err
}

func (ada *Adabas) Close() {
	if ada.conn != nil {
		ada.conn.Close()
		ada.conn = nil
	}
}

func (ada *Adabas) Insert(name string, insert *def.Entries) error {
	return def.NewError(65535)
}

func (ada *Adabas) Update(name string, insert *def.Entries) error {
	return def.NewError(65535)
}

func (ada *Adabas) Delete(name string, remove *def.Entries) error {
	con, err := ada.Open()
	if err != nil {
		return err
	}

	conn := con.(*adabas.Connection)
	req, err := conn.CreateMapDeleteRequest(name)
	if err != nil {
		return err
	}
	isns := make([]adatypes.Isn, 0)

	if len(remove.Fields) != 1 || remove.Fields[0] != "ISN" {
		return def.NewError(23444)
	}

	for i := 0; i < len(remove.Values); i++ {

		switch v := remove.Values[i][0].(type) {
		case int:
			isns = append(isns, adatypes.Isn(v))
		case int32:
			isns = append(isns, adatypes.Isn(v))
		case int64:
			isns = append(isns, adatypes.Isn(v))
		case uint:
			isns = append(isns, adatypes.Isn(v))
		case uint32:
			isns = append(isns, adatypes.Isn(v))
		case uint64:
			isns = append(isns, adatypes.Isn(v))
		case string:
			iv, err := strconv.ParseUint(v, 0, 10)
			if err != nil {
				return def.NewError(23445)
			}
			isns = append(isns, adatypes.Isn(iv))
		}
	}
	return req.DeleteList(isns)

}

func (ada *Adabas) GetTableColumn(tableName string) ([]string, error) {
	con, err := ada.Open()
	if err != nil {
		return nil, err
	}

	conn := con.(*adabas.Connection)
	return conn.GetMaps()
}

func (ada *Adabas) Query(search *def.Query, f def.ResultFunction) (*common.Result, error) {
	con, err := ada.Open()
	if err != nil {
		return nil, err
	}

	conn := con.(*adabas.Connection)
	var request *adabas.ReadRequest
	if search.DataStruct != nil {
		request, err = conn.CreateMapReadRequest(search.DataStruct)
		if err != nil {
			return nil, err
		}

	} else {
		request, err = conn.CreateMapReadRequest(search.TableName)
		if err != nil {
			return nil, err
		}

	}
	var buffer bytes.Buffer
	for _, f := range search.Fields {
		if buffer.Len() > 0 {
			buffer.WriteRune(',')
		}
		buffer.WriteString(f)
	}
	err = request.QueryFields(buffer.String())
	if err != nil {
		return nil, err
	}

	cursor, err := request.ReadPhysicalWithCursoring()
	if err != nil {
		return nil, err
	}
	result := &def.Result{}
	for cursor.HasNextRecord() {
		if search.DataStruct != nil {
			record, err := cursor.NextData()
			if err != nil {
				return nil, err
			}
			result.Data = record
			err = f(search, result)
			if err != nil {
				return nil, err
			}
		} else {
			record, err := cursor.NextRecord()
			if err != nil {
				return nil, err
			}
			result.Rows = make([]any, 0)
			for _, v := range record.Value {
				var vi interface{}
				switch v.Type().Type() {
				case adatypes.FieldTypeUnicode, adatypes.FieldTypeString:
					vi = v.String()
				default:
					vi = v.Value()
				}
				if common.IsDebugLevel() {
					def.Log.Debugf("%v %s %T", v, v.Type().Name(), v)
				}
				result.Rows = append(result.Rows, vi)
			}
			err = f(search, result)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func (ada *Adabas) CreateTable(string, any) error {
	return def.NewError(65535)
}

func (ada *Adabas) DeleteTable(string) error {
	return def.NewError(65535)
}

func (ada *Adabas) BatchSQL(batch string) error {
	return def.NewError(65535)
}
