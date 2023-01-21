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

package adabas

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/tknie/adabas-go-api/adabas"
	"github.com/tknie/adabas-go-api/adatypes"
	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/common"
	def "github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

type Adabas struct {
	def.CommonDatabase
	dbURL        string
	conn         *adabas.Connection
	dbTableNames []string
	user         string
	password     string
}

func syncLog() {
	log.Log.Debugf("Try init debugging on adatypes")
	adatypes.Central.Log = log.Log
	adatypes.Central.SetDebugLevel(log.IsDebugLevel())
	adatypes.Central.Log.Debugf("Init debugging adatypes")
}

// New create new postgres reference instance
func New(id def.RegDbID, url string) (def.Database, error) {
	if adatypes.Central.Log != log.Log {
		syncLog()
	}
	ada := &Adabas{def.CommonDatabase{RegDbID: id}, url,
		nil, nil, "", ""}
	return ada, nil
}

// SetCredentials set credentials to connect to database
func (ada *Adabas) SetCredentials(user, password string) error {
	ada.user = user
	ada.password = password
	return nil
}

// ID current id used
func (ada *Adabas) ID() def.RegDbID {
	return ada.RegDbID
}

// URL current URL used
func (ada *Adabas) URL() string {
	return ada.dbURL
}

// Maps database maps, tables or views
func (ada *Adabas) Maps() ([]string, error) {
	if ada.dbTableNames == nil {
		err := ada.Ping()
		if err != nil {
			return nil, err
		}
	}
	return ada.dbTableNames, nil
}

// Ping create short test database connection
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

// Open open the database connection
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

// Close close the database connection
func (ada *Adabas) Close() {
	if ada.conn != nil {
		ada.conn.Close()
		ada.conn = nil
	}
}

// Insert insert record into table
func (ada *Adabas) Insert(name string, insert *def.Entries) error {
	con, err := ada.Open()
	if err != nil {
		return err
	}

	conn := con.(*adabas.Connection)
	req, err := conn.CreateMapStoreRequest(name)
	if err != nil {
		return err
	}
	log.Log.Debugf("Fields %#v\n", insert.Fields)
	err = req.StoreFields(insert.Fields)
	if err != nil {
		return err
	}
	for _, v := range insert.Values {
		record, rerr := req.CreateRecord()
		if rerr != nil {
			return rerr
		}
		for i, rv := range v {
			log.Log.Debugf("%d. %s %v\n", i, insert.Fields[i], rv)
			err = record.SetValue(insert.Fields[i], rv)
			if err != nil {
				return err
			}
		}
		log.Log.Debugf("Values %#v\n", v)
		err = req.Store(record)
		if err != nil {
			log.Log.Debugf("Error %v\n", err)
			return err
		}
		err = req.EndTransaction()
		if err != nil {
			log.Log.Debugf("ET Error %v\n", err)
			return err
		}
	}
	err = req.EndTransaction()

	return err
}

// Update update record in table
func (ada *Adabas) Update(name string, insert *def.Entries) error {
	return errorrepo.NewError("DB065535")
}

// Delete Delete database records
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

	log.Log.Debugf("Delete started")

	if len(remove.Fields) != 1 || remove.Fields[0] != "ISN" {
		log.Log.Debugf("Delete search call")
		queryReq, err := conn.CreateMapReadRequest(name)
		if err != nil {
			return err
		}
		log.Log.Debugf("SEARCH fields")
		err = queryReq.QueryFields("")
		if err != nil {
			log.Log.Debugf("Error SEARCH fields %v", err)
			return err
		}
		search := createSearch(remove)
		log.Log.Debugf("SEARCH %s", search)

		queryReq.Limit = 0
		result, err := queryReq.ReadLogicalWith(search)
		log.Log.Debugf("Search done")
		if err != nil {
			return err
		}
		for _, v := range result.Values {
			isns = append(isns, v.Isn)
			log.Log.Debugf("Add to delete list:", v.Isn)
		}
	} else {

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
					return errorrepo.NewError("DB23445")
				}
				isns = append(isns, adatypes.Isn(iv))
			}
		}
	}
	log.Log.Debugf("Start deleting %d ISNs/records\n", len(isns))
	err = req.DeleteList(isns)
	if err != nil {
		return err
	}
	log.Log.Debugf("Commit deleting %d ISNs/records\n", len(isns))
	err = req.EndTransaction()
	if err != nil {
		log.Log.Debugf("Error commit deleting ISNs/records: %v\n", err)
		return err
	}
	log.Log.Debugf("Done deleting ISNs/records\n")
	return nil
}

func createSearch(remove *def.Entries) string {
	var buffer bytes.Buffer
	for i, f := range remove.Fields {
		if f[0] == '%' {
			val := remove.Values[0][i].(string)
			if strings.HasSuffix(val, "%") {
				val = val[:len(val)-1]
				val = "['" + val + "'0x00:'" + val + "'0xff]"
			} else {
				val = "[" + val + "]"
			}
			buffer.WriteString(f[1:] + "=" + val)
		} else {
			buffer.WriteString(f + "=" + remove.Values[0][i].(string))
		}
	}
	return buffer.String()
}

// GetTableColumn get table columne names
func (ada *Adabas) GetTableColumn(tableName string) ([]string, error) {
	con, err := ada.Open()
	if err != nil {
		return nil, err
	}

	conn := con.(*adabas.Connection)
	return conn.GetMaps()
}

// Query query database records with search or SELECT
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
				if log.IsDebugLevel() {
					log.Log.Debugf("%v %s %T", v, v.Type().Name(), v)
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

// CreateTable create a new table
func (ada *Adabas) CreateTable(string, any) error {
	return errorrepo.NewError("DB065535")
}

// DeleteTable delete a table
func (ada *Adabas) DeleteTable(string) error {
	return errorrepo.NewError("DB065535")
}

// BatchSQL batch SQL query in table
func (ada *Adabas) BatchSQL(batch string) error {
	return errorrepo.NewError("DB065535")
}
