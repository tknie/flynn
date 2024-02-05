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

package flynn

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tknie/flynn/common"
	def "github.com/tknie/flynn/common"
	"github.com/tknie/log"

	"github.com/stretchr/testify/assert"
)

const testCreationTable = "TESTTABLE"
const testCreationTableStruct = "TESTTABLESTRUCT"

type target struct {
	layer string
	url   string
}

type msg struct {
	index int
	msg   string
}

func (m *msg) values() []any {
	return []any{strconv.Itoa(m.index), m.msg}
}

var dataChan = make(chan *msg, 0)
var wgThread sync.WaitGroup
var doneChan = make(chan bool, 0)
var wgTest sync.WaitGroup
var atomicInt = int32(0)

const nrLoops = 1000

func getTestTargets(t *testing.T) (targets []*target) {
	url, err := mysqlTarget(t)
	if !assert.NoError(t, err) {
		return nil
	}
	targets = append(targets, &target{"mysql", url})
	url, err = postgresTarget(t)
	if !assert.NoError(t, err) {
		return nil
	}
	targets = append(targets, &target{"postgres", url})
	url, err = adabasTarget(t)
	if !assert.NoError(t, err) {
		return nil
	}
	targets = append(targets, &target{"adabas", url})
	return
}

func TestCreateStringArray(t *testing.T) {
	InitLog(t)

	columns := make([]*def.Column, 0)
	columns = append(columns, &def.Column{Name: "Id", DataType: def.Alpha, Length: 8})
	columns = append(columns, &def.Column{Name: "Name", DataType: def.Alpha, Length: 10})
	columns = append(columns, &def.Column{Name: "FirstName", DataType: def.Alpha, Length: 20})

	for _, target := range getTestTargets(t) {
		fmt.Println("Working at string creation on target " + target.layer)
		log.Log.Debugf("Working at string creation on target " + target.layer)

		id, err := Handle(target.layer, target.url)
		if !assert.NoError(t, err, "register fail using "+target.layer) {
			return
		}
		if target.layer == "adabas" {
			_, err := id.Delete(testCreationTable, &def.Entries{Fields: []string{"%Id"},
				Values: [][]any{{"TEST%"}}})
			if !assert.NoError(t, err, "DELETE") {
				return
			}
		}
		if target.layer != "adabas" {
			id.DeleteTable(testCreationTable)
			err = id.CreateTable(testCreationTable, columns)
			if !assert.NoError(t, err, "create fail using "+target.layer) {
				unregisterDatabase(t, id)
				return
			}
		}
		count := 1
		list := make([][]any, 0)
		list = append(list, []any{"TEST" + strconv.Itoa(count), "Eins", "Ernie"})
		for i := 1; i < nrLoops; i++ {
			count++
			list = append(list, []any{"TEST" + strconv.Itoa(count), strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i)})
		}
		count++
		list = append(list, []any{"TEST" + strconv.Itoa(count), "Letztes", "Anton"})
		err = id.Insert(testCreationTable, &def.Entries{Fields: []string{"Id", "Name", "FirstName"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		log.Log.Debugf("Delete TEST records")
		dr, err := id.Delete(testCreationTable, &def.Entries{Fields: []string{"%Id"},
			Values: [][]any{{"TEST%"}}})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		assert.Equal(t, int64(1001), dr)
		count++
		log.Log.Debugf("Delete of records done")
		tId := "TEST" + strconv.Itoa(count)
		list = append(list, []any{tId, "Tom", "Terminal"})
		err = id.Insert(testCreationTable, &def.Entries{Fields: []string{"Id", "Name", "FirstName"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		dr, err = id.Delete(testCreationTable, &def.Entries{Criteria: "Id='" + tId + "'"})
		if !assert.NoError(t, err, "delete fail using "+target.layer) {
			return
		}
		assert.Equal(t, int64(1), dr)
		if target.layer != "adabas" {
			deleteTable(t, id, testCreationTable, target.layer)
		}
		unregisterDatabase(t, id)
	}
}

func unregisterDatabase(t *testing.T, id def.RegDbID) {
	log.Log.Debugf("FreeHandler %s", id)
	err := id.FreeHandler()
	assert.NoError(t, err)
}

func deleteTable(t *testing.T, id def.RegDbID, name, layer string) {
	log.Log.Debugf("Delete table %s", name)
	err := id.DeleteTable(name)
	assert.NoError(t, err, "delete fail using "+layer)
}

func TestCreateStruct(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	for _, target := range getTestTargets(t) {
		log.Log.Debugf("Work on target %#v", target)
		err := createStruct(t, target)
		assert.NoError(t, err)
	}
}

func createStruct(t *testing.T, target *target) error {
	columns := struct {
		XY        uint64 `flynn:"ID::SERIAL"`
		Name      string
		FirstName string
		LastName  string
		Address   string `flynn:"Street"`
		Salary    uint64 `flynn:"Salary"`
		Bonus     int64
	}{XY: nrLoops + 10, Name: "Gellanger",
		FirstName: "Bob", Salary: 10000}
	log.Log.Debugf("Working on creating with target " + target.layer)
	if target.layer == "adabas" {
		return nil
	}
	id, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return err
	}
	defer unregisterDatabase(t, id)
	defer id.DeleteTable(testCreationTableStruct)

	log.Log.Debugf("Delete table: %s", testCreationTableStruct)
	err = id.DeleteTable(testCreationTableStruct)
	log.Log.Debugf("Delete table: %s returns with %v", testCreationTableStruct, err)
	err = id.CreateTable(testCreationTableStruct, columns)
	if !assert.NoError(t, err, "create fail using "+target.layer) {
		return err
	}
	x, err := id.CreateTableIfNotExists(testCreationTableStruct, columns)
	assert.NoError(t, err)
	assert.Equal(t, def.CreateExists, x)

	list := make([][]any, 0)
	list = append(list, []any{"Eins", "Ernie"})
	for i := 1; i < nrLoops; i++ {
		list = append(list, []any{strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i)})
	}
	list = append(list, []any{"Letztes", "Anton"})
	err = id.Insert(testCreationTableStruct, &def.Entries{Fields: []string{"name", "firstname"},
		Values: list})
	if !assert.NoError(t, err, "insert fail using "+target.layer) {
		return err
	}
	// Insert data (all fields)
	err = id.Insert(testCreationTableStruct, &def.Entries{Fields: []string{"*"},
		DataStruct: &columns})
	if !assert.NoError(t, err, "insert data struct fail using "+target.layer) {
		return err
	}
	log.Log.Debugf("Inserting into table: %s", testCreationTableStruct)
	err = id.Batch("SELECT NAME FROM " + testCreationTableStruct)
	assert.NoError(t, err, "select fail using "+target.layer)
	found := false
	err = id.BatchSelectFct(&common.Query{Search: "SELECT NAME FROM " + testCreationTableStruct + " WHERE NAME='Gellanger'"},
		func(search *def.Query, result *def.Result) error {
			assert.Equal(t, uint64(1), result.Counter)
			assert.Equal(t, "Gellanger", result.Rows[0].(string))
			found = true
			return nil
		})
	assert.NoError(t, err)
	assert.True(t, found, "on "+target.layer)
	err = id.Commit()
	assert.NoError(t, err)
	err = id.BatchSelectFct(&common.Query{Search: "SELECT COUNT(*) FROM " + testCreationTableStruct},
		func(search *def.Query, result *def.Result) error {
			count := uint64(0)
			switch c := result.Rows[0].(type) {
			case int64:
				count = uint64(c)
			case string:
				ct, err := strconv.ParseUint(c, 10, 0)
				assert.NoError(t, err)
				count = ct
			default:
				fmt.Printf("Unknown TYPE %T\n", result.Rows[0])
			}
			// fmt.Println("COUNTER", result.Counter)
			assert.Equal(t, uint64(1), result.Counter)
			if !assert.Equal(t, uint64(nrLoops+2), count) {
				log.Log.Infof("Error entries missing")
			}
			return nil
		})
	assert.NoError(t, err)

	placeHolder := "$1"
	if target.layer != "postgres" {
		placeHolder = " ? "
	}
	found = false
	err = id.BatchSelectFct(&common.Query{Search: "SELECT NAME FROM " + testCreationTableStruct + " WHERE NAME = " + placeHolder, Parameters: []any{"Gellanger"}},
		func(search *def.Query, result *def.Result) error {
			assert.Equal(t, uint64(1), result.Counter)
			assert.Equal(t, "Gellanger", result.Rows[0].(string))
			found = true
			return nil
		})
	assert.NoError(t, err, "on "+target.layer)
	assert.True(t, found, "on "+target.layer)

	err = id.Batch("TRUNCATE " + testCreationTableStruct)
	if !assert.NoError(t, err) {
		return err
	}
	err = initTheadTest(t, target.layer, target.url, insertThread)
	assert.NoError(t, err)
	log.Log.Debugf("Ended thread first test on target %s", target.layer)
	err = initTheadTest(t, target.layer, target.url, insertAtomarThread)
	assert.NoError(t, err)
	log.Log.Debugf("Ended thread last test on target %s", target.layer)
	return err
}

func initTheadTest(t *testing.T, layer, url string, f func(t *testing.T, layer, url string)) error {
	urlMaxConns := url
	if layer == "postgres" {
		urlMaxConns = url + "?pool_max_conns=100"
	}
	for i := 0; i < 10; i++ {
		log.Log.Debugf("Trigger thread %02d ....", i)
		go f(t, layer, urlMaxConns)
	}

	for i := 1; i < 100; i++ {
		fmt.Println("ADD-" + layer)
		wgTest.Add(1)
		messgage := "Kermit und Pigi " + strconv.Itoa(i)
		log.Log.Debugf("Put into channel " + messgage)
		dataChan <- &msg{i, messgage}
	}

	log.Log.Debugf("Waiting for insert wait group " + layer)
	fmt.Println("WAIT-" + layer)
	wgTest.Wait()
	fmt.Println("WENDED-" + layer)
	log.Log.Debugf("Closeing group")
	for i := 0; i < 10; i++ {
		doneChan <- true
	}
	log.Log.Debugf("Waiting for thread wait group")
	wgThread.Wait()
	atomicInt = 0
	log.Log.Debugf("Ready waiting for thread wait group %s", layer)
	//log.Log.Debugf("Deleting table: %s", testCreationTableStruct)
	//deleteTable(t, id, testCreationTableStruct, target.layer)
	return nil
}

func insertThread(t *testing.T, layer, url string) {
	nr := atomic.AddInt32(&atomicInt, 1)
	log.Log.Debugf("%02d: Start threads ....", nr)
	id, err := Handle(layer, url)
	if !assert.NoError(t, err, "register fail using "+layer) {
		log.Log.Fatal("Error registrer")
	}
	// fmt.Println("Start thread ....", nr)
	defer id.FreeHandler()
	defer log.Log.Debugf("Close thread %d", nr)
	wgThread.Add(1)
	defer wgThread.Done()
	for {
		log.Log.Debugf("%02d: Waiting for entry .... %s", nr, layer)
		select {
		case x := <-dataChan:
			log.Log.Debugf("%v-%02d: Received entry  ....%v -> %s", id, nr, x.msg, layer)
			err = id.Insert(testCreationTableStruct, &def.Entries{Fields: []string{"name", "firstname"},
				Values: [][]any{x.values()}})
			log.Log.Debugf("%v-%02d: insert returned  ....%v -> %s %v", id, nr, x.msg, layer, err)
			if !assert.NoError(t, err, "insert fail using "+layer) {
				fmt.Println("Error thread ....")
				log.Log.Debugf("%02d: Error storing  ....%v", nr, x.msg)
			} else {
				log.Log.Debugf("%d-%02d: Entry thread stored .... %s -> %v", id, nr, layer, x.msg)
			}
			fmt.Printf("DONEX-%d-%s", nr, layer)
			log.Log.Debugf("DONEX-%s -> %s", layer, x.msg)
			wgTest.Done()
		case <-doneChan:
			// fmt.Println("Ready thread ....", nr)
			log.Log.Debugf("%02d: exiting thread %s", nr, url)
			return
		}
	}
}

func insertAtomarThread(t *testing.T, layer, url string) {
	nr := atomic.AddInt32(&atomicInt, 1)
	log.Log.Debugf("%02d: Start thread ....", nr)
	// fmt.Println("Start thread ....", nr)
	wgThread.Add(1)
	defer wgThread.Done()
	insertRecordForThread(t, layer, url, nr)
}

func insertRecordForThread(t *testing.T, layer, url string, nr int32) {
	for {
		id, err := Handle(layer, url)
		if !assert.NoError(t, err, "register fail using "+layer) {
			log.Log.Fatal("Error registrer")
		}
		defer id.FreeHandler()
		log.Log.Debugf("%02d: Waiting for entry .... ", nr)
		select {
		case x := <-dataChan:
			log.Log.Debugf("%02d: Received entry  ....%v", nr, x.msg)
			err = id.Insert(testCreationTableStruct, &def.Entries{Fields: []string{"name", "firstname"},
				Values: [][]any{x.values()}})
			if !assert.NoError(t, err, "insert fail using "+layer) {
				fmt.Println("Error thread ....")
				log.Log.Debugf("%02d: Error storing  ....%v", nr, x.msg)
			} else {
				log.Log.Debugf("%02d: Entry ready ....", nr)
			}
			fmt.Println("DONEY-" + layer)
			wgTest.Done()
		case <-doneChan:
			// fmt.Println("Ready thread ....", nr)
			log.Log.Debugf("%02d: exiting thread %s", nr, url)
			return
		}
	}

}
