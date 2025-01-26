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
	"math"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tknie/flynn/common"
	"github.com/tknie/log"

	"github.com/stretchr/testify/assert"
)

const testCreationTable = "TESTTABLE"
const testCreationTableStruct = "TESTTABLESTRUCT"
const CreationAdaptTable = "TestCreateAdaptTable"

var testWg sync.WaitGroup

type target struct {
	layer string
	url   string
}

// type msg struct {
// 	index int
// 	msg   string
// }

// func (m *msg) values() []any {
// 	return []any{strconv.Itoa(m.index), m.msg}
// }

var dataChan = make(chan *testRecord)
var wgThread sync.WaitGroup
var doneChan = make(chan bool)
var wgTest sync.WaitGroup
var atomicInt = int32(0)

const nrLoops = 1000

type SubField struct {
	SubName string
	Number  int
}
type TestUser struct {
	Name  string `xml:"name,attr" yaml:"name"`
	Read  string `xml:"read,attr" yaml:"read"`
	Write string `xml:"write,attr" yaml:"write"`
}

type testRecord struct {
	ID         int
	Name       string
	FirstName  string
	LastName   string
	Address    string `flynn:"Street"`
	Salary     uint64 `flynn:"Salary"`
	Bonus      int64
	Sub        *SubField `flynn:":sub"`
	Permission *TestUser `flynn:":YAML"`
}

func (tr *testRecord) values(fields []string) []any {
	values := make([]any, 0)
	for _, n := range []string{"id", "name", "firstname",
		"lastname", "address", "salary", "bonus", "sub"} {
		if slices.Contains(fields, strings.ToLower(n)) {
			switch strings.ToLower(n) {
			case "id":
				values = append(values, tr.ID)
			case "name":
				values = append(values, tr.Name)
			case "firstname":
				values = append(values, tr.FirstName)
			case "lastname":
				values = append(values, tr.LastName)
			case "address":
				values = append(values, tr.Address)
			case "salary":
				values = append(values, tr.Salary)
			case "bonus":
				values = append(values, tr.Bonus)
			case "permission":
				values = append(values, tr.Permission)
			default:
				log.Log.Fatal("Appender for " + n + " not found")

			}
		}
	}
	if len(fields) != len(values) {
		debug.PrintStack()
		fmt.Println(fields)
		fmt.Println(values)
		log.Log.Fatal("Error values or fields len different")
	}
	return values
}

func (s *SubField) Data() []byte {
	if s == nil {
		return []byte("")
	}
	return []byte(fmt.Sprintf("%s:%03d", s.SubName, s.Number))
}

func (s *SubField) ParseData(sub []byte) error {
	sp := strings.Split(string(sub), ":")
	s.SubName = sp[0]
	n, err := strconv.Atoi(sp[1])
	if err != nil {
		return err
	}
	s.Number = n
	return nil
}

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

	columns := make([]*common.Column, 0)
	columns = append(columns, &common.Column{Name: "Id", DataType: common.Alpha, Length: 8})
	columns = append(columns, &common.Column{Name: "Name", DataType: common.Alpha, Length: 10})
	columns = append(columns, &common.Column{Name: "FirstName", DataType: common.Alpha, Length: 20})

	for _, target := range getTestTargets(t) {
		fmt.Println("Working at string creation on target " + target.layer)
		log.Log.Debugf("Working at string creation on target " + target.layer)

		id, err := Handle(target.layer, target.url)
		if !assert.NoError(t, err, "register fail using "+target.layer) {
			return
		}
		if target.layer == "adabas" {
			_, err := id.Delete(testCreationTable, &common.Entries{Fields: []string{"%Id"},
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
		_, err = id.Insert(testCreationTable, &common.Entries{Fields: []string{"Id", "Name", "FirstName"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		log.Log.Debugf("Delete TEST records")
		dr, err := id.Delete(testCreationTable, &common.Entries{Fields: []string{"%Id"},
			Values: [][]any{{"TEST%"}}})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		assert.Equal(t, int64(1001), dr)
		count++
		log.Log.Debugf("Delete of records done")
		tId := "TEST" + strconv.Itoa(count)
		list = append(list, []any{tId, "Tom", "Terminal"})
		_, err = id.Insert(testCreationTable, &common.Entries{Fields: []string{"Id", "Name", "FirstName"},
			Values: list})
		if !assert.NoError(t, err, "insert fail using "+target.layer) {
			return
		}
		dr, err = id.Delete(testCreationTable, &common.Entries{Criteria: "Id='" + tId + "'"})
		if !assert.NoError(t, err, "delete fail using "+target.layer) {
			return
		}
		assert.Equal(t, int64(1), dr)
		if target.layer != "adabas" {
			deleteTable(t, id, testCreationTable, target.layer)
		}
		unregisterDatabase(t, id)
	}
	finalCheck(t, 0)
}

func unregisterDatabase(t *testing.T, id common.RegDbID) {
	log.Log.Debugf("FreeHandler %s", id)
	err := id.FreeHandler()
	assert.NoError(t, err)
}

func deleteTable(t *testing.T, id common.RegDbID, name, layer string) {
	log.Log.Debugf("Delete table %s", name)
	err := id.DeleteTable(name)
	assert.NoError(t, err, "delete fail using "+layer)
}

func TestCreateStruct(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())
	targetsEnv := os.Getenv("TEST_FILTER_TARGETS")
	targets := strings.Split(targetsEnv, ",")
	for _, target := range getTestTargets(t) {
		if targetsEnv != "" && !slices.Contains(targets, target.layer) {
			continue
		}
		log.Log.Debugf("Work on target %#v", target)
		err := createStruct(t, target)
		assert.NoError(t, err)
		finalCheck(t, 0)
	}
}

func createStruct(t *testing.T, target *target) error {
	columns := struct {
		XY          uint64 `flynn:"ID::SERIAL"`
		Name        string
		FirstName   string
		LastName    string
		Address     string `flynn:"Street"`
		Salary      uint64 `flynn:"Salary"`
		Bonus       int64
		IgnoreField int64     `flynn:":ignore"`
		Sub         *SubField `flynn:":sub"`
		Permission  *TestUser `flynn:":YAML"`
	}{XY: nrLoops + 10, Name: "Gellanger",
		FirstName: "Bob", Salary: 10000,
		Sub:        &SubField{SubName: "AAAA", Number: 12},
		Permission: &TestUser{Name: "TESTUSER", Read: "READ INFO", Write: "WRITE INFO"}}

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
	assert.Equal(t, common.CreateExists, x)

	_, err = id.BatchSelect("SELECT Sub FROM " + testCreationTableStruct)
	if !assert.NoError(t, err) {
		return err
	}

	_, err = id.BatchSelect("SELECT IgnoreField FROM " + testCreationTableStruct)
	if !assert.Error(t, err) {
		return err
	}

	list := make([][]any, 0)
	list = append(list, []any{"Eins", "Ernie"})
	for i := 1; i < nrLoops; i++ {
		list = append(list, []any{strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i)})
	}
	list = append(list, []any{"Letztes", "Anton"})
	_, err = id.Insert(testCreationTableStruct,
		&common.Entries{Fields: []string{"name", "firstname"},
			Values: list})
	if !assert.NoError(t, err, "insert fail using "+target.layer) {
		return err
	}
	// Insert data (all fields)
	_, err = id.Insert(testCreationTableStruct, &common.Entries{Fields: []string{"*"},
		DataStruct: &columns, Values: [][]any{{&columns}}})
	if !assert.NoError(t, err, "insert data struct fail using "+target.layer) {
		return err
	}
	log.Log.Debugf("Inserting into table: %s", testCreationTableStruct)
	err = id.Batch("SELECT NAME, SUB FROM " + testCreationTableStruct)
	assert.NoError(t, err, "select fail using "+target.layer)
	found := false
	err = id.BatchSelectFct(&common.Query{Search: "SELECT NAME FROM " + testCreationTableStruct + " WHERE NAME='Gellanger'"},
		func(search *common.Query, result *common.Result) error {
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
		func(search *common.Query, result *common.Result) error {
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
	err = id.BatchSelectFct(&common.Query{Search: "SELECT NAME FROM " +
		testCreationTableStruct + " WHERE NAME = " +
		placeHolder, Parameters: []any{"Gellanger"}},
		func(search *common.Query, result *common.Result) error {
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
	err = initTheadTest(&threadTest{"insertThread", t, target.layer, target.url,
		insertThread, []string{"name", "firstname"}})
	assert.NoError(t, err)
	log.Log.Debugf("Ended thread first test on target %s", target.layer)
	err = initTheadTest(&threadTest{"insertAtomarThread", t, target.layer,
		target.url, insertAtomarThread, []string{"name", "firstname", "salary"}})
	assert.NoError(t, err)
	log.Log.Debugf("Ended thread second test on target %s", target.layer)
	err = initTheadTest(&threadTest{"insertStructThread1", t, target.layer,
		target.url, insertStructThread, []string{"name", "firstname", "bonus", "permission"}})
	assert.NoError(t, err)
	err = initTheadTest(&threadTest{"insertStructThread2", t, target.layer,
		target.url, insertStructThread,
		[]string{"name", "firstname", "lastname", "street", "sub"}})
	assert.NoError(t, err)
	log.Log.Debugf("Ended thread last test on target %s", target.layer)

	validateTestResult(t, target.layer, target.url)
	return err
}

type threadTest struct {
	name   string
	t      *testing.T
	layer  string
	url    string
	f      func(t *testing.T, layer, url string, fields []string)
	fields []string
}

func initTheadTest(test *threadTest) error {
	urlMaxConns := test.url
	if test.layer == "postgres" {
		urlMaxConns = test.url + "?pool_max_conns=100"
	}
	for i := 0; i < 5; i++ {
		log.Log.Debugf("Trigger thread %02d ....", i)
		go test.f(test.t, test.layer, urlMaxConns, test.fields)
	}

	for i := 1; i < 20; i++ {
		//fmt.Println("ADD-" + layer)
		wgTest.Add(1)
		messgage := "Kermit und Pigi " + strconv.Itoa(i)
		log.Log.Debugf("Put into channel " + messgage)
		dataChan <- &testRecord{Name: strconv.Itoa(i),
			LastName:   messgage,
			FirstName:  test.name,
			Bonus:      int64(math.Pow(-1, float64(i%2))*7000 - float64(i)),
			Salary:     uint64(80000 + 10*i),
			Sub:        &SubField{SubName: "Gonzo", Number: i},
			Permission: &TestUser{Name: "TTT", Read: "RRRR", Write: "WWWW"},
		}
	}

	log.Log.Debugf("Waiting for insert wait group " + test.layer)
	// fmt.Println("WAIT-" + layer)
	wgTest.Wait()
	// fmt.Println("WENDED-" + layer)
	log.Log.Debugf("Closeing group")
	for i := 0; i < 5; i++ {
		doneChan <- true
	}
	log.Log.Debugf("Waiting for thread wait group")
	wgThread.Wait()
	atomicInt = 0
	log.Log.Debugf("Ready waiting for thread wait group %s", test.layer)
	//log.Log.Debugf("Deleting table: %s", testCreationTableStruct)
	// deleteTable(t, id, testCreationTableStruct, target.layer)
	return nil
}

func insertThread(t *testing.T, layer, url string, fields []string) {
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
		log.Log.Debugf("%02d: Waiting for entry .... with handle %s", nr, id.String())
		select {
		case x := <-dataChan:
			log.Log.Debugf("%v-%02d: Received entry  ....%v -> %s", id, nr, x.LastName, layer)
			_, err = id.Insert(testCreationTableStruct,
				&common.Entries{Fields: fields,
					Values: [][]any{x.values(fields)}})
			log.Log.Debugf("%v-%02d: insert returned  ....%v -> %s %v", id, nr, x.LastName, layer, err)
			if !assert.NoError(t, err, "insert fail using "+layer) {
				fmt.Println("Error thread ....")
				log.Log.Debugf("%02d: Error storing  ....%v", nr, x.LastName)
			} else {
				log.Log.Debugf("%d-%02d: Entry thread stored .... %s -> %v", id, nr, layer, x.LastName)
			}
			// fmt.Printf("DONEX-%d-%s", nr, layer)
			log.Log.Debugf("DONEX-%s -> %s", layer, x.LastName)
			wgTest.Done()
		case <-doneChan:
			// fmt.Println("Ready thread ....", nr)
			log.Log.Debugf("%02d: exiting thread %s", nr, url)
			return
		}
	}
}

func insertAtomarThread(t *testing.T, layer, url string, fields []string) {
	nr := atomic.AddInt32(&atomicInt, 1)
	log.Log.Debugf("%02d: Start thread ....", nr)
	// fmt.Println("Start thread ....", nr)
	wgThread.Add(1)
	defer wgThread.Done()
	insertRecordForThread(t, layer, url, nr, fields)
}

func insertRecordForThread(t *testing.T, layer, url string, nr int32, fields []string) {
	for {
		id, err := Handle(layer, url)
		if !assert.NoError(t, err, "register fail using "+layer) {
			log.Log.Fatal("Error registrer")
		}
		log.Log.Debugf("%02d: Waiting for entry .... with handle %s", nr, id.String())
		select {
		case x := <-dataChan:
			log.Log.Debugf("%02d: Received entry  ....%v", nr, x.LastName)
			_, err = id.Insert(testCreationTableStruct,
				&common.Entries{Fields: fields,
					Values: [][]any{x.values(fields)}})
			if !assert.NoError(t, err, "insert fail using "+layer) {
				fmt.Println("Error thread ....")
				log.Log.Debugf("%02d: Error storing  ....%v", nr, x.LastName)
			} else {
				log.Log.Debugf("%02d: Entry ready ....", nr)
			}
			// fmt.Println("DONEY-" + layer)
			wgTest.Done()
		case <-doneChan:
			// fmt.Println("Ready thread ....", nr)
			log.Log.Debugf("%02d: exiting thread %s", nr, url)
			id.FreeHandler()
			return
		}
		id.FreeHandler()
	}

}

func insertStructThread(t *testing.T, layer, url string, fields []string) {
	nr := atomic.AddInt32(&atomicInt, 1)
	log.Log.Debugf("%02d: Start thread ....", nr)
	// fmt.Println("Start thread ....", nr)
	wgThread.Add(1)
	defer wgThread.Done()
	insertStructForThread(t, layer, url, nr, fields)
}

func insertStructForThread(t *testing.T, layer, url string, nr int32, fields []string) {
	log.Log.Debugf("%02d: starting thread %s with fields %s", nr, url, fields)
	for {
		id, err := Handle(layer, url)
		if !assert.NoError(t, err, "register fail using "+layer) {
			log.Log.Fatal("Error registrer")
		}
		log.Log.Debugf("%02d: Waiting for entry .... with handle %s", nr, id.String())
		select {
		case x := <-dataChan:
			log.Log.Debugf("%02d/%s: Received entry  ....%v - %s", nr, id.String(), x.LastName, x.FirstName)
			_, err = id.Insert(testCreationTableStruct,
				&common.Entries{Fields: fields,
					DataStruct: x,
					Values:     [][]any{{x}}})
			if !assert.NoError(t, err, "insert fail using "+layer) {
				fmt.Println("Error thread ....")
				log.Log.Debugf("%02d/%s: Error storing  ....%v - %s", nr, id.String(), x.LastName, x.FirstName)
			} else {
				log.Log.Debugf("%02d/%s: Entry ready .... %v - %s", nr, id.String(), x.LastName, x.FirstName)
			}
			wgTest.Done()
		case <-doneChan:
			log.Log.Debugf("%02d/%s: exiting thread %s", nr, id.String(), url)
			id.FreeHandler()
			return
		}
		id.FreeHandler()
	}

}

func validateTestResult(t *testing.T, layer, url string) {
	fmt.Println("Sleeping one minute")
	time.Sleep(10 * time.Second)
	log.Log.Debugf("Validating test results for %s on %s", layer, url)
	id, err := Handle(layer, url)
	if !assert.NoError(t, err, "register fail using "+layer) {
		log.Log.Fatal("Error registrer")
	}
	defer id.FreeHandler()

	counter := 0
	permissionCounter := 0
	err = id.BatchSelectFct(&common.Query{DataStruct: &testRecord{},
		Search: "SELECT * FROM " + testCreationTableStruct + " WHERE name='9'"},
		func(search *common.Query, result *common.Result) error {
			record := result.Data.(*testRecord)
			fmt.Printf("-> %#v\n", record)
			fmt.Printf("   %#v\n", record.Permission)
			if record.Permission.Name == "TTT" {
				permissionCounter++
			}
			counter++
			return nil
		})
	assert.NoError(t, err)
	assert.Equal(t, 4, counter)
	assert.Equal(t, 1, permissionCounter)
}

func finalCheck(t *testing.T, expected int) {
	if len(common.Databases) != expected {
		fmt.Println("More databases active as expected")
		for _, d := range common.Databases {
			fmt.Println(d.ID().URL())

		}
	}
	assert.True(t, len(common.Databases) <= expected)
}

func TestCreateAndAdapt(t *testing.T) {
	InitLog(t)

	columns := make([]*common.Column, 0)
	columns = append(columns, &common.Column{Name: "Id", DataType: common.Alpha, Length: 8})
	columns = append(columns, &common.Column{Name: "Name", DataType: common.Alpha, Length: 10})
	columns = append(columns, &common.Column{Name: "FirstName", DataType: common.Alpha, Length: 20})
	columns = append(columns, &common.Column{Name: "LastName", DataType: common.Alpha, Length: 20})

	for _, target := range getTestTargets(t) {
		testWg.Add(1)
		go testAdapt(t, target, columns)
	}
	testWg.Wait()
	finalCheck(t, 0)
}

func testAdapt(t *testing.T, target *target, columns []*common.Column) {
	defer testWg.Done()
	// Problem that ADABAS does not support adaption and mysql has a cachning problem
	if target.layer == "adabas" || target.layer == "mysql" {
		return
	}
	fmt.Println("Working at string creation on target " + target.layer)
	log.Log.Debugf("Working at string creation on target " + target.layer)

	id, err := Handle(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return
	}
	_, _ = id.Delete(CreationAdaptTable, &common.Entries{Fields: []string{"%Id"},
		Values: [][]any{{"TEST%"}}})

	err = id.DeleteTable(CreationAdaptTable)
	if !assert.NoError(t, err, "create delete failure "+target.layer) {
		unregisterDatabase(t, id)
		return
	}
	err = id.CreateTable(CreationAdaptTable, columns)
	if !assert.NoError(t, err, "create fail using "+target.layer) {
		unregisterDatabase(t, id)
		return
	}
	err = id.Commit()
	assert.NoError(t, err, "commit fail using "+target.layer)
	time.Sleep(60 * time.Second)
	/*
		c, err := id.GetTableColumn(CreationAdaptTable)
		if !assert.NoError(t, err, "create fail using "+target.layer) {
			unregisterDatabase(t, id)
			return
		}
		assert.Equal(t, []string{"id", "name", "firstname", "lastname"}, c)
	*/

	count := 1
	list := make([][]any, 0)
	list = append(list, []any{"TEST" + strconv.Itoa(count), "Eins", "Ernie", "Sesamstrasse"})
	for i := 1; i < 10; i++ {
		count++
		list = append(list, []any{"TEST" + strconv.Itoa(count), strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i), "Graf String " + strconv.Itoa(i)})
	}
	count++
	list = append(list, []any{"TEST" + strconv.Itoa(count), "Letztes", "Anton", "X"})
	_, err = id.Insert(CreationAdaptTable, &common.Entries{Fields: []string{"Id", "Name", "FirstName", "LastName"},
		Values: list})
	if !assert.NoError(t, err, "insert fail using "+target.layer) {
		return
	}
	newStructure := struct {
		Id        string
		Name      string
		FirstName string
		LastName  string
		Street    string
		Home      bool
		Counter   int
	}{"ABC", "Müller abc", "Otto", "Walkes", "Sonnenalle", false, 100}
	err = id.AdaptTable(CreationAdaptTable, &newStructure)
	if !assert.NoError(t, err, "Adapt fail using "+target.layer) {
		return
	}
	_, err = id.Insert(CreationAdaptTable, &common.Entries{Fields: []string{"*"},
		DataStruct: newStructure,
		Values:     [][]any{{newStructure}}})
	if !assert.NoError(t, err, "Insert db fail using "+target.layer) {
		return
	}
	list = make([][]any, 0)
	for i := 20; i < 30; i++ {
		count++
		list = append(list, []any{"TEST" + strconv.Itoa(count), strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i), "Graf String " + strconv.Itoa(i), "NEW", i%2 == 0, i})
	}
	count++
	list = append(list, []any{"TEST" + strconv.Itoa(count), "Letztes", "Anton", "X", "NEW", true, -1})
	_, err = id.Insert(CreationAdaptTable, &common.Entries{Fields: []string{"Id", "Name", "FirstName", "LastName", "Street", "Home", "Counter"},
		Values: list})
	if !assert.NoError(t, err, "insert fail using "+target.layer) {
		return
	}
	err = id.BatchSelectFct(&common.Query{DataStruct: struct{ Count int }{1},
		Search: "SELECT Counter FROM " + CreationAdaptTable + " WHERE name in ('9','20')"},
		func(search *common.Query, result *common.Result) error {
			record := result.Data.(*struct{ Count int })
			fmt.Printf("-> %#v\n", record.Count)
			assert.True(t, record.Count == 0 || record.Count == 20)
			return nil
		})
	if !assert.NoError(t, err, "insert fail using "+target.layer) {
		return
	}
	unregisterDatabase(t, id)
}
