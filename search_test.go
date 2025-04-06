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
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

var logRus = logrus.StandardLogger()
var once = new(sync.Once)

func InitLog(t *testing.T) {
	once.Do(startLog)
	log.Log.Debugf("TEST: %s", t.Name())
}

func startLog() {
	fmt.Println("Init logging")
	fileName := "db.trace.log"
	level := os.Getenv("ENABLE_DB_DEBUG")
	logLevel := logrus.WarnLevel
	switch level {
	case "debug", "1":
		log.SetDebugLevel(true)
		logLevel = logrus.DebugLevel
	case "info", "2":
		log.SetDebugLevel(false)
		logLevel = logrus.InfoLevel
	default:
	}
	logRus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05",
	})
	logRus.SetLevel(logLevel)
	p := os.Getenv("LOGPATH")
	if p == "" {
		p = os.TempDir()
	}
	f, err := os.OpenFile(p+"/"+fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening log:", err)
		return
	}
	logRus.SetOutput(f)
	logRus.Infof("Init logrus")
	log.Log = logRus
	fmt.Println("Logging running")
}

func TestSearchQuery(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	fmt.Println("Register postgres: " + pg)
	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	tables, err := x.Tables()
	if !assert.NoError(t, err) {
		return
	}
	fmt.Println("TABLES:", tables)
	columns, err := x.GetTableColumn("Albums")
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, columns, 11)
	finalCheck(t, 1)
}

func TestSearchPgRows(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title", "created"},
		Order:  []string{"Title:ASC"},
	}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		fmt.Println("RESULT:", result.Rows)
		ns := result.Rows[0].(string)
		ts := result.Rows[1].(time.Time)
		ti := result.GetRowValueByName("title")
		assert.Equal(t, ns, ti)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "1.Hälfte Sommerferien 2019 sind vorbei", ns)
			assert.Equal(t, "1.Hälfte Sommerferien 2019 sind vorbei", ti)
			assert.Equal(t, "2023-03-15 14:54:51.305585 +0000 UTC", ts.String())
		case 10:
			assert.Equal(t, "Fasching 2019", ns)
			assert.Equal(t, "2023-03-15 14:54:51.849488 +0000 UTC", ts.String())
		case 48:
			assert.Equal(t, "Weihnachtszeit 2019", ns)
			assert.Equal(t, "2023-03-15 14:54:53.617203 +0000 UTC", ts.String())
		default:
			assert.NotEqual(t, "blabla", ns)
		}

		return nil
	})
	assert.NoError(t, err)
	finalCheck(t, 1)
}

func TestQueryPgFunctions(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Pictures",
		Search: "",
		Fields: []string{"length(Media)", "checksumpicture"},
	}
	counter := 0
	length := uint64(0)
	lenList := make([]uint64, 0)
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		log.Log.Debugf("Query row function called...")
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		l := uint64(result.Rows[0].(int32))
		length += l
		if l == 158005189 {
			fmt.Println(result.Rows[1].(string))
		}
		lenList = append(lenList, l)
		log.Log.Debugf("RESULT: %d -> %s", l, result.Rows[1].(string))
		counter++

		return nil
	})
	sort.Slice(lenList, func(i, j int) bool {
		return lenList[i] < lenList[j]
	})
	assert.Equal(t, 2222, counter)
	assert.Equal(t, uint64(0x10023369e), length)
	assert.NoError(t, err)
	for i := len(lenList) - 3; i < len(lenList); i++ {
		fmt.Println(lenList[i])
	}
	finalCheck(t, 1)
}

func TestSearchPgCriteriaRows(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search: "id=1",
		Fields: []string{"Title", "created"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		fmt.Println("RESULT:", result.Rows)
		ns := result.Rows[0].(string)
		ts := result.Rows[1].(time.Time)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "5. Klasse", ns)
			assert.Equal(t, "2023-02-24 20:48:09.881439 +0000 UTC", ts.String())
		default:
			assert.Fail(t, "Should not come here")
		}

		return nil
	})
	assert.NoError(t, err)
	finalCheck(t, 1)
}

type TestString struct {
	Title     string
	Published time.Time
	Ignore    string `flynn:":ignore"`
}

type TestDeepString struct {
	Title     string
	Published time.Time
	Sub       struct {
		Directory string
		Thumbnail string
	}
	Ignore string `flynn:":ignore"`
}

type SubPtr struct {
	Directory string
	Thumbnail string
}

type TestDeepPtrString struct {
	Title     string
	Published time.Time
	SubPtr    *SubPtr
	Ignore    string `flynn:":ignore"`
}

func TestSearchPgStruct(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search:     "",
		DataStruct: TestString{Title: "blabla"},
		Fields:     []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.IsType(t, &TestString{}, result.Data)
		td := result.Data.(*TestString)
		// x := &td.Title
		// if !assert.Equal(t, x, result.Rows[0]) {
		// 	return fmt.Errorf("Error found")
		// }
		counter++
		switch counter {
		case 1:
			assert.Equal(t, td.Title, "5. Klasse")
		case 10:
			assert.Equal(t, td.Title, "Es ist Herbst.")
		case 48:
			assert.Equal(t, td.Title, "Vito")
		default:
			assert.NotEqual(t, td.Title, "blabla")
		}
		return nil
	})
	assert.NoError(t, err)
	finalCheck(t, 1)
}

func TestSearchPgPtrStruct(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search:     "",
		DataStruct: &TestString{Title: "blabla"},
		Fields:     []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.IsType(t, &TestString{}, result.Data)
		//rs := result.Rows[0].(*string)
		td := result.Data.(*TestString)
		//x := &td.Title
		//fmt.Printf("Row=%s oldData=%v newDaa=%v oldStruct=%p newStruct=%p field is=%p must=%p\n",
		//	*rs, search.DataStruct, td, search.DataStruct, td, result.Rows[0], x)
		// if !assert.Equal(t, x, result.Rows[0]) {
		// 	return fmt.Errorf("Error found")
		// }
		counter++
		switch counter {
		case 1:
			assert.Equal(t, td.Title, "5. Klasse")
		case 10:
			assert.Equal(t, td.Title, "Es ist Herbst.")
		case 48:
			assert.Equal(t, td.Title, "Vito")
		default:
			assert.NotEqual(t, td.Title, "blabla")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 50, counter)
	finalCheck(t, 1)
}

func TestSearchPgPtrStructDeep(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search:     "",
		DataStruct: &TestDeepString{Title: "blabla"},
		Fields:     []string{"Title", "Directory"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.IsType(t, &TestDeepString{}, result.Data)
		td := result.Data.(*TestDeepString)
		fmt.Printf("Deep %#v\n", td)
		counter++
		assert.Equal(t, td.Sub.Thumbnail, "")
		assert.NotEmpty(t, td.Sub.Directory)
		switch counter {
		case 1:
			assert.Equal(t, td.Title, "5. Klasse")
		case 10:
			assert.Equal(t, td.Title, "Es ist Herbst.")
		case 48:
			assert.Equal(t, td.Title, "Vito")
		default:
			assert.NotEqual(t, td.Title, "blabla")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 50, counter)
	finalCheck(t, 1)
}

func TestSearchPgPtrStructAll(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search:     "",
		DataStruct: &TestDeepString{Title: "blabla"},
		Fields:     []string{"*"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.IsType(t, &TestDeepString{}, result.Data)
		td := result.Data.(*TestDeepString)
		// fmt.Printf("%d: Deep %#v\n", counter, td)
		counter++
		assert.NotEmpty(t, td.Sub.Directory)
		switch counter {
		case 1:
			assert.Equal(t, td.Title, "5. Klasse")
			assert.Equal(t, td.Sub.Thumbnail, "3C83A22329A7CDCDAAD39D1B5A041E49")
		case 10:
			assert.Equal(t, td.Title, "Es ist Herbst.")
			assert.Equal(t, td.Sub.Thumbnail, "37591E6BE9EE899A92943D6862BE9C79")
		case 48:
			assert.Equal(t, td.Title, "Vito")
			assert.Equal(t, td.Sub.Thumbnail, "937A61D44BEE8AF355FA2BA28A44076F")
		default:
			assert.NotEqual(t, td.Title, "blabla")
			assert.NotEmpty(t, td.Sub.Thumbnail)
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 50, counter)
	finalCheck(t, 1)
}

func TestSearchPgPtrPtrStructAll(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search:     "",
		DataStruct: &TestDeepPtrString{Title: "blabla"},
		Fields:     []string{"*"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.IsType(t, &TestDeepPtrString{}, result.Data)
		td := result.Data.(*TestDeepPtrString)
		// fmt.Printf("%d: Deep %#v\n", counter, td)
		counter++
		if assert.NotNil(t, td.SubPtr) {
			assert.NotEmpty(t, td.SubPtr.Directory)
			switch counter {
			case 1:
				assert.Equal(t, td.Title, "5. Klasse")
				assert.Equal(t, td.SubPtr.Thumbnail, "3C83A22329A7CDCDAAD39D1B5A041E49")
			case 10:
				assert.Equal(t, td.Title, "Es ist Herbst.")
				assert.Equal(t, td.SubPtr.Thumbnail, "37591E6BE9EE899A92943D6862BE9C79")
			case 48:
				assert.Equal(t, td.Title, "Vito")
				assert.Equal(t, td.SubPtr.Thumbnail, "937A61D44BEE8AF355FA2BA28A44076F")
			default:
				assert.NotEqual(t, td.Title, "blabla")
				assert.NotEmpty(t, td.SubPtr.Thumbnail)
			}
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 50, counter)
	finalCheck(t, 1)
}

func TestSearchAdaStruct(t *testing.T) {
	InitLog(t)
	ada, err := adabasTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("adabas", ada)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Nil(t, result.Data)
		counter++
		title := result.Rows[0].(string)
		switch counter {
		case 1:
			assert.Equal(t, "5. Klasse", title)
		case 10:
			assert.Equal(t, "Es ist Herbst.", result.Rows[0])
		case 48:
			assert.Equal(t, "Vito", result.Rows[0])
		default:
			assert.NotEqual(t, "blabla", result.Rows[0])
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 50, counter)
	finalCheck(t, 1)
}

type Albums struct {
	Title string
}

func TestSearchAdaPtrStruct(t *testing.T) {
	InitLog(t)
	ada, err := adabasTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("adabas", ada)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search:     "",
		DataStruct: &Albums{Title: "blabla"},
		Fields:     []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.NotNil(t, result.Data)
		counter++
		title := result.Data.(*Albums).Title
		switch counter {
		case 1:
			assert.Equal(t, "5. Klasse", title)
		case 10:
			assert.Equal(t, "Es ist Herbst.", title)
		case 48:
			assert.Equal(t, "Vito", title)
		default:
			assert.NotEqual(t, "blabla", title)
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 50, counter)
	finalCheck(t, 1)
}

func TestSearchMariaDBRows(t *testing.T) {
	InitLog(t)

	db, err := mysqlTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("mysql", db)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title", "created"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		ns := result.Rows[0].(string)
		ts := result.Rows[1].(time.Time)

		counter++
		switch counter {
		case 1:
			assert.Equal(t, "5. Klasse", ns)
			assert.Equal(t, "2022-10-27 15:15:10 +0000 UTC", ts.String())
		case 2:
			assert.Equal(t, "Spontane Ausflüge", ns)
		case 10:
			assert.Equal(t, "Es ist Herbst.", ns)
		case 48:
			assert.Equal(t, "Vito", ns)
		default:
			assert.NotEqual(t, "blabla", ns)
		}

		return nil
	})
	assert.NoError(t, err)
	finalCheck(t, 1)
}

func TestSearchPgRowsOrdered(t *testing.T) {
	InitLog(t)
	pgUrl, err := postgresUserTarget(t)
	if !assert.NoError(t, err) {
		return
	}
	pgUrl += "?pool_max_conns=10"
	x, err := Handle("postgres", pgUrl)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	pwd := os.Getenv("POSTGRES_PWD")
	if !assert.NotEmpty(t, pwd) {
		return
	}
	x.SetCredentials("admin", pwd)

	q := &common.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title", "published"},
		Order:  []string{"published:DESC"},
	}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		ns := result.Rows[0].(string)
		ts := result.Rows[1].(time.Time)
		fmt.Println("RESULT:", ns, ts)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "Winter 2023", ns)
			assert.Equal(t, "2023-02-11 19:32:15 +0000 UTC", ts.String())
		case 10:
			assert.Equal(t, "Weihnachtsgruß2021", ns)
			assert.Equal(t, "2021-12-19 09:45:07 +0000 UTC", ts.String())
		case 48:
			assert.Equal(t, "Die Familienausflüge", ns)
			assert.Equal(t, "2016-05-25 08:07:48 +0000 UTC", ts.String())
		default:
			assert.NotEqual(t, "blabla", ns)
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestSearchMySQLRowsOrdered(t *testing.T) {
	InitLog(t)
	mysql, err := mysqlUserTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Handle("mysql", mysql)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	mysqlPassword := os.Getenv("MYSQL_PWD")
	x.SetCredentials("admin", mysqlPassword)

	q := &common.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title", "created"},
		Order:  []string{"created:DESC"},
	}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		ns := (result.Rows[0].(string))
		ts := result.Rows[1].(time.Time)
		fmt.Println("RESULT:", ns, ts)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "Sommerferien 2022", ns)
			assert.Equal(t, "2022-10-27 15:15:22 +0000 UTC", ts.String())
		case 10:
			assert.Equal(t, "Neues aus Seeheim...", ns)
			assert.Equal(t, "2022-10-27 15:15:20 +0000 UTC", ts.String())
		case 48:
			assert.Equal(t, "Spontane Ausflüge", ns)
			assert.Equal(t, "2022-10-27 15:15:10 +0000 UTC", ts.String())
		default:
			assert.NotEqual(t, "blabla", ns)
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestSearchPgRowsDistinct(t *testing.T) {
	InitLog(t)
	pgInstance, passwd, err := postgresTargetInstance(t)
	if !assert.NoError(t, err) {
		return
	}

	log.Log.Debugf("Postgres target registered")

	x, err := Handler(pgInstance, passwd)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Pictures",
		Search:     "",
		Descriptor: true,
		Limit:      "22",
		Fields:     []string{"directory"},
		Order:      []string{"directory:ASC"},
	}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 1)
		ns := result.Rows[0].(string)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "1.ferienhaelfte2019", ns)
		case 10:
			assert.Equal(t, "ferien2017", ns)
		case 22:
			assert.Equal(t, "Juni 2021", ns)
		case 23:
			assert.Fail(t, "Limit exceed")
		default:
			assert.NotEqual(t, "blabla", ns)
		}

		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 22, counter)
}

func TestMultipleOpenClose(t *testing.T) {
	InitLog(t)

	pgInstance, passwd, err := postgresTargetInstance(t)
	if !assert.NoError(t, err) {
		return
	}

	log.Log.Debugf("Postgres target registered")

	type pic struct {
		Title        string
		ExifOrigTime time.Time
	}
	idx := make([]common.RegDbID, 10)
	for i := 0; i < 10; i++ {
		idx[i], err = Handler(pgInstance, passwd)
		if !assert.NoError(t, err) {
			return
		}
		fmt.Println("GOT", idx[i])
		p := &pic{}
		q := &common.Query{TableName: "Pictures",
			Search:     "checksumpicture='02E88E36FF888D0344B633B329AE8C5E'",
			DataStruct: p,
			Fields:     []string{"title", "exiforigtime"},
		}
		counter := 0
		_, err = idx[i].Query(q, func(search *common.Query, result *common.Result) error {
			assert.Len(t, result.Fields, 2)
			p := result.Data.(*pic)
			fmt.Println(p.Title, p.ExifOrigTime)
			counter++

			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, counter)
		defer idx[i].FreeHandler()
		defer idx[i].Close()
	}
}

type Employee struct {
	FirstName   string `flynn:"first_name"`
	Name        string `flynn:"last_name"`
	Department  string
	ignoreField string
	Birth       time.Time
}

func TestReadmeQuery(t *testing.T) {
	InitLog(t)

	employee := &Employee{}

	userURL := "postgres://admin:abc@bear:5432/bitgarten"
	userDbRef, _, err := common.NewReference(userURL)
	if !assert.NoError(t, err) {
		return
	}
	postgresPassword := os.Getenv("POSTGRES_PWD")
	x, err := Handler(userDbRef, postgresPassword)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Employees",
		DataStruct: employee,
		Search:     "id=23",
		Fields:     []string{"*"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		counter++
		log.Log.Debugf("Got resut data ... %d", counter)
		e := result.Data.(*Employee)
		fmt.Println(e.FirstName, " ", e.Name, " ", e.Birth)
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, counter)
}
