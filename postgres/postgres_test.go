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

package postgres

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
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

const testStructTable = "TestStructTableData"

type Picture struct {
	Index       int
	Description string
	Md5         string
	Mimetype    string
}

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

func PostgresTable(t *testing.T) string {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPort := os.Getenv("POSTGRES_PORT")
	postgresPassword := os.Getenv("POSTGRES_PWD")
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return ""
	}
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", postgresPassword, postgresHost, port, "bitgarten")
	return url
}

func TestPostgresInit(t *testing.T) {
	InitLog(t)

	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	m, err := pg.Maps()
	sort.Strings(m)
	assert.NoError(t, err)
	assert.Equal(t, []string{"albumpictures", "albums",
		"employees", "picturelocations",
		"pictures", "testcreateadapttable", "teststructtabledata",
		"teststructuser", "testtabledata"}, m)
}

func TestPostgresTableColumns(t *testing.T) {
	InitLog(t)

	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}

	m, err := pg.GetTableColumn("Albums")
	sort.Strings(m)
	assert.NoError(t, err)
	assert.Equal(t, []string{"albumkey", "albumtype",
		"created", "description", "directory", "id",
		"option", "published", "thumbnail", "title",
		"updated_at"}, m)

}

func TestPostgresBatchSelect(t *testing.T) {
	InitLog(t)

	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}

	result, err := pg.BatchSelect("select * from Albums where Title = 'Weihnachtsgruß2021'")

	assert.NoError(t, err)
	assert.Equal(t, [][]interface{}{{int32(42), sql.NullString{String: "", Valid: true}, sql.NullString{String: "C603FEF5DFF8AED9CFF3C182AB3F54EE", Valid: true},
		sql.NullString{String: "Weihnachtsgruß2021", Valid: true}, sql.NullString{String: "Weihnachtsgruß2021", Valid: true},
		sql.NullString{String: "", Valid: true}, sql.NullString{String: "", Valid: true}, sql.NullString{String: "B1543D579D15650CAE108E5657AC769C", Valid: true},
		time.Date(2021, time.December, 19, 9, 45, 7, 0, time.UTC), time.Date(2023, time.March, 15, 14, 54, 54, 46871000, time.UTC),
		time.Date(2023, time.March, 15, 14, 54, 54, 46871000, time.UTC)}}, result)

	result, err = pg.BatchSelect("select id,thumbnail,media,checksumpicture from Pictures where md5 = '6C377DCDBD4DF3B1B64FFF74C78A9A08'")

	assert.NoError(t, err)
	assert.Equal(t, int32(3), result[0][0])
	md5Hash := fmt.Sprintf("%X", md5.Sum(result[0][1].([]byte)))
	assert.Equal(t, "B885CA8F7EB9364557C0CA12283C7823", md5Hash)
	media := result[0][2].([]byte)
	md5Hash = fmt.Sprintf("%X", md5.Sum(media))
	assert.Equal(t, "B64F1DDF5683608579998E618545E497", md5Hash)
	assert.Equal(t, 1073835, len(media))
	assert.Equal(t, "B64F1DDF5683608579998E618545E497", result[0][3].(sql.NullString).String)

	result, err = pg.BatchSelect("select * from Pictures where md5 = '6C377DCDBD4DF3B1B64FFF74C78A9A08'")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 24, len(result[0]))

	result, err = pg.BatchSelect("select * from Pictures where md5 = 'E87BCC9195520D129D8F5A3E14CD5604'")

	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, 24, len(result[0]))

}

func TestPostgresBatchSelectFct(t *testing.T) {
	InitLog(t)

	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}

	count := 0
	err = pg.BatchSelectFct(&common.Query{Search: "select * from Pictures where md5 = '3C57AAD81E3121C48ED3FC752C1DC2BC'"},
		func(q *common.Query, result *common.Result) error {
			if count == 0 {
				for _, h := range result.Header {
					fmt.Printf("%s,\t", h.Name)
				}
				fmt.Println()
			}
			assert.Equal(t, 24, len(result.Header))
			assert.Equal(t, 24, len(result.Rows))
			if !assert.NotNil(t, result.Rows) {
				return fmt.Errorf("Rows empty")
			}
			for i := range result.Header {
				//fmt.Printf(" %T->", result[i])
				switch s := result.Rows[i].(type) {
				case sql.NullString:
					fmt.Print(s.String)
				case int32:
					fmt.Print(s)
				case string:
					fmt.Print(s)
				case []byte:
					fmt.Print("[", len(s), "]")
				default:
					fmt.Print(s)
				}
				fmt.Print(",\t")
			}
			fmt.Println()
			return nil
		})
	assert.NoError(t, err)

	count = 0
	err = pg.BatchSelectFct(&common.Query{Search: "select title,albumkey,directory,published from Albums where directory = 'Herbst2020'"},
		func(q *common.Query, result *common.Result) error {
			if count == 0 {
				fmt.Printf("%03d\t", result.Counter)
				for _, h := range result.Header {
					fmt.Printf("%s,\t", h.Name)
				}
				fmt.Println()
			}
			if !assert.NotNil(t, result.Rows) {
				return fmt.Errorf("Rows empty")
			}
			fmt.Printf("%03d\t", result.Counter)
			for _, r := range result.Rows {
				switch v := r.(type) {
				case sql.NullString:
					fmt.Printf("%s,\t", v.String)
				default:
					fmt.Printf("%v,\t", r)
				}
			}
			fmt.Println()
			count++
			return nil
		})
	assert.NoError(t, err)

	complexSelect := `with albumSelect as (select id,title,albumkey,directory,published from Albums where directory = 'Herbst2020')
	select index,description,md5,mimetype from albumpictures, albumSelect where albumid = albumSelect.id`
	err = pg.BatchSelectFct(&common.Query{Search: complexSelect},
		func(q *common.Query, result *common.Result) error {
			if !assert.NotNil(t, result.Rows) {
				return fmt.Errorf("Rows empty")
			}
			assert.Equal(t, "index", result.Header[0].Name)
			assert.Equal(t, "description", result.Header[1].Name)
			assert.Equal(t, "md5", result.Header[2].Name)
			assert.Equal(t, int64(result.Counter), result.Rows[0])
			switch result.Counter {
			case 1:
				assert.Equal(t, "Hallo Familie und Freunde, ...", result.Rows[1].(string))
			case 14:
				assert.Equal(t, "... und bleiben eher daheim. Mama hat Papa auch die ...", result.Rows[1].(string))
			case 25:
				assert.Equal(t, "Bis zum nächsten Mal. Tschhhuuueeesss", result.Rows[1].(string))
			default:
				assert.Equal(t, "image/jpeg", result.Rows[3].(string))
			}
			return nil
		})
	assert.NoError(t, err)

	complexSelect = `with albumSelect as (select id,title,albumkey,directory,published from Albums where directory = 'Herbst2020')
	select index,description,md5,mimetype from albumpictures, albumSelect where albumid = albumSelect.id`
	count = 0
	err = pg.BatchSelectFct(&common.Query{Search: complexSelect, DataStruct: &Picture{}},
		func(q *common.Query, result *common.Result) error {
			count++
			if !assert.NotNil(t, result.Data) {
				return fmt.Errorf("Data empty")
			}
			d := result.Data.(*Picture)
			assert.Equal(t, count, d.Index)
			assert.Equal(t, uint64(count), result.Counter)
			switch result.Counter {
			case 1:
				assert.Equal(t, "Hallo Familie und Freunde, ...", d.Description)
				assert.Equal(t, "CA1A0AF7675121560DC8B0F124146922", d.Md5)
			case 14:
				assert.Equal(t, "... und bleiben eher daheim. Mama hat Papa auch die ...", d.Description)
				assert.Equal(t, "97003231153D0C1C149B78F7B24356FF", d.Md5)
			case 25:
				assert.Equal(t, "Bis zum nächsten Mal. Tschhhuuueeesss", d.Description)
				assert.Equal(t, "5A41D2ACFBAF023A467D07FCADC10783", d.Md5)
			default:
				assert.Equal(t, "image/jpeg", d.Mimetype)
			}
			return nil
		})
	assert.Equal(t, 25, count)
	assert.NoError(t, err)
}

func TestPostgresInsertRows(t *testing.T) {
	InitLog(t)

	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	defer pg.FreeHandler()
	nameValue := time.Now().Format("20060102150405")
	vId1 := "i-" + nameValue + "-1"
	vId2 := "i-" + nameValue + "-2"
	list := [][]any{{vId1, "aaadasfdsnaflksdnf", 1}, {vId2, "dmfklsfgmskdlmgsmgls", 2}}
	input := &common.Entries{Fields: []string{"ID", "Name", "account"},
		Update:    []string{"ID"},
		Values:    list,
		Returning: []string{"ID", "account"}}
	returning, err := pg.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, 2, len(returning))
	for i, r := range returning {
		assert.Equal(t, 2, len(r))
		assert.Equal(t, list[i][0], r[0])
		l, err := strconv.ParseFloat(r[1].(string), 64)
		assert.NoError(t, err)
		assert.Equal(t, float64(list[i][2].(int)), l)
	}
}

type TestInsertData struct {
	Name    string
	ID      string
	Account float64
}

func TestPostgresInsertStruct(t *testing.T) {
	InitLog(t)

	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	defer pg.FreeHandler()
	nameValue := time.Now().Format("20060102150405")
	vId1 := "s-" + nameValue + "-1"
	vId2 := "s-" + nameValue + "-2"
	list := [][]any{{&TestInsertData{vId1, "aaadasfdsnaflksdnf", 1}},
		{&TestInsertData{vId2, "dmfklsfgmskdlmgsmgls", 2}}}
	input := &common.Entries{Fields: []string{"ID", "Name", "Account"},
		Update:     []string{"ID"},
		DataStruct: &TestInsertData{},
		Values:     list,
		Returning:  []string{"ID", "Account"}}
	returning, err := pg.Insert(testStructTable, input)
	if !assert.NoError(t, err) {
		return
	}
	if assert.Equal(t, 2, len(returning)) {
		for i, r := range returning {
			if x, ok := r[0].(*TestInsertData); ok {
				assert.Equal(t, list[i][0].(*TestInsertData).ID, x.ID)
				assert.Equal(t, "", x.Name)
				assert.Equal(t, list[i][0].(*TestInsertData).Account, x.Account)
				assert.Equal(t, &TestInsertData{ID: list[i][0].(*TestInsertData).ID, Account: list[i][0].(*TestInsertData).Account}, x)
			}
		}
	}
}

func TestPostgresReference(t *testing.T) {
	InitLog(t)

	ref, p, err := common.NewReference("postgres://admin:axx@localhost:5432/bitgarten?pool_max_conns=10")
	assert.NoError(t, err)

	db, err := NewInstance(199, ref, p)
	pg := db.(*PostGres)
	assert.NoError(t, err)
	assert.Equal(t, ref, pg.ConRef)
	// assert.Equal(t, "postgres://admin:<password>@localhost:5432/bitgarten?pool_max_conns=10", pg.dbURL)
	assert.Equal(t, "postgres://admin:axx@localhost:5432/bitgarten?pool_max_conns=10", pg.generateURL())

	ref, p, err = common.NewReference("postgres://admin:axx@localhost:5432/xxx")
	assert.NoError(t, err)

	db, err = NewInstance(199, ref, p)
	pg = db.(*PostGres)
	assert.NoError(t, err)
	assert.Equal(t, pg.ConRef, ref)
	assert.Equal(t, "postgres://admin:axx@localhost:5432/xxx", pg.generateURL())

}
