package db

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/tknie/db/common"
	def "github.com/tknie/db/common"
)

var log = logrus.StandardLogger()

func init() {
	err := initLog("search.log")
	if err != nil {
		fmt.Println("ERROR : ", err)
		return
	}

}

func initLog(fileName string) (err error) {
	common.SetDebugLevel(true)
	log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02T15:04:05",
	})
	log.SetLevel(logrus.DebugLevel)
	p := os.Getenv("LOGPATH")
	if p == "" {
		p = os.TempDir()
	}
	f, err := os.OpenFile(p+"/"+fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	log.SetOutput(f)
	log.Debugf("Init logrus")
	common.Log = log

	return
}

func TestSearchQuery(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	columns, err := x.GetTableColumn("Albums")
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, columns, 10)
}

func TestSearchPgRows(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title", "created"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		fmt.Println("RESULT:", result.Rows)
		ns := *(result.Rows[0].(*string))
		ts := result.Rows[1].(*time.Time)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "5. Klasse", ns)
			assert.Equal(t, "2022-11-06 18:12:02.764303 +0000 UTC", ts.String())
		case 10:
			assert.Equal(t, "Es ist Herbst.", ns)
			assert.Equal(t, "2022-11-06 18:12:04.228919 +0000 UTC", ts.String())
		case 48:
			assert.Equal(t, "Vito", ns)
			assert.Equal(t, "2022-11-06 18:12:11.216235 +0000 UTC", ts.String())
		default:
			assert.NotEqual(t, "blabla", ns)
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestSearchPgCriteriaRows(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Albums",
		Search: "id=1",
		Fields: []string{"Title", "created"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		fmt.Println("RESULT:", result.Rows)
		ns := *(result.Rows[0].(*string))
		ts := result.Rows[1].(*time.Time)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "5. Klasse", ns)
			assert.Equal(t, "2022-11-06 18:12:02.764303 +0000 UTC", ts.String())
		default:
			assert.Fail(t, "Should not come here")
		}

		return nil
	})
	assert.NoError(t, err)
}

type TestString struct {
	Title string
}

func TestSearchPgStruct(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Albums",
		Search:     "",
		DataStruct: TestString{Title: "blabla"},
		Fields:     []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
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
			assert.Equal(t, td.Title, "Spontane Ausflüge")
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
}

func TestSearchPgPtrStruct(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Albums",
		Search:     "",
		DataStruct: &TestString{Title: "blabla"},
		Fields:     []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
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
	assert.Equal(t, 49, counter)
}

func TestSearchAdaStruct(t *testing.T) {
	ada, err := adabasTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("adabas", ada)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
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
	assert.Equal(t, 49, counter)
}

type Albums struct {
	Title string
}

func TestSearchAdaPtrStruct(t *testing.T) {
	ada, err := adabasTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("adabas", ada)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Albums",
		Search:     "",
		DataStruct: &Albums{Title: "blabla"},
		Fields:     []string{"Title"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
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
	assert.Equal(t, 49, counter)
}

func TestSearchMariaDBRows(t *testing.T) {

	db, err := mysqlTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("mysql", db)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &def.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title", "created"}}
	counter := 0
	_, err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		ns := result.Rows[0].(string)
		ts := result.Rows[1].(*time.Time)

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
}
