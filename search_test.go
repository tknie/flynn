package db

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	def "github.com/tknie/db/common"
)

func TestPgSearchRows(t *testing.T) {
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
		Fields: []string{"Title"}}
	counter := 0
	err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		if !assert.IsType(t, &sql.NullString{}, result.Rows[0]) {
			return fmt.Errorf("Nullstring expected")
		}
		ns := result.Rows[0].(*sql.NullString)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "5. Klasse", ns.String)
		case 10:
			assert.Equal(t, "Es ist Herbst.", ns.String)
		case 48:
			assert.Equal(t, "Vito", ns.String)
		default:
			assert.NotEqual(t, "blabla", ns.String)
		}

		return nil
	})
	assert.NoError(t, err)
}

type TestString struct {
	Title string
}

func TestPgSearchStruct(t *testing.T) {
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
	err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.IsType(t, &TestString{}, result.Data)
		td := result.Data.(*TestString)
		x := &td.Title
		if !assert.Equal(t, x, result.Rows[0]) {
			return fmt.Errorf("Error found")
		}
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
}

func TestPgSearchPtrStruct(t *testing.T) {
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
	err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.IsType(t, &TestString{}, result.Data)
		//rs := result.Rows[0].(*string)
		td := result.Data.(*TestString)
		x := &td.Title
		//fmt.Printf("Row=%s oldData=%v newDaa=%v oldStruct=%p newStruct=%p field is=%p must=%p\n",
		//	*rs, search.DataStruct, td, search.DataStruct, td, result.Rows[0], x)
		if !assert.Equal(t, x, result.Rows[0]) {
			return fmt.Errorf("Error found")
		}
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

func TestAdaSearchStruct(t *testing.T) {
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
	err = x.Query(q, func(search *def.Query, result *def.Result) error {
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

func TestAdaPtrSearchStruct(t *testing.T) {
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
	err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.NotNil(t, result.Data)
		if !assert.IsType(t, &Albums{}, result.Data) {
			return fmt.Errorf("Result type wrong")
		}
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

func TestMariaDBSearchRows(t *testing.T) {
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
		Fields: []string{"Title"}}
	counter := 0
	err = x.Query(q, func(search *def.Query, result *def.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		if !assert.IsType(t, &sql.NullString{}, result.Rows[0]) {
			return fmt.Errorf("Nullstring expected")
		}
		ns := result.Rows[0].(*sql.NullString)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "1.Hälfte Sommerferien 2019 sind vorbei", ns.String)
		case 10:
			assert.Equal(t, "Ferien 2017", ns.String)
		case 48:
			assert.Equal(t, "Zwischen den Jahren", ns.String)
		default:
			assert.NotEqual(t, "blabla", ns.String)
		}

		return nil
	})
	assert.NoError(t, err)
}
