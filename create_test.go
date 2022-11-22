package db

import (
	"fmt"
	"strconv"
	"testing"

	def "github.com/tknie/db/common"

	"github.com/stretchr/testify/assert"
)

type target struct {
	layer string
	url   string
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
	// url, err = adabasTarget(t)
	// if !assert.NoError(t, err) {
	// 	return nil
	// }
	// targets = append(targets, &target{"adabas", url})
	return
}

func TestCreate(t *testing.T) {
	columns := make([]*def.Column, 0)
	columns = append(columns, &def.Column{Name: "Name", DataType: def.Alpha, Length: 10})
	columns = append(columns, &def.Column{Name: "FirstName", DataType: def.Alpha, Length: 20})

	for _, target := range getTestTargets(t) {
		fmt.Println("Work on " + target.layer)
		id, err := Register(target.layer, target.url)
		if !assert.NoError(t, err, "register fail using "+target.layer) {
			return
		}
		id.DeleteTable("TESTTABLE")
		err = id.CreateTable("TESTTABLE", columns)
		if !assert.NoError(t, err, "create fail using "+target.layer) {
			unregisterDatabase(t, id)
			continue
		}
		list := make([]any, 0)
		list = append(list, []any{"Eins", "Ernie"})
		for i := 1; i < 100; i++ {
			list = append(list, []any{strconv.Itoa(i), "Graf Zahl " + strconv.Itoa(i)})
		}
		list = append(list, []any{"Letztes", "Anton"})
		err = id.Insert("TESTTABLE", &def.Entries{Fields: []string{"name", "firstname"},
			Values: list})
		assert.NoError(t, err, "insert fail using "+target.layer)
		deleteTable(t, id, "TESTTABLE", target.layer)
		unregisterDatabase(t, id)
	}
}

func unregisterDatabase(t *testing.T, id def.RegDbID) {
	err := Unregister(id)
	assert.NoError(t, err)
}

func deleteTable(t *testing.T, id def.RegDbID, name, layer string) {
	err := id.DeleteTable(name)
	assert.NoError(t, err, "delete fail using "+layer)
}
