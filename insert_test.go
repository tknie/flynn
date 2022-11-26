package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	def "github.com/tknie/db/common"
)

const testTable = "TestTableData"

func TestInsertInitTestTable(t *testing.T) {
	for _, target := range getTestTargets(t) {
		if target.layer == "adabas" {
			continue
		}
		if checkTableAvailablefunc(t, target) != nil {
			return
		}
	}
}

func checkTableAvailablefunc(t *testing.T, target *target) error {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return err
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return err
	}
	defer Unregister(x)

	q := &def.Query{TableName: testTable,
		Search: "",
		Fields: []string{"Name"}}
	counter := 0
	err = x.Query(q, func(search *def.Query, result *def.Result) error {
		counter++
		return nil
	})
	if err == nil {
		return nil
	}
	if counter == 0 {
		err = createTestTable(t, target)
		if !assert.NoError(t, err) {
			return err
		}
	}
	return nil
}

func createTestTable(t *testing.T, target *target) error {
	columns := make([]*def.Column, 0)
	columns = append(columns, &def.Column{Name: "ID", DataType: def.Alpha, Length: 10})
	columns = append(columns, &def.Column{Name: "Name", DataType: def.Alpha, Length: 200})
	columns = append(columns, &def.Column{Name: "MiddleName", DataType: def.Alpha, Length: 50})
	columns = append(columns, &def.Column{Name: "FirstName", DataType: def.Alpha, Length: 50})
	columns = append(columns, &def.Column{Name: "PersonnelNo", DataType: def.Number, Length: 4})
	columns = append(columns, &def.Column{Name: "CardNo", DataType: def.Bytes, Length: 8})
	columns = append(columns, &def.Column{Name: "Signature", DataType: def.Alpha, Length: 20})
	columns = append(columns, &def.Column{Name: "Sex", DataType: def.Alpha, Length: 1})
	columns = append(columns, &def.Column{Name: "MarrieState", DataType: def.Alpha, Length: 1})
	columns = append(columns, &def.Column{Name: "Street", DataType: def.Alpha, Length: 200})
	columns = append(columns, &def.Column{Name: "Address", DataType: def.Alpha, Length: 200})
	columns = append(columns, &def.Column{Name: "City", DataType: def.Alpha, Length: 200})
	columns = append(columns, &def.Column{Name: "PostCode", DataType: def.Alpha, Length: 10})
	columns = append(columns, &def.Column{Name: "Birth", DataType: def.Date, Length: 10})
	columns = append(columns, &def.Column{Name: "Account", DataType: def.Decimal, Length: 10, Digits: 2})
	columns = append(columns, &def.Column{Name: "Description", DataType: def.Text, Length: 0})
	columns = append(columns, &def.Column{Name: "Flags", DataType: def.Bit, Length: 8})
	columns = append(columns, &def.Column{Name: "AreaCode", DataType: def.Integer, Length: 8})
	columns = append(columns, &def.Column{Name: "Phone", DataType: def.Integer, Length: 8})
	columns = append(columns, &def.Column{Name: "Department", DataType: def.Alpha, Length: 6})
	columns = append(columns, &def.Column{Name: "JobTitle", DataType: def.Alpha, Length: 20})
	columns = append(columns, &def.Column{Name: "Currency", DataType: def.Alpha, Length: 2})
	columns = append(columns, &def.Column{Name: "Salary", DataType: def.Integer, Length: 8})
	columns = append(columns, &def.Column{Name: "Bonus", DataType: def.Integer, Length: 8})
	columns = append(columns, &def.Column{Name: "LeaveDue", DataType: def.Integer, Length: 2})
	columns = append(columns, &def.Column{Name: "LeaveTaken", DataType: def.Integer, Length: 2})
	columns = append(columns, &def.Column{Name: "LeaveStart", DataType: def.Date})
	columns = append(columns, &def.Column{Name: "LeaveEnd", DataType: def.Date})
	columns = append(columns, &def.Column{Name: "Language", DataType: def.Integer, Length: 8})

	fmt.Println("Create database table")

	id, err := Register(target.layer, target.url)
	if !assert.NoError(t, err, "register fail using "+target.layer) {
		return err
	}
	defer unregisterDatabase(t, id)
	id.DeleteTable(testTable)
	err = id.CreateTable(testTable, columns)
	if !assert.NoError(t, err, "create test table fail using "+target.layer) {
		return err
	}
	return nil
}
