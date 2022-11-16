package common

import (
	"database/sql"
	"time"
)

type RegDbID uint64

type Query struct {
	TableName  string
	Search     string
	Fields     []string
	DataStruct any
	TypeInfo   any
}

type Result struct {
	Rows []any
	Data any
}

type Entries struct {
	Fields []string
	Values []any
}

type Database interface {
	ID() RegDbID
	URL() string
	Maps() ([]string, error)
	GetTableColumn(tableName string) ([]string, error)
	Insert(insert *Entries) error
	Delete(remove *Entries) error
	Query(search *Query, f ResultFunction) error
}

type ResultFunction func(search *Query, result *Result) error

type CommonDatabase struct {
	RegDbID RegDbID
}

func (id RegDbID) Query(query *Query, f ResultFunction) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Query(query, f)
}

func (id RegDbID) Insert(insert *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Insert(insert)
}

func (id RegDbID) Delete(remove *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Delete(remove)
}

func (id RegDbID) GetTableColumn(tableName string) ([]string, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	return driver.GetTableColumn(tableName)
}

func (result *Result) GenerateColumnByStruct(search *Query, rows *sql.Rows) error {
	ti := search.TypeInfo.(*typeInterface)
	ti.CreateQueryValues()
	result.Rows = ti.RowValues
	result.Data = ti.DataType
	return nil
}

func (search *Query) QueryRows(rows *sql.Rows, f ResultFunction) (err error) {
	result := &Result{}

	result.Data = search.DataStruct
	if search.DataStruct == nil {
		result.Rows, err = generateColumnByValues(rows)
	} else {
		err = result.GenerateColumnByStruct(search, rows)
	}
	if err != nil {
		return err
	}
	for rows.Next() {
		err := rows.Scan(result.Rows...)
		if err != nil {
			return err
		}
		err = f(search, result)
		if err != nil {
			return err
		}
	}
	return nil
}

func generateColumnByValues(rows *sql.Rows) ([]any, error) {
	colsType, err := rows.ColumnTypes()
	if err != nil {
		if Log.IsDebugLevel() {
			Log.Debugf("Error cols read: %v", err)
		}
		return nil, err
	}
	colsValue := make([]any, 0)
	for nr, col := range colsType {
		len, ok := col.Length()
		if Log.IsDebugLevel() {
			Log.Debugf("Colnr=%d name=%s len=%d ok=%v", nr, col.Name(), len, ok)
		}
		switch col.DatabaseTypeName() {
		case "VARCHAR2":
			s := ""
			colsValue = append(colsValue, &s)
		case "NUMBER":
			s := int64(0)
			colsValue = append(colsValue, &s)
		case "LONG":
			s := ""
			colsValue = append(colsValue, &s)
		case "DATE":
			n := time.Now()
			colsValue = append(colsValue, &n)
		default:
			s := sql.NullString{}
			colsValue = append(colsValue, &s)
		}
	}
	return colsValue, nil
}
