package common

import "database/sql"

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

type Database interface {
	ID() RegDbID
	URL() string
	Maps() ([]string, error)
	Insert(fields []string, values []any) error
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

func (result *Result) GenerateColumnByStruct(search *Query, rows *sql.Rows) error {
	ti := search.TypeInfo.(*typeInterface)
	ti.CreateQueryValues()
	result.Rows = ti.RowValues
	result.Data = ti.DataType
	return nil
}
