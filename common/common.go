package common

import (
	"database/sql"
	"fmt"
	"time"
)

type RegDbID uint64

type Query struct {
	TableName  string
	Search     string
	Fields     []string
	Limit      uint32
	DataStruct any
	TypeInfo   any
}

type Result struct {
	Fields []string
	Rows   []any
	Data   any
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
	CreateTable(string, any) error
	DeleteTable(string) error
	Insert(name string, insert *Entries) error
	Delete(name string, remove *Entries) error
	BatchSQL(batch string) error
	Query(search *Query, f ResultFunction) (*Result, error)
}

type Column struct {
	Name       string
	DataType   DataType
	Length     uint16
	Digits     uint8
	SubColumns []*Column
}

type ResultFunction func(search *Query, result *Result) error

type CommonDatabase struct {
	RegDbID RegDbID
}

func (id RegDbID) Query(query *Query, f ResultFunction) (*Result, error) {
	driver, err := searchDataDriver(id)
	if err != nil {
		return nil, err
	}
	return driver.Query(query, f)
}

func (id RegDbID) CreateTable(tableName string, columns any) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.CreateTable(tableName, columns)
}

func (id RegDbID) DeleteTable(tableName string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.DeleteTable(tableName)
}

func (id RegDbID) BatchSQL(batch string) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.BatchSQL(batch)
}

func (id RegDbID) Insert(name string, insert *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Insert(name, insert)
}

func (id RegDbID) Delete(name string, remove *Entries) error {
	driver, err := searchDataDriver(id)
	if err != nil {
		return err
	}
	return driver.Delete(name, remove)
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

func (search *Query) ParseRows(rows *sql.Rows, f ResultFunction) (result *Result, err error) {
	result = &Result{}

	result.Data = search.DataStruct
	// rows := make([]any, len(result.Rows))
	var scanRows []any
	if search.DataStruct == nil {
		scanRows, err = generateColumnByValues(rows)
	} else {
		err = result.GenerateColumnByStruct(search, rows)
	}
	if err != nil {
		Log.Debugf("Error generating column: %v", err)
		return nil, err
	}
	Log.Debugf("Parse columns rows")
	result.Fields, err = rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		err := rows.Scan(scanRows...)
		if err != nil {
			Log.Debugf("Error during scan: %v", err)
			return nil, err
		}
		result.Rows = make([]any, len(scanRows))
		for i, r := range scanRows {
			switch n := r.(type) {
			case *sql.NullByte:
				if n.Valid {
					result.Rows[i] = n.Byte
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullBool:
				if n.Valid {
					result.Rows[i] = n.Bool
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullString:
				if n.Valid {
					result.Rows[i] = n.String
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullInt32:
				if n.Valid {
					result.Rows[i] = n.Int32
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullInt64:
				if n.Valid {
					result.Rows[i] = n.Int64
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullInt16:
				if n.Valid {
					result.Rows[i] = n.Int16
				} else {
					result.Rows[i] = nil
				}
			case *sql.NullTime:
				if n.Valid {
					result.Rows[i] = n.Time
				} else {
					result.Rows[i] = nil
				}
			default:
				result.Rows[i] = r
			}
		}
		err = f(search, result)
		if err != nil {
			return nil, err
		}
	}
	return
}

func generateColumnByValues(rows *sql.Rows) ([]any, error) {
	colsType, err := rows.ColumnTypes()
	if err != nil {
		if Log.IsDebugLevel() {
			Log.Debugf("Error cols read: %v", err)
		}
		return nil, err
	}
	Log.Debugf("Create columns values")
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
		case "DATE", "TIMESTAMP":
			n := time.Now()
			colsValue = append(colsValue, &n)
		default:
			s := sql.NullString{}
			colsValue = append(colsValue, &s)
		}
	}
	return colsValue, nil
}

func (q *Query) Select() string {
	selectCmd := ""
	switch {
	case q.DataStruct != nil:
		selectCmd = "select "
		ti := CreateInterface(q.DataStruct)
		q.TypeInfo = ti
		selectCmd += ti.CreateQueryFields()
		selectCmd += " from " + q.TableName
	default:
		selectCmd = "select "
		for i, s := range q.Fields {
			if i > 0 {
				selectCmd += ","
			}
			selectCmd += s
		}
		selectCmd += " from " + q.TableName
	}
	if q.Search != "" {
		selectCmd += " where " + q.Search
	}
	if q.Limit > 0 {
		selectCmd += fmt.Sprintf(" limit = %d", q.Limit)
	}
	return selectCmd
}
