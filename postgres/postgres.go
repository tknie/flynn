//go:build !flynn_nopostgres
// +build !flynn_nopostgres

/*
* Copyright 2022-2023 Thorsten A. Knieling
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
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tknie/flynn/common"
	"github.com/tknie/flynn/dbsql"
	"github.com/tknie/log"
)

const defaultBlocksize = 4096

const (
	userPlaceholder   = "<user>"
	passwdPlaceholder = "<password>"
)

// PostGres instane for PostgresSQL
type PostGres struct {
	common.CommonDatabase
	openDB       *pgx.Conn
	dbURL        string
	dbTableNames []string
	user         string
	password     string
	tx           pgx.Tx
	ctx          context.Context
}

// New create new postgres reference instance
func NewInstance(id common.RegDbID, reference *common.Reference, password string) (common.Database, error) {

	url := fmt.Sprintf("postgres://%s:"+passwdPlaceholder+"@%s:%d/%s%s", reference.User,
		reference.Host, reference.Port, reference.Database, reference.OptionString())
	pg := &PostGres{common.CommonDatabase{RegDbID: id}, nil,
		url, nil, "", password, nil, nil}

	return pg, nil
}

// New create new postgres reference instance
func New(id common.RegDbID, url string) (common.Database, error) {
	pg := &PostGres{common.CommonDatabase{RegDbID: id}, nil,
		url, nil, "", "", nil, nil}
	// err := pg.check()
	// if err != nil {
	// 	return nil, err
	// }
	return pg, nil
}

// SetCredentials set credentials to connect to database
func (pg *PostGres) SetCredentials(user, password string) error {
	pg.user = user
	pg.password = password
	fmt.Println("Store credentials")
	return nil
}

func (pg *PostGres) generateURL() string {
	url := pg.dbURL
	if pg.user != "" {
		url = strings.Replace(url, userPlaceholder, pg.user, -1)

	}
	if pg.password != "" {
		url = strings.Replace(url, passwdPlaceholder, pg.password, -1)
	}
	return url
}

// Reference reference to postgres URL
func (pg *PostGres) Reference() (string, string) {
	return "pgx", pg.generateURL()
}

// IndexNeeded index needed for the SELECT statement value reference
func (pg *PostGres) IndexNeeded() bool {
	return true
}

// ByteArrayAvailable byte array available in SQL database
func (pg *PostGres) ByteArrayAvailable() bool {
	return true
}

// ID current id used
func (pg *PostGres) ID() common.RegDbID {
	return pg.RegDbID
}

// URL current URL used
func (pg *PostGres) URL() string {
	return pg.dbURL
}

// Maps database maps, tables or views
func (pg *PostGres) Maps() ([]string, error) {
	if pg.dbTableNames == nil {
		err := pg.Ping()
		if err != nil {
			return nil, err
		}
	}
	return pg.dbTableNames, nil
}

func (pg *PostGres) open() (dbOpen *pgx.Conn, err error) {
	if pg.IsTransaction() && pg.openDB != nil {
		return pg.openDB, nil
	}
	pg.ctx = context.Background()
	log.Log.Debugf("Open Postgres database to %s", pg.dbURL)
	log.Log.Debugf("Postgres database URL to %s", pg.generateURL())
	dbOpen, err = pgx.Connect(pg.ctx, pg.generateURL())
	if err != nil {
		log.Log.Debugf("Postgres driver connect error: %v", err)
		return nil, err
	}
	log.Log.Debugf("Opened postgres database")
	if dbOpen == nil {
		return nil, fmt.Errorf("error open handle and err nil")
	}
	return dbOpen, nil
}

// Open open the database connection
func (pg *PostGres) Open() (dbOpen any, err error) {
	db, err := pg.open()
	if err != nil {
		return nil, err
	}
	dbOpen = db

	if pg.IsTransaction() {
		pg.tx, err = db.Begin(context.Background())
		//db.BeginTx(pg.ctx, nil)
		if err != nil {
			return nil, err
		}

	}
	log.Log.Debugf("Opened database %s after transaction", pg.dbURL)
	return db, nil
}

// StartTransaction start transaction the database connection
func (pg *PostGres) BeginTransaction() error {
	if pg.tx != nil && pg.ctx != nil {
		return nil
	}
	var err error
	if pg.openDB == nil {
		_, err = pg.Open()
		if err != nil {
			return err
		}
	}
	_, _, err = pg.StartTransaction()
	if err != nil {
		return err
	}
	//	pg.Transaction = true
	return nil
}

func (pg *PostGres) EndTransaction(commit bool) (err error) {
	if !pg.IsTransaction() {
		return nil
	}
	if pg.tx == nil {
		pg.Transaction = false
		return fmt.Errorf("error transaction not started")
	}
	log.Log.Debugf("End transaction ...%v", pg.IsTransaction())
	if commit {
		err = pg.tx.Commit(pg.ctx)
	} else {
		err = pg.tx.Rollback(pg.ctx)
	}
	pg.tx = nil
	log.Log.Debugf("End transaction done")
	pg.Transaction = false

	return
}

// Close close the database connection
func (pg *PostGres) Close() {
	if pg.ctx != nil {
		log.Log.Debugf("Rollback transaction during close")
		pg.EndTransaction(false)
	}
	if pg.openDB != nil {
		pg.openDB.Close(pg.ctx)
		pg.openDB = nil
		pg.tx = nil
		pg.ctx = nil
		log.Log.Debugf("Closing database done")
		return
	}
	log.Log.Debugf("Close not opened database")
}

// Ping create short test database connection
func (pg *PostGres) Ping() error {
	log.Log.Debugf("Ping database ... by receiving table names")
	pg.dbTableNames = nil
	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}
	db := dbOpen.(*pgx.Conn)
	defer db.Close(pg.ctx)

	pg.dbTableNames = make([]string, 0)

	rows, err := db.Query(context.Background(), "SELECT table_name FROM information_schema.tables WHERE table_schema='public' and (table_type = 'BASE TABLE' or table_type = 'VIEW')")
	if err != nil {
		log.Log.Debugf("Error pinging database ...%v", err)
		return err
	}
	tableName := ""
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			log.Log.Debugf("Error pinging and scan database ...%v", err)
			return err
		}
		pg.dbTableNames = append(pg.dbTableNames, tableName)
	}
	log.Log.Debugf("Pinging and scanning database ended")

	return nil
}

// Delete Delete database records
func (pg *PostGres) Delete(name string, remove *common.Entries) (rowsAffected int64, err error) {
	tx, ctx, err := pg.StartTransaction()
	if err != nil {
		return -1, err
	}
	defer pg.Close()

	if remove.Criteria != "" {
		deleteCmd := "DELETE FROM " + name + " WHERE " + remove.Criteria
		res, err := tx.Exec(ctx, deleteCmd)
		if err != nil {
			log.Log.Debugf("Delete error: %v", err)
			pg.EndTransaction(false)
			return -1, err
		}
		rowsAffected += res.RowsAffected()
	} else {
		for i := 0; i < len(remove.Values); i++ {
			deleteCmd, av := dbsql.GenerateDelete(pg.IndexNeeded(), name, 0, remove)
			log.Log.Debugf("Delete cmd: %s -> %#v", deleteCmd, av)
			res, err := tx.Exec(ctx, deleteCmd, av...)
			// tx.ExecContext(ctx, deleteCmd, av...)
			if err != nil {
				log.Log.Debugf("Delete error: %v", err)
				pg.EndTransaction(false)
				return -1, err
			}
			rowsAffected += res.RowsAffected()
		}
	}
	err = pg.EndTransaction(true)
	if err != nil {
		return -1, err
	}
	log.Log.Debugf("Delete done")
	return
}

// GetTableColumn get table columne names
func (pg *PostGres) GetTableColumn(tableName string) ([]string, error) {
	log.Log.Debugf("Get table column ...")
	dbOpen, err := pg.Open()
	if err != nil {
		return nil, err
	}
	defer pg.Close()

	db := dbOpen.(*pgx.Conn)
	// rows, err := db.Query(`SELECT table_schema, table_name, column_name, data_type
	// FROM INFORMATION_SCHEMA.COLUMNS
	rows, err := db.Query(context.Background(), `SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '`+strings.ToLower(tableName)+`'`)
	if err != nil {
		return nil, err
	}
	// c, err := rows.Columns()
	tableRows := make([]string, 0)
	tableRow := ""
	for rows.Next() {
		err = rows.Scan(&tableRow)
		if err != nil {
			return nil, err
		}
		tableRows = append(tableRows, tableRow)
	}

	return tableRows, nil
}

// Query query database records with search or SELECT
func (pg *PostGres) Query(search *common.Query, f common.ResultFunction) (*common.Result, error) {
	log.Log.Debugf("Query postgres database")
	dbOpen, err := pg.Open()
	if err != nil {
		return nil, err
	}

	db := dbOpen.(*pgx.Conn)
	ctx := context.Background()
	defer db.Close(ctx)
	selectCmd, err := search.Select()
	if err != nil {
		return nil, err
	}
	log.Log.Debugf("Query: %s", selectCmd)
	rows, err := db.Query(ctx, selectCmd)
	if err != nil {
		log.Log.Debugf("Query error: %v", err)
		return nil, err
	}
	if search.DataStruct == nil {
		return pg.ParseRows(search, rows, f)
	}
	return pg.ParseStruct(search, rows, f)
}

func (pg *PostGres) ParseRows(search *common.Query, rows pgx.Rows, f common.ResultFunction) (result *common.Result, err error) {
	log.Log.Debugf("Parse rows ....")
	result = &common.Result{}
	result.Data = search.DataStruct
	result.Fields = make([]string, 0)
	for _, f := range rows.FieldDescriptions() {
		result.Fields = append(result.Fields, f.Name)
	}
	for rows.Next() {
		log.Log.Debugf("Checking row...")

		result.Rows, err = rows.Values()
		if err != nil {
			return nil, err
		}
		err = f(search, result)
		if err != nil {
			return nil, err
		}
	}
	log.Log.Debugf("Finishing row...")
	if err = rows.Err(); err != nil {
		log.Log.Debugf("Error found: %v", err)
		return nil, err
	}
	return result, nil
}

func (pg *PostGres) ParseStruct(search *common.Query, rows pgx.Rows, f common.ResultFunction) (result *common.Result, err error) {
	if search.DataStruct == nil {
		return pg.ParseRows(search, rows, f)
	}
	result = &common.Result{}

	result.Data = search.DataStruct
	copy, values, err := result.GenerateColumnByStruct(search)
	if err != nil {
		log.Log.Debugf("Error generating column: %v", err)
		return nil, err
	}
	log.Log.Debugf("Parse columns rows -> flen=%d vlen=%d %T",
		len(result.Fields), len(values), copy)
	for rows.Next() {
		log.Log.Debugf("Row found and scanning")
		if len(result.Fields) == 0 {
			for _, f := range rows.FieldDescriptions() {
				result.Fields = append(result.Fields, f.Name)
			}
		}
		err := rows.Scan(values...)
		if err != nil {
			fmt.Println("Error scanning structs", values, err)
			log.Log.Debugf("Error during scan of struct: %v/%v", err, copy)
			return nil, err
		}
		result.Data = copy
		err = f(search, result)
		if err != nil {
			return nil, err
		}
	}
	return
}

// CreateTable create a new table
func (pg *PostGres) CreateTable(name string, col any) error {
	//	columns []*common.Column
	log.Log.Debugf("Create SQL table")
	layer, url := pg.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	createCmd := `CREATE TABLE ` + name + ` (`
	switch columns := col.(type) {
	case []*common.Column:
		createCmd += dbsql.CreateTableByColumns(pg.ByteArrayAvailable(), columns)
	default:
		c, err := dbsql.CreateTableByStruct(pg.ByteArrayAvailable(), col)
		if err != nil {
			log.Log.Errorf("Error parsing structure: %v", err)
			return err
		}
		createCmd += c
	}
	createCmd += ")"
	log.Log.Debugf("Create cmd %s", createCmd)
	_, err = db.Exec(createCmd)
	if err != nil {
		log.Log.Errorf("Error returned by SQL: %v", err)
		return err
	}
	//log.Log.Debugf("Table created, waiting ....")
	//time.Sleep(60 * time.Second)
	log.Log.Debugf("Table created")
	err = db.Close()
	if err != nil {
		return err
	}
	log.Log.Debugf("Table db handler closed")
	return nil
}

// DeleteTable delete a table
func (pg *PostGres) DeleteTable(name string) error {
	layer, url := pg.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Log.Debugf("Init Drop table %s", name)
	_, err = db.Query("DROP TABLE " + name)
	if err != nil {
		log.Log.Debugf("Drop table error: %v", err)
		return err
	}
	log.Log.Debugf("Drop table " + name)
	return nil
}

// Insert insert record into table
func (pg *PostGres) Insert(name string, insert *common.Entries) (err error) {
	var ctx context.Context
	var tx pgx.Tx
	transaction := pg.IsTransaction()
	log.Log.Debugf("Transaction (begin insert): %v", transaction)
	if !transaction {
		tx, ctx, err = pg.StartTransaction()
		if err != nil {
			return err
		}
		defer pg.Close()
	} else {
		tx = pg.tx
		ctx = pg.ctx
	}
	if tx == nil || ctx == nil {
		return fmt.Errorf("transaction=%v or context=%v not set", tx, ctx)
	}
	if !pg.IsTransaction() {
		log.Log.Debugf("Init defer close ... in inserting")
		pg.Close()
		return fmt.Errorf("init of transaction fails")
	}

	log.Log.Debugf("Insert SQL record")

	insertCmd := "INSERT INTO " + name + " ("
	values := "("

	indexNeed := pg.IndexNeeded()
	var insertValues [][]any
	var insertFields []string
	if insert.DataStruct != nil {
		dynamic := common.CreateInterface(insert.DataStruct, insert.Fields)
		insertFields = dynamic.RowFields
		v := dynamic.CreateInsertValues()
		insertValues = [][]any{v}
		log.Log.Debugf("Row   fields: %#v", insertFields)
		log.Log.Debugf("Value fields: %#v", insertValues)
	} else {
		insertFields = insert.Fields
		insertValues = insert.Values
	}
	for i, field := range insertFields {
		if i > 0 {
			insertCmd += ","
			values += ","
		}
		if indexNeed {
			insertCmd += `"` + strings.ToLower(field) + `"`
			values += "$" + strconv.Itoa(i+1)
		} else {
			insertCmd += "`" + strings.ToLower(field) + "`"
			values += "?"
		}
	}

	values += ")"
	insertCmd += ") VALUES " + values
	log.Log.Debugf("Insert pre-CMD: %s", insertCmd)
	for _, v := range insertValues {
		av := v
		log.Log.Debugf("Insert values: %d -> %#v", len(av), av)
		res, err := tx.Exec(ctx, insertCmd, av...)
		if err != nil {
			trErr := pg.EndTransaction(false)
			log.Log.Debugf("Error insert CMD: %v of %s and cmd %s trErr=%v",
				err, name, insertCmd, trErr)
			return err
		}
		l := res.RowsAffected()
		if l == 0 {
			return fmt.Errorf("insert of rows failed")
		}
	}
	if !transaction {
		log.Log.Debugf("Need to end because not in Transaction: %v", pg.IsTransaction())
		err = pg.EndTransaction(true)
		if err != nil {
			log.Log.Debugf("Error transaction %v", err)
			return err
		}
	}
	return nil
}

// Update update record in table
func (pg *PostGres) Update(name string, updateInfo *common.Entries) (rowsAffected int64, err error) {
	transaction := pg.IsTransaction()
	var ctx context.Context
	var tx pgx.Tx
	if !transaction {
		tx, ctx, err = pg.StartTransaction()
		if err != nil {
			return -1, err
		}
		defer pg.Close()
	} else {
		tx = pg.tx
		ctx = pg.ctx
	}
	insertCmd, whereFields := dbsql.GenerateUpdate(pg.IndexNeeded(), name, updateInfo)
	var updateValues [][]any
	if updateInfo.DataStruct != nil {
		dynamic := common.CreateInterface(updateInfo.DataStruct, updateInfo.Fields)
		v := dynamic.CreateInsertValues()
		updateValues = [][]any{v}
	} else {
		updateValues = updateInfo.Values
	}

	for i, v := range updateValues {
		whereClause := dbsql.CreateWhere(i, updateInfo, whereFields)
		ic := insertCmd + whereClause
		log.Log.Debugf("Update call: %s", ic)
		log.Log.Debugf("Update values: %d -> %#v tx=%v %v", len(v), v, tx, ctx)
		res, err := tx.Exec(ctx, ic, v...)
		if err != nil {
			log.Log.Debugf("Update error: %s -> %v", ic, err)
			pg.EndTransaction(false)
			return 0, err
		}
		rowsAffected += res.RowsAffected()
		log.Log.Debugf("Rows affected %d", rowsAffected)
	}
	log.Log.Debugf("Update done")

	if !transaction {
		err = pg.EndTransaction(true)
		if err != nil {
			return -1, err
		}
	}
	return rowsAffected, nil
}

// Batch batch SQL query in table
func (pg *PostGres) Batch(batch string) error {
	layer, url := pg.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	// Query batch SQL
	rows, err := db.Query(batch)
	if err != nil {
		return err
	}
	for rows.Next() {
		if rows.Err() != nil {
			fmt.Println("Batch SQL error:", rows.Err())
			return rows.Err()
		}
	}
	return nil
}

// BatchSelect batch SQL query in table with values returned
func (pg *PostGres) BatchSelect(batch string) ([][]interface{}, error) {
	layer, url := pg.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	// Query batch SQL
	rows, err := db.Query(batch)
	if err != nil {
		return nil, err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([][]interface{}, 0)
	for rows.Next() {
		if rows.Err() != nil {
			fmt.Println("Batch SQL error:", rows.Err())
			return nil, rows.Err()
		}
		data := common.CreateTypeData(ct)
		err := rows.Scan(data...)
		if err != nil {
			return nil, err
		}
		data = common.Unpointer(data)
		result = append(result, data)
	}
	return result, nil
}

// BatchSelectFct batch SQL query in table with fct called
func (pg *PostGres) BatchSelectFct(batch string, fct common.ResultDataFunction) error {
	layer, url := pg.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()
	// Query batch SQL
	rows, err := db.Query(batch)
	if err != nil {
		return err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	var header []*common.Column
	count := uint64(0)
	for rows.Next() {
		if rows.Err() != nil {
			fmt.Println("Batch SQL error:", rows.Err())
			return rows.Err()
		}
		if header == nil {
			header = common.CreateHeader(ct)
		}
		data := common.CreateTypeData(ct)
		err := rows.Scan(data...)
		if err != nil {
			return err
		}
		data = common.Unpointer(data)
		count++
		fct(count, header, data)
	}
	return nil
}

// StartTransaction start transaction
func (pg *PostGres) StartTransaction() (pgx.Tx, context.Context, error) {
	var err error
	if pg.openDB == nil {
		pg.Transaction = true
		pg.openDB, err = pg.open()
		if err != nil {
			log.Log.Debugf("Error opening connection for transaction")
			return nil, nil, err
		}
	}
	log.Log.Debugf("Start transaction opened")
	pg.ctx = context.Background()
	pg.tx, err = pg.openDB.Begin(pg.ctx)
	if err != nil {
		pg.ctx = nil
		pg.tx = nil
		log.Log.Debugf("Begin of transaction fails: %v", err)
		return nil, nil, err
	}
	log.Log.Debugf("Start transaction begin")
	return pg.tx, pg.ctx, nil
}

// Commit commit the transaction
func (pg *PostGres) Commit() error {
	log.Log.Debugf("Commit transaction")
	return pg.EndTransaction(true)
}

// Rollback rollback the transaction
func (pg *PostGres) Rollback() error {
	log.Log.Debugf("Rollback transaction")
	return pg.EndTransaction(false)
}

func (pg *PostGres) Stream(search *common.Query, sf common.StreamFunction) error {
	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}

	ctx := pg.ctx
	conn := dbOpen.(*pgx.Conn)
	defer conn.Close(ctx)

	blocksize := search.Blocksize
	if blocksize == 0 {
		blocksize = defaultBlocksize
	}
	offset := int32(0)
	dataMaxLen := int32(math.MaxInt32)

	log.Log.Debugf("Start stream for %s for %s", search.Fields[0], search.TableName)
	for offset < dataMaxLen {
		selectCmd := ""
		if dataMaxLen == int32(math.MaxInt32) {
			selectCmd = fmt.Sprintf("SELECT substring(%s,%d,%d),length(%s) FROM %s WHERE %s",
				search.Fields[0], offset, blocksize, search.Fields[0], search.TableName, search.Search)
		} else {
			selectCmd = fmt.Sprintf("SELECT substring(%s,%d,%d) FROM %s WHERE %s",
				search.Fields[0], offset, blocksize, search.TableName, search.Search)
		}
		log.Log.Debugf("Read = %d,%d -> %s\n", offset, offset+blocksize, selectCmd)
		rows, err := conn.Query(ctx, selectCmd)
		if err != nil {
			log.Log.Debugf("Stream query error: %v", err)
			return err
		}
		stream := &common.Stream{}
		for rows.Next() {
			v, err := rows.Values()
			if err != nil {
				log.Log.Debugf("Stream value error: %v", err)
				return err
			}
			if dataMaxLen == int32(math.MaxInt32) {
				dataMaxLen = v[1].(int32)
				log.Log.Debugf("Data maximal length = %d\n", dataMaxLen)
			}
			stream.Data = v[0].([]uint8)
			err = sf(search, stream)
			if err != nil {
				log.Log.Debugf("Stream error: %v", err)
				return err
			}
		}
		offset += blocksize
		if offset >= dataMaxLen {
			break
		}
		if offset+blocksize > dataMaxLen {
			blocksize = dataMaxLen - offset + 1
		}
		log.Log.Debugf("Stream offset = %d,%d\n", offset, blocksize)
	}
	log.Log.Debugf("Stream finished")
	return nil
}
