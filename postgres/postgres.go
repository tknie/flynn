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
	def "github.com/tknie/flynn/common"
	"github.com/tknie/flynn/dbsql"
	"github.com/tknie/log"
)

const defaultBlocksize = 4096

// PostGres instane for PostgresSQL
type PostGres struct {
	def.CommonDatabase
	openDB       *pgx.Conn
	dbURL        string
	dbTableNames []string
	user         string
	password     string
	tx           pgx.Tx
	ctx          context.Context
}

// New create new postgres reference instance
func NewInstance(id def.RegDbID, reference *common.Reference, password string) (def.Database, error) {
	url := fmt.Sprintf("postgres://%s:<password>@%s:%d/%s", reference.User, reference.Host, reference.Port, reference.Database)
	pg := &PostGres{def.CommonDatabase{RegDbID: id}, nil,
		url, nil, "", password, nil, nil}

	return pg, nil
}

// New create new postgres reference instance
func New(id def.RegDbID, url string) (def.Database, error) {
	pg := &PostGres{def.CommonDatabase{RegDbID: id}, nil,
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
		url = strings.Replace(url, "<user>", pg.user, -1)
	}
	if pg.password != "" {
		url = strings.Replace(url, "<password>", pg.password, -1)
	}
	return url
}

// Reference reference to postgres URL
func (pg *PostGres) Reference() (string, string) {
	return "pgx", pg.dbURL
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
func (pg *PostGres) ID() def.RegDbID {
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
	if pg.openDB == nil {
		log.Log.Debugf("Open postgres database to %s", pg.dbURL)
		pg.openDB, err = pgx.Connect(context.Background(), pg.generateURL())
		// sql.Open("pgx", pg.generateURL())
		if err != nil {
			return nil, err
		}
	}
	log.Log.Debugf("Opened postgres database")
	if pg.openDB == nil {
		return nil, fmt.Errorf("error open handle and err nil")
	}
	return pg.openDB, nil
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
	log.Log.Debugf("Open database %s after transaction", pg.dbURL)
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
	pg.Transaction = true
	return nil
}

func (pg *PostGres) EndTransaction(commit bool) (err error) {
	if pg.tx == nil && pg.ctx == nil {
		return nil
	}
	if pg.IsTransaction() {
		return nil
	}
	if commit {
		err = pg.tx.Commit(pg.ctx)
	} else {
		err = pg.tx.Rollback(pg.ctx)
	}
	pg.tx = nil
	pg.ctx = nil
	return
}

// Close close the database connection
func (pg *PostGres) Close() {
	if pg.ctx != nil {
		log.Log.Debugf("Rollback transaction during close")
		pg.EndTransaction(false)
	}
	if pg.openDB != nil {
		pg.openDB.Close(context.Background())
		pg.openDB = nil
		pg.tx = nil
		pg.ctx = nil
		log.Log.Debugf("Close database")
	} else {
		log.Log.Debugf("Close not opened database")
	}
}

// Ping create short test database connection
func (pg *PostGres) Ping() error {

	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}
	defer pg.Close()

	db := dbOpen.(*pgx.Conn)

	pg.dbTableNames = make([]string, 0)

	rows, err := db.Query(context.Background(), "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		return err
	}
	tableName := ""
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			return err
		}
		pg.dbTableNames = append(pg.dbTableNames, tableName)
	}

	return nil
}

// Delete Delete database records
func (pg *PostGres) Delete(name string, remove *def.Entries) (rowsAffected int64, err error) {
	tx, ctx, err := pg.StartTransaction()
	if err != nil {
		return -1, err
	}
	if !pg.IsTransaction() {
		defer pg.Close()
	}

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
	err = pg.EndTransaction(true)
	if err != nil {
		return -1, err
	}
	log.Log.Debugf("Delete done")
	return
}

// GetTableColumn get table columne names
func (pg *PostGres) GetTableColumn(tableName string) ([]string, error) {
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
func (pg *PostGres) Query(search *def.Query, f def.ResultFunction) (*common.Result, error) {
	log.Log.Debugf("Query postgres database")
	dbOpen, err := pg.Open()
	if err != nil {
		return nil, err
	}

	db := dbOpen.(*pgx.Conn)
	ctx := context.Background()
	defer db.Close(ctx)
	selectCmd := search.Select()

	log.Log.Debugf("Query: %s", selectCmd)
	rows, err := db.Query(ctx, selectCmd)
	if err != nil {
		return nil, err
	}
	if search.DataStruct == nil {
		return pg.ParseRows(search, rows, f)
	}
	return pg.ParseStruct(search, rows, f)
}

func (pg *PostGres) ParseRows(search *def.Query, rows pgx.Rows, f common.ResultFunction) (result *common.Result, err error) {
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

func (pg *PostGres) ParseStruct(search *def.Query, rows pgx.Rows, f common.ResultFunction) (result *common.Result, err error) {
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
	log.Log.Debugf("Parse columns rows")
	for rows.Next() {
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
	//	columns []*def.Column
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
	_, err = db.Query(createCmd)
	if err != nil {
		log.Log.Errorf("Error returned by SQL: %v", err)
		return err
	}
	log.Log.Debugf("Table created, waiting ....")
	//time.Sleep(60 * time.Second)
	log.Log.Debugf("Table created")
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

	_, err = db.Query("DROP TABLE " + name)
	if err != nil {
		log.Log.Debugf("Drop table error: %v", err)
		return err
	}
	log.Log.Debugf("Drop table " + name)
	return nil
}

// Insert insert record into table
func (pg *PostGres) Insert(name string, insert *def.Entries) error {
	log.Log.Debugf("Transaction (begin insert): %v", pg.IsTransaction())
	tx, ctx, err := pg.StartTransaction()
	if err != nil {
		return err
	}
	if !pg.IsTransaction() {
		log.Log.Debugf("Init defer close ... in inserting")
		defer pg.Close()
	}

	log.Log.Debugf("Insert SQL record")

	insertCmd := "INSERT INTO " + name + " ("
	values := "("

	indexNeed := pg.IndexNeeded()
	for i, field := range insert.Fields {
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
	for _, v := range insert.Values {
		av := v
		log.Log.Debugf("Insert values: %d -> %#v", len(av), av)
		res, err := tx.Exec(ctx, insertCmd, av...)
		if err != nil {
			pg.EndTransaction(false)
			log.Log.Debugf("Error insert CMD: %v of %s and cmd %s", err, name, insertCmd)
			return err
		}
		l := res.RowsAffected()
		if l == 0 {
			return fmt.Errorf("insert of rows failed")
		}
	}
	log.Log.Debugf("Transaction: %v", pg.IsTransaction())
	if !pg.IsTransaction() {
		log.Log.Debugf("No transaction, end and close")
		err = pg.EndTransaction(true)
		if err != nil {
			log.Log.Debugf("Error transaction %v", err)
			pg.Close()
			return err
		}
		log.Log.Debugf("Close ...")
		pg.Close()
	} else {
		log.Log.Debugf("Transaction, NO end and close")
	}
	return nil
}

// Update update record in table
func (pg *PostGres) Update(name string, updateInfo *def.Entries) (rowsAffected int64, err error) {
	tx, ctx, err := pg.StartTransaction()
	if err != nil {
		return -1, err
	}
	if !pg.IsTransaction() {
		defer pg.Close()
	}
	insertCmd, whereFields := dbsql.GenerateUpdate(pg.IndexNeeded(), name, updateInfo)
	for i, v := range updateInfo.Values {
		whereClause := dbsql.CreateWhere(i, updateInfo, whereFields)
		ic := insertCmd + whereClause
		log.Log.Debugf("Update values: %d -> %#v tx=%v %v", len(v), v, tx, ctx)
		res, err := tx.Exec(ctx, ic, v...)
		if err != nil {
			log.Log.Debugf("Update error: %s -> %v", ic, err)
			pg.EndTransaction(false)
			return 0, err
		}
		rowsAffected += res.RowsAffected()
	}
	log.Log.Debugf("Update done")

	err = pg.EndTransaction(true)
	if err != nil {
		return -1, err
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
	// TODO
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

// StartTransaction start transaction
func (pg *PostGres) StartTransaction() (pgx.Tx, context.Context, error) {
	_, err := pg.open()
	if err != nil {
		return nil, nil, err
	}
	pg.ctx = context.Background()
	pg.tx, err = pg.openDB.Begin(pg.ctx)
	if err != nil {
		pg.ctx = nil
		pg.tx = nil
		return nil, nil, err
	}
	return pg.tx, pg.ctx, nil
}

// Commit commit the transaction
func (pg *PostGres) Commit() error {
	pg.Transaction = false
	return pg.EndTransaction(true)
}

// Rollback rollback the transaction
func (pg *PostGres) Rollback() error {
	pg.Transaction = false
	return pg.EndTransaction(false)
}

func (pg *PostGres) Stream(search *def.Query, sf def.StreamFunction) error {
	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}

	ctx := context.Background()
	conn := dbOpen.(*pgx.Conn)
	defer conn.Close(ctx)

	blocksize := search.Blocksize
	if blocksize == 0 {
		blocksize = defaultBlocksize
	}
	offset := int32(0)
	dataMaxLen := int32(math.MaxInt32)

	for offset < dataMaxLen {
		selectCmd := fmt.Sprintf("SELECT substring(%s,%d,%d),length(%s) FROM %s WHERE %s",
			search.Fields[0], offset, blocksize, search.Fields[0], search.TableName, search.Search)
		rows, err := conn.Query(ctx, selectCmd)
		if err != nil {
			return err
		}
		stream := &def.Stream{}
		for rows.Next() {
			v, err := rows.Values()
			if err != nil {
				return err
			}
			dataMaxLen = v[1].(int32)
			// fmt.Printf("Len = %d\n", dataMaxLen)
			stream.Data = v[0].([]uint8)
			err = sf(search, stream)
			if err != nil {
				return err
			}
		}
		offset += blocksize
	}
	return nil
}
