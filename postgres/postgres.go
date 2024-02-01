//go:build !flynn_nopostgres
// +build !flynn_nopostgres

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
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	openDB *pgxpool.Conn
	// pool         *pgxpool.Pool
	dbURL        string
	dbTableNames []string
	user         string
	password     string
	tx           pgx.Tx
	ctx          context.Context
	cancel       context.CancelFunc
	txLock       sync.Mutex
}

type pool struct {
	useCounter uint64
	pool       *pgxpool.Pool
	ctx        context.Context
	url        string
}

var poolMap sync.Map

func (p *pool) IncUsage() uint64 {
	used := atomic.AddUint64(&p.useCounter, 1)
	log.Log.Debugf("Inc usage = %d", used)
	return used
}

func (p *pool) DecUsage() uint64 {
	c := atomic.AddUint64(&p.useCounter, ^uint64(0))
	log.Log.Debugf("Dec usage = %d", c)
	if c == 0 {
		log.Log.Debugf("Pool closing %p", p.pool)
		p.pool.Close()
		p.pool = nil
		log.Log.Debugf("Pool closed")
		poolMap.Delete(p.url)
	}
	return c
}

// NewInstance create new postgres reference instance using reference structure
func NewInstance(id common.RegDbID, reference *common.Reference, password string) (common.Database, error) {

	url := fmt.Sprintf("postgres://%s:"+passwdPlaceholder+"@%s:%d/%s%s", reference.User,
		reference.Host, reference.Port, reference.Database, reference.OptionString())
	pg := &PostGres{common.CommonDatabase{RegDbID: id}, nil,
		url, nil, "", password, nil, nil, nil, sync.Mutex{}}
	log.Log.Debugf("PG Password is empty=%v", password == "")
	return pg, nil
}

// New create new postgres reference instance
func New(id common.RegDbID, url string) (common.Database, error) {
	reference, passwd, err := common.NewReference(url)
	if err != nil {
		return nil, err
	}
	return NewInstance(id, reference, passwd)
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
		log.Log.Debugf("Replace URL user")
		url = strings.Replace(url, userPlaceholder, pg.user, -1)

	}
	if pg.password != "" {
		log.Log.Debugf("Replace URL password")
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

func (pg *PostGres) getPool() (*pool, error) {
	url := pg.generateURL()
	if p, ok := poolMap.Load(url); ok {
		pg.ctx = p.(*pool).ctx
		return p.(*pool), nil
	} else {
		pg.defineContext()
		if pg.ctx == nil {
			return nil, fmt.Errorf("context error nil")
		}
		//config := &pgx.ConnConfig{Tracer: tracer}
		config, err := pgxpool.ParseConfig(pg.generateURL())
		if err != nil {
			return nil, err
		}
		// config.Tracer = tracer

		// pg.ctx = context.Background()
		log.Log.Debugf("Create pool for Postgres database to %s", pg.dbURL)
		p := &pool{url: url, ctx: pg.ctx}
		p.pool, err = pgxpool.NewWithConfig(pg.ctx, config)
		if err != nil {
			log.Log.Debugf("Postgres driver connect error: %v", err)
			return nil, err
		}

		poolMap.Store(url, p)

		return p, nil
	}
}

func (pg *PostGres) open() (dbOpen *pgxpool.Conn, err error) {
	if pg.IsTransaction() && pg.openDB != nil {
		return pg.openDB, nil
	}
	p, err := pg.getPool()
	if err != nil {
		return nil, err
	}
	if p == nil {
		log.Log.Fatalf("p=%v defined", (p == nil))
	}
	if p.pool == nil {
		log.Log.Fatalf("p.pool=%v defined", p.pool == nil)
	}
	/*tracer := &tracelog.TraceLog{
		Logger:   NewLogger(),
		LogLevel: tracelog.LogLevelTrace,
	}*/
	log.Log.Debugf("Acquire Postgres to pool=%p %v", p.pool, pg.ID())
	dbOpen, err = p.pool.Acquire(pg.ctx)
	if err != nil {
		log.Log.Debugf("Acquire Postgres err=%v", err)
		return nil, err
	}

	log.Log.Debugf("Acquire Postgres (%p) database to %s: db=%p", pg, pg.dbURL, dbOpen)
	log.Log.Debugf("Opened postgres database")
	if dbOpen == nil {
		return nil, fmt.Errorf("error open handle and err nil")
	}
	p.IncUsage()
	return dbOpen, nil
}

// Open open the database connection
func (pg *PostGres) Open() (dbOpen any, err error) {
	if pg.openDB != nil {
		log.Log.Debugf("Already open pg=%p/db=%p", pg, pg.openDB)
		return pg.openDB, nil
	}
	db, err := pg.open()
	if err != nil {
		return nil, err
	}
	dbOpen = db
	pg.openDB = db

	if pg.IsTransaction() {
		// log.Log.Debugf("Lock open")
		// pg.txLock.Lock()
		// defer pg.txLock.Unlock()
		// defer log.Log.Debugf("Unlock open")
		pg.tx, err = db.Begin(context.Background())
		if err != nil {
			return nil, err
		}
		log.Log.Debugf("Tx started pg=%p/tx=%p", pg, pg.tx)

	}
	log.Log.Debugf("Opened database %s after transaction (pg=%p,db=%p)", pg.dbURL, pg, db)
	return db, nil
}

// BeginTransaction begin transaction the database connection
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
	if pg == nil || !pg.IsTransaction() {
		return nil
	}
	// log.Log.Debugf("Lock end transaction")
	// pg.txLock.Lock()
	// defer pg.txLock.Unlock()
	// defer log.Log.Debugf("Unlock end transaction")
	if pg.ctx == nil {
		return fmt.Errorf("error context not valid")
	}
	if pg.tx == nil {
		pg.Transaction = false
		return fmt.Errorf("error tx empty")
	}
	if commit {
		log.Log.Debugf("End transaction commiting ...(pg=%p/tx=%p) %v", pg, pg.tx, pg.IsTransaction())
		err = pg.tx.Commit(pg.ctx)
	} else {
		log.Log.Debugf("End transaction rollback ...(pg=%p/tx=%p) %v", pg, pg.tx, pg.IsTransaction())
		err = pg.tx.Rollback(pg.ctx)
	}
	log.Log.Debugf("Tx cleared pg=%p/tx=%p", pg, pg.tx)
	if pg.cancel != nil {
		pg.cancel()
	}
	pg.tx = nil
	pg.cancel = nil
	log.Log.Debugf("End transaction done: %v", err)
	pg.Transaction = false
	if err != nil {
		log.Log.Errorf("Error end transaction commit=%v: %v", commit, err)
	}
	return err
}

// Close close the database connection
func (pg *PostGres) Close() {
	// log.Log.Debugf("Lock close")
	// pg.txLock.Lock()
	// defer pg.txLock.Unlock()
	// defer log.Log.Debugf("Unlock close")
	if pg.ctx != nil {
		log.Log.Debugf("Rollback transaction during close %s", pg.ID())
		pg.EndTransaction(false)
	}
	if pg.openDB != nil {
		log.Log.Debugf("Close/release %p(pg=%p/tx=%p)", pg.openDB, pg, pg.tx)
		db := pg.openDB
		pg.openDB = nil
		pg.tx = nil
		pg.ctx = nil
		defer db.Release()
		log.Log.Debugf("Closing database done (pg=%p) %s", pg, pg.ID())
		if p, err := pg.getPool(); err == nil {
			used := p.DecUsage()
			log.Log.Debugf("Reduce database pool usage %d", used)
		}
		return
	}
	log.Log.Debugf("Close not opened database (pg=%p) %s", pg, pg.ID())
}

// FreeHandler don't use the driver anymore
func (pg *PostGres) FreeHandler() {
	if pg.openDB != nil {
		log.Log.Debugf("Free handler release entry %p(pg=%p/tx=%p)", pg.openDB, pg, pg.tx)
		db := pg.openDB
		defer db.Release()
		pg.openDB = nil
		pg.tx = nil
		pg.ctx = nil
	}
}

// Ping create short test database connection
func (pg *PostGres) Ping() error {
	log.Log.Debugf("Ping database ... by receiving table names")
	pg.dbTableNames = nil
	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}
	db := dbOpen.(*pgxpool.Conn)
	defer pg.Close()

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
		log.Log.Debugf("Tx used pg=%p/tx=%p", pg, pg.tx)
		tx = pg.tx
		ctx = pg.ctx
	}

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

	if !transaction {
		err = pg.EndTransaction(true)
		if err != nil {
			return -1, err
		}
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

	db := dbOpen.(*pgxpool.Conn)
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

	db := dbOpen.(*pgxpool.Conn)
	ctx := context.Background()
	defer pg.Close()
	selectCmd, err := search.Select()
	if err != nil {
		return nil, err
	}
	log.Log.Debugf("Query: %s (%p)", selectCmd, db)
	rows, err := db.Query(ctx, selectCmd)
	if err != nil {
		log.Log.Debugf("Query error: %v (%p)", err, db)
		if err.Error() == "conn busy" {
			pg.Close()
		}
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
		result.Header = append(result.Header, &common.Column{Name: f.Name,
			Length: uint16(f.DataTypeSize)})
	}
	currentCounter := uint64(0)
	for rows.Next() {
		currentCounter++
		result.Counter = currentCounter
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
	copy, values, scanValues, err := result.GenerateColumnByStruct(search)
	if err != nil {
		log.Log.Debugf("Error generating column: %v", err)
		return nil, err
	}
	log.Log.Debugf("Parse columns rows -> flen=%d vlen=%d %T scanVal=%d",
		len(result.Fields), len(values), copy, len(scanValues))
	for rows.Next() {
		result.Counter++
		log.Log.Debugf("Row found and scanning")
		if len(result.Fields) == 0 {
			for _, f := range rows.FieldDescriptions() {
				result.Fields = append(result.Fields, f.Name)
			}
		}
		err := rows.Scan(scanValues...)
		if err != nil {
			fmt.Println("Error scanning structs", values, err)
			log.Log.Debugf("Error during scan of struct: %v/%v", err, copy)
			return nil, err
		}
		err = common.ShiftValues(scanValues, values)
		if err != nil {
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
		log.Log.Debugf("Tx ended pg=%p/tx=%p", pg, pg.tx)

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
		log.Log.Debugf("Tx used pg=%p/tx=%p", pg, pg.tx)
		tx = pg.tx
		ctx = pg.ctx
	}
	if tx == nil {
		return 0, fmt.Errorf("nil internal error update")
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

	log.Log.Debugf("Calling batch " + batch)

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
func (pg *PostGres) BatchSelectFct(search *common.Query, fct common.ResultFunction) error {
	log.Log.Debugf("Query postgres database")
	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}

	db := dbOpen.(*pgxpool.Conn)
	ctx := context.Background()
	defer pg.Close()
	selectCmd := search.Search
	if err != nil {
		return err
	}
	log.Log.Debugf("Query: %s Parameters: %#v", selectCmd, search.Parameters)
	rows, err := db.Query(ctx, selectCmd, search.Parameters...)
	if err != nil {
		log.Log.Debugf("Query error: %v", err)
		return err
	}
	if search.DataStruct == nil {
		_, err = pg.ParseRows(search, rows, fct)
	} else {
		search.TypeInfo = common.CreateInterface(search.DataStruct, search.Fields)
		_, err = pg.ParseStruct(search, rows, fct)
	}
	return err
}

func (pg *PostGres) defineContext() {
	// pg.ctx, pg.cancel = context.WithTimeout(context.Background(), 120*time.Second)
	pg.ctx = context.Background()
}

// StartTransaction start transaction
func (pg *PostGres) StartTransaction() (pgx.Tx, context.Context, error) {
	var err error
	if pg.openDB == nil {
		log.Log.Debugf("Open with transaction enabled")
		pg.openDB, err = pg.open()
		if err != nil {
			log.Log.Debugf("Error opening connection for transaction")
			return nil, nil, err
		}
	}
	// log.Log.Debugf("Lock Start transaction opened")
	// pg.txLock.Lock()
	// defer pg.txLock.Unlock()
	// defer log.Log.Debugf("Unlock Start transaction opened")
	pg.defineContext()
	if pg.openDB == nil || pg == nil || pg.ctx == nil {
		log.Log.Fatalf("Error invalid openDB handle")
	}
	pg.tx, err = pg.openDB.Begin(pg.ctx)
	if err != nil {
		pg.ctx = nil
		pg.tx = nil
		log.Log.Debugf("Begin of transaction fails: %v", err)
		return nil, nil, err
	}
	log.Log.Debugf("Start transaction begin (pg=%p/tx=%p)", pg, pg.tx)
	pg.Transaction = true
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
	conn := dbOpen.(*pgxpool.Conn)
	defer pg.Close()

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
