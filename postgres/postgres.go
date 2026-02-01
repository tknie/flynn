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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tknie/errorrepo"
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
	openDB       *pgxpool.Conn
	dbTableNames []string
	password     string
	tx           pgx.Tx
	ctx          context.Context
	cancel       context.CancelFunc
	lock         sync.Mutex
}

type pool struct {
	useCounter uint64
	pool       *pgxpool.Pool
	ctx        context.Context
	url        string
	lock       sync.Mutex
}

var poolMap sync.Map
var postgresPool sync.Pool = sync.Pool{New: PostgresNew}

// var poolLock sync.Mutex

func (p *pool) IncUsage() uint64 {
	used := atomic.AddUint64(&p.useCounter, 1)
	log.Log.Debugf("Inc usage = %d", used)
	return used
}

func (p *pool) DecUsage() uint64 {
	p.lock.Lock()
	defer p.lock.Unlock()
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

func PostgresNew() any {
	return &PostGres{common.CommonDatabase{}, nil,
		nil, "", nil, nil, nil, sync.Mutex{}}
}

func (pg *PostGres) reset() {
	pg.CommonDatabase.RegDbID = 0
	pg.openDB = nil
	pg.password = ""
	pg.lock = sync.Mutex{}
}

// NewInstance create new postgres reference instance using reference structure
func NewInstance(id common.RegDbID, reference *common.Reference, password string) (common.Database, error) {
	pg := postgresPool.Get().(*PostGres)
	pg.CommonDatabase = common.NewCommonDatabase(id, "postgres")
	pg.password = password
	pg.ConRef = reference
	log.Log.Debugf("PG Password is empty=%v", password == "")
	return pg, nil
}

func (pg *PostGres) Clone() common.Database {
	newPg := postgresPool.Get().(*PostGres)
	*newPg = *pg
	newPg.ctx = nil
	newPg.openDB = nil
	newPg.tx = nil
	newPg.lock = sync.Mutex{}
	return newPg
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
	pg.ConRef.User = user
	pg.password = password
	log.Log.Debugf("Store credentials")
	return nil
}

func (pg *PostGres) generateURL() string {
	reference := pg.ConRef
	user := pg.ConRef.User
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s%s", user, pg.password,
		reference.Host, reference.Port, reference.Database, reference.OptionString())
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
	reference := pg.ConRef
	return fmt.Sprintf("postgres://%s:"+passwdPlaceholder+"@%s:%d/%s%s", reference.User,
		reference.Host, reference.Port, reference.Database, reference.OptionString())
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
		log.Log.Debugf("%s pool entry found", pg.ID().String())
		pl := p.(*pool)
		pl.lock.Lock()
		defer pl.lock.Unlock()
		if pl.pool == nil {
			config, err := pgxpool.ParseConfig(pg.generateURL())
			if err != nil {
				log.Log.Debugf("Error parsing url: %s", pg.URL())
				return nil, err
			}
			pg.defineContext()
			if pg.ctx == nil {
				return nil, errorrepo.NewError("DB000023")
			}
			pl.pool, err = pgxpool.NewWithConfig(pg.ctx, config)
			if err != nil {
				return nil, err
			}
			log.Log.Debugf("%s pool entry recreated", pg.ID().String())
		} else {
			log.Log.Debugf("%s pool entry found", pg.ID().String())
		}
		pl.IncUsage()

		log.Log.Debugf("%s Pool use counter %d", pg.ID().String(), pl.useCounter)
		pg.ctx = pl.ctx
		return pl, nil
	} else {
		log.Log.Debugf("%s pool entry not found", pg.ID().String())
		pg.defineContext()
		if pg.ctx == nil {
			return nil, errorrepo.NewError("DB000024")
		}
		//config := &pgx.ConnConfig{Tracer: tracer}

		// Increase max conns because of Deadlock
		//config, err := pgxpool.ParseConfig(pg.generateURL() + "?pool_max_conns=100")
		config, err := pgxpool.ParseConfig(pg.generateURL())
		if err != nil {
			log.Log.Debugf("Error parsing url: %s", pg.URL())
			return nil, err
		}
		// config.Tracer = tracer
		// pg.ctx = context.Background()
		log.Log.Debugf("%s Create pool for Postgres database to %s", pg.ID().String(), pg.URL())
		p := &pool{url: url, ctx: pg.ctx}
		p.lock.Lock()
		defer p.lock.Unlock()
		p.pool, err = pgxpool.NewWithConfig(pg.ctx, config)
		if err != nil {
			log.Log.Debugf("Postgres driver connect error: %v", err)
			return nil, err
		}

		poolMap.Store(url, p)

		p.IncUsage()
		return p, nil
	}

	// TODO: handle timeout
	// ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cancelfunc()
	// conn, err := di.db.Conn(ctx)
	// if err != nil {
	// 	return 0, err
	// }
	// conn.Raw(func(driverConn any) error {
	// 	c := driverConn.(*stdlib.Conn).Conn()
	// 	pgxdecimal.Register(c.TypeMap())
	// 	return nil
	// })

}

func (pg *PostGres) open() (dbOpen *pgxpool.Conn, err error) {
	log.Log.Debugf("%s Open pool Lock", pg.ID().String())
	pg.lock.Lock()
	defer pg.lock.Unlock()
	defer log.Log.Debugf("%s Open pool Unlock", pg.ID().String())
	if pg.IsTransaction() && pg.openDB != nil {
		return pg.openDB, nil
	}

	p, err := pg.getPool()
	if err != nil {
		return nil, err
	}
	if p == nil {
		log.Log.Fatalf("Fatal error p=%v defined", (p == nil))
	}
	if p.pool == nil {
		log.Log.Fatalf("%s p.pool=%v defined", pg.ID().String(), p.pool == nil)
	}
	/*tracer := &tracelog.TraceLog{
		Logger:   NewLogger(),
		LogLevel: tracelog.LogLevelTrace,
	}*/
	log.Log.Debugf("%s Acquire Postgres to pool=%p %v", pg.ID().String(), p.pool, pg.ID())
	dbOpen, err = p.pool.Acquire(pg.ctx)
	if err != nil {
		log.Log.Debugf("Acquire Postgres err=%v", err)
		return nil, err
	}

	log.Log.Debugf("%s Acquire Postgres (%p) database to %s: db=%p", pg.ID().String(), pg, pg.URL(), dbOpen)
	log.Log.Debugf("%s Opened postgres database", pg.ID().String())
	if dbOpen == nil {
		return nil, errorrepo.NewError("DB000025")
	}
	return dbOpen, nil
}

// Open open the database connection
func (pg *PostGres) Open() (dbOpen any, err error) {
	if pg.openDB != nil {
		log.Log.Debugf("%s Already open pg=%p/db=%p", pg.ID().String(), pg, pg.openDB)
		return pg.openDB, nil
	}
	db, err := pg.open()
	if err != nil {
		return nil, err
	}
	dbOpen = db
	pg.openDB = db

	if pg.IsTransaction() {
		pg.tx, err = db.Begin(context.Background())
		if err != nil {
			return nil, err
		}
		log.Log.Debugf("Tx started pg=%p/tx=%p", pg, pg.tx)

	}
	log.Log.Debugf("%s Opened database %s after transaction (pg=%p,db=%p)", pg.ID().String(),
		pg.URL(), pg, db)
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
	log.Log.Debugf("%s Start end transaction", pg.ID().String())
	if pg.ctx == nil {
		return errorrepo.NewError("DB000026")
	}
	if pg.tx == nil {
		pg.Transaction = false
		return errorrepo.NewError("DB000027")
	}
	if commit {
		log.Log.Debugf("%s End transaction commiting ...(pg=%p/tx=%p) %v", pg.ID().String(), pg, pg.tx, pg.IsTransaction())
		err = pg.tx.Commit(pg.ctx)
	} else {
		log.Log.Debugf("%s End transaction rollback ...(pg=%p/tx=%p) %v", pg.ID().String(), pg, pg.tx, pg.IsTransaction())
		err = pg.tx.Rollback(pg.ctx)
	}
	log.Log.Debugf("Tx cleared pg=%p/tx=%p", pg, pg.tx)
	if pg.cancel != nil {
		pg.cancel()
	}
	pg.tx = nil
	pg.cancel = nil
	log.Log.Debugf("%s End transaction done: %v", pg.ID().String(), err)
	pg.Transaction = false
	if err != nil {
		log.Log.Errorf("Error end transaction commit=%v: %v", commit, err)
	}
	return err
}

// Close close the database connection
func (pg *PostGres) Close() {
	log.Log.Debugf("%s Close of connection", pg.ID().String())
	if pg.ctx != nil {
		log.Log.Debugf("%s Rollback transaction during close", pg.ID().String())
		pg.EndTransaction(false)
	}
	pg.lock.Lock()
	defer pg.lock.Unlock()
	if pg.openDB != nil {

		log.Log.Debugf("%s Close/release %p(pg=%p/tx=%p)", pg.ID().String(), pg.openDB, pg, pg.tx)
		db := pg.openDB
		pg.openDB = nil
		pg.tx = nil
		pg.ctx = nil
		if db != nil {
			db.Release()
		}
		log.Log.Debugf("%s Released database done (pg=%p)", pg.ID().String(), pg)
		if p, err := pg.getPool(); err == nil {
			used := p.DecUsage()
			log.Log.Debugf("Reduce database pool usage %d", used)
		}
		return
	}
	log.Log.Debugf("%s Close not opened database (pg=%p)", pg.ID().String(), pg)
}

// FreeHandler don't use the driver anymore
func (pg *PostGres) FreeHandler() {
	log.Log.Debugf("%s free postgres handler", pg.ID().String())
	pg.lock.Lock()
	defer postgresPool.Put(pg)
	defer pg.reset()
	defer pg.lock.Unlock()
	if pg.openDB != nil {
		log.Log.Debugf("%s Free handler release entry %p(pg=%p/tx=%p)", pg.ID().String(), pg.openDB, pg, pg.tx)
		db := pg.openDB
		db.Release()
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
		log.Log.Debugf("%s Error pinging database ...%v", pg.ID().String(), err)
		return err
	}
	defer rows.Close()
	tableName := ""
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			log.Log.Debugf("%s Error pinging and scan database ...%v", pg.ID().String(), err)
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
		log.Log.Debugf("Delete cmd: %s", deleteCmd)
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
		log.Log.Debugf("Delete not in transaction and commit data to database")
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
	defer rows.Close()
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
	search.Driver = common.PostgresType
	log.Log.Debugf("%s Query of postgres database", pg.ID().String())
	defer log.Log.Debugf("%s Query ended for postgres database", pg.ID().String())
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
	log.Log.Debugf("Postgres Query: %s (%p)", selectCmd, db)
	startTime := time.Now()
	rows, err := db.Query(ctx, selectCmd)
	used := time.Since(startTime)
	if err != nil {
		log.Log.Infof("Postgres Query error (%v): %v (%p)", used, err, db)
		if err.Error() == "conn busy" {
			pg.Close()
		}
		return nil, err
	}
	log.Log.Debugf("Postgres Query used %v", used)
	defer rows.Close()
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
	log.Log.Debugf("Go through rows ... fields=%d header=%d desc=%d",
		len(result.Fields), len(result.Header), len(rows.FieldDescriptions()))
	currentCounter := uint64(0)
	for rows.Next() {
		currentCounter++
		result.Counter = currentCounter
		log.Log.Debugf("Checking row %d...", currentCounter)

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
	log.Log.Debugf("Parse struct .... started")
	result = &common.Result{}
	result.Data = search.DataStruct
	vd, err := result.GenerateColumnByStruct(search)
	if err != nil {
		log.Log.Debugf("Error generating column: %v", err)
		return nil, err
	}
	log.Log.Debugf("Parse columns rows -> flen=%d vlen=%d %T scanVal=%d",
		len(result.Fields), len(vd.Values), vd.Copy, len(vd.ScanValues))
	for rows.Next() {
		result.Counter++
		log.Log.Debugf("%d. row found and scanning with %#v", result.Counter, vd.ScanValues)
		if len(result.Fields) == 0 {
			for _, f := range rows.FieldDescriptions() {
				result.Fields = append(result.Fields, f.Name)
			}
			log.Log.Debugf("Fields: %v", result.Fields)
		}
		err := rows.Scan(vd.ScanValues...)
		if err != nil {
			log.Log.Debugf("Error during parse of struct: %v/%v", err, vd.Copy)
			debug.PrintStack()
			return nil, err
		}
		err = vd.ShiftValues()
		if err != nil {
			log.Log.Debugf("Error during shift values of struct: %v/%v", err, vd.Copy)
			return nil, err
		}
		result.Data = vd.Copy
		err = f(search, result)
		if err != nil {
			log.Log.Debugf("Error in function call: %v/%v", err, vd.Copy)
			return nil, err
		}
	}
	log.Log.Debugf("Parse of structure finished, counter=%d", result.Counter)
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
	case map[string]interface{}:
		createCmd += dbsql.CreateTableByMaps(pg.ByteArrayAvailable(), columns)
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
	// err = db.Close()
	// if err != nil {
	// 	return err
	// }
	log.Log.Debugf("Table db handler closed")
	return nil
}

// AdaptTable adapt a new table
func (pg *PostGres) AdaptTable(name string, col any) error {
	log.Log.Debugf("%s: Adapt SQL table of type %T", pg.ID(), col)
	layer, url := pg.Reference()
	db, err := sql.Open(layer, url)
	if err != nil {
		return err
	}
	defer db.Close()

	if columns, ok := col.([]*common.Column); ok {
		log.Log.Debugf("%s: Found %d SQL columns", pg.ID(), len(columns))
		var buffer bytes.Buffer

		buffer.WriteString(`ALTER TABLE ` + name)
		i := 0
		for _, c := range columns {
			if i > 0 {
				buffer.WriteString(",")
			}
			i++
			buffer.WriteString(` ADD COLUMN `)
			dbsql.CreateTableByColumn(&buffer, pg.ByteArrayAvailable(), c)
		}
		log.Log.Debugf(buffer.String())
		_, err = db.Query(buffer.String())
		if err != nil {
			log.Log.Errorf("Error returned by SQL: %v", err)
			return err
		}
		return nil
	}
	columnCurrent, err := pg.ID().GetTableColumn(name)
	if err != nil {
		return err
	}
	log.Log.Debugf("Got struct and new columns: %v", columnCurrent)
	columStruct, err := dbsql.SqlDataType(false, col, columnCurrent)
	if err != nil {
		return err
	}
	var buffer bytes.Buffer
	buffer.WriteString(`ALTER TABLE ` + name)
	i := 0
	for _, f := range strings.Split(columStruct, ",") {
		if i > 0 {
			buffer.WriteString(",")
		}
		i++
		buffer.WriteString(` ADD COLUMN ` + f)
	}

	_, err = db.Query(buffer.String())
	if err != nil {
		log.Log.Errorf("Error returned by SQL: %v", err)
		return err
	}
	log.Log.Debugf("Table adapted")
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

	log.Log.Debugf("Init DROP TABLE %s", name)
	_, err = db.Query("DROP TABLE " + name)
	if err != nil {
		log.Log.Debugf("DROP TABLE error: %v", err)
		return err
	}
	log.Log.Debugf("Drop table " + name)
	return nil
}

// Insert insert record into table
func (pg *PostGres) Insert(name string, insert *common.Entries) (returning [][]any, err error) {
	log.Log.Debugf("%s: Insert in posgres database", pg.ID().String())
	if insert == nil || len(insert.Values) == 0 {
		return nil, errorrepo.NewError("DB000029")
	}
	defer log.Log.Debugf("%s: Insert ended for posgres database", pg.ID().String())

	var ctx context.Context
	var tx pgx.Tx

	transaction := pg.IsTransaction()
	log.Log.Debugf("%s Transaction (begin insert): %v", pg.ID().String(), transaction)
	if !transaction {
		tx, ctx, err = pg.StartTransaction()
		if err != nil {
			log.Log.Debugf("%s Error start transaction: %v", pg.ID().String(), err)
			return nil, err
		}
		// defer pg.Close()
	} else {
		log.Log.Debugf("%s Tx ended pg=%p/tx=%p", pg.ID().String(), pg, pg.tx)

		tx = pg.tx
		ctx = pg.ctx
	}
	if tx == nil || ctx == nil {
		log.Log.Debugf("Error context transaction")
		return nil, errorrepo.NewError("DB000028", pg, tx, ctx)
	}
	if !pg.IsTransaction() {
		log.Log.Debugf("%s: Init defer close ... in inserting", pg.ID().String())
		pg.Close()
		return nil, errorrepo.NewError("DB000029")
	}

	log.Log.Debugf("%s Insert SQL record", pg.ID().String())

	insertCmd := "INSERT INTO " + name + " ("
	values := "("

	indexNeed := pg.IndexNeeded()
	var insertValues [][]any
	var insertFields []string
	_, isMap := insert.Values[0][0].(map[string]interface{})
	switch {
	case insert.DataStruct != nil:
		insertFields, insertValues, err = createDynamic(insert)
		if err != nil {
			return nil, err
		}

	case isMap:
		insertFields, insertValues, err = createMaps(insert)
		if err != nil {
			return nil, err
		}
	default:
		insertFields = insert.Fields
		insertValues = insert.Values
	}

	log.Log.Debugf("Final values: %#v", insertValues)
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
	if len(insert.Returning) > 0 {
		insertCmd += " RETURNING "
		for i, r := range insert.Returning {
			if i > 0 {
				insertCmd += ","
			}
			insertCmd += r
		}
	}
	log.Log.Debugf("%s Insert pre-CMD: %s", pg.ID().String(), insertCmd)
	returning = make([][]any, 0)
	for _, v := range insertValues {
		av := v
		log.Log.Debugf("%s Insert values: %d -> %#v", pg.ID().String(), len(av), av)
		if len(insert.Returning) > 0 {
			row := tx.QueryRow(ctx, insertCmd, av...)
			if insert.DataStruct != nil {
				log.Log.Debugf("Use data struct for returning")
				rv, err := scanStruct(row, insert)
				if err != nil {
					trErr := pg.EndTransaction(false)
					log.Log.Debugf("Error insert CMD: %v of %s and cmd %s trErr=%v",
						err, name, insertCmd, trErr)
					return nil, err
				}
				returning = append(returning, rv)
			} else {
				rv, err := scanRow(row, len(insert.Returning))
				if err != nil {
					trErr := pg.EndTransaction(false)
					log.Log.Debugf("Error insert CMD: %v of %s and cmd %s trErr=%v",
						err, name, insertCmd, trErr)
					return nil, err
				}
				returning = append(returning, rv)
			}
		} else {
			res, err := tx.Exec(ctx, insertCmd, av...)
			if err != nil {
				trErr := pg.EndTransaction(false)
				log.Log.Debugf("Error insert CMD: %v of %s and cmd %s trErr=%v",
					err, name, insertCmd, trErr)
				return nil, err
			}
			l := res.RowsAffected()
			if l == 0 {
				return nil, errorrepo.NewError("DB000030")
			}
		}
	}

	if !transaction {
		log.Log.Debugf("%s Need to end because not in Transaction: %v", pg.ID().String(), pg.IsTransaction())
		err = pg.EndTransaction(true)
		if err != nil {
			log.Log.Debugf("Error transaction %v", err)
			return nil, err
		}
		pg.Close()
	}
	return returning, nil
}

func createDynamic(insert *common.Entries) ([]string, [][]any, error) {
	insertValues := make([][]any, 0)
	dynamic := common.CreateInterface(insert.DataStruct, insert.Fields)
	insertFields := dynamic.RowFields
	for _, vi := range insert.Values {
		v, err := dynamic.CreateValues(vi[0])
		if err != nil {
			return nil, nil, err
		}
		log.Log.Debugf("Row   fields: %#v", insertFields)
		log.Log.Debugf("Value fields: %#v", insertValues)
		insertValues = append(insertValues, v)
	}
	log.Log.Debugf("Final number records %d", len(insertValues))
	return insertFields, insertValues, nil
}

func createMaps(insert *common.Entries) ([]string, [][]any, error) {
	insertFields := insert.Fields
	insertValues := make([][]any, 0)
	if slices.Contains(insert.Fields, "*") {
		insertFields = make([]string, 0)
		for n, _ := range insert.Values[0][0].(map[string]interface{}) {
			insertFields = append(insertFields, n)
		}
	}
	for _, vals := range insert.Values[0] {
		m := vals.(map[string]interface{})
		rv := make([]any, 0)
		for _, f := range insertFields {
			if slices.Contains(insert.Fields, f) {
				if v, ok := m[f]; ok {
					rv = append(rv, v)
				} else {
					rv = append(rv, nil)
				}
			}
		}
		insertValues = append(insertValues, rv)
	}
	return insertFields, insertValues, nil
}

func scanRow(row pgx.Row, cols int) ([]any, error) {
	scanData := make([]interface{}, 0)
	for i := 0; i < cols; i++ {
		id := ""
		scanData = append(scanData, &id)
	}
	err := row.Scan(scanData...)
	if err != nil {
		return nil, err
	}
	rv := make([]any, 0)
	for _, sd := range scanData {
		rv = append(rv, *sd.(*string))
	}
	return rv, nil
}

func scanStruct(row pgx.Row, insert *common.Entries) ([]any, error) {
	typeInfo := common.CreateInterface(insert.DataStruct, insert.Returning)
	// copy, values, scanValues := typeInfo.CreateQueryValues()
	vd, err := typeInfo.CreateQueryValues()
	if err != nil {
		log.Log.Debugf("Error during value query: %v", err)
		return nil, err
	}
	log.Log.Debugf("Parse columns row -> flen=%d vlen=%d %T scanVal=%d",
		len(insert.Returning), len(vd.Values), vd.Copy, len(vd.ScanValues))
	err = row.Scan(vd.ScanValues...)
	if err != nil {
		log.Log.Debugf("Error during scan of struct: %v/%v", err, vd.Copy)
		return nil, err
	}
	log.Log.Debugf("Scan values %#v", vd.ScanValues)
	err = vd.ShiftValues()
	if err != nil {
		return nil, err
	}
	log.Log.Debugf("Returning: %#v", vd.Copy)
	rv := make([]any, 0)
	rv = append(rv, vd.Copy)
	return rv, nil
}

// Update update record in table
func (pg *PostGres) Update(name string, updateInfo *common.Entries) (returning [][]any, rowsAffected int64, err error) {
	log.Log.Debugf("%s: Update in posgres database", pg.ID().String())
	defer log.Log.Debugf("%s: Update ended for posgres database", pg.ID().String())
	transaction := pg.IsTransaction()
	var ctx context.Context
	var tx pgx.Tx
	if !transaction {
		tx, ctx, err = pg.StartTransaction()
		if err != nil {
			return nil, -1, err
		}
		defer pg.Close()
	} else {
		log.Log.Debugf("Tx used pg=%p/tx=%p", pg, pg.tx)
		tx = pg.tx
		ctx = pg.ctx
	}
	if tx == nil {
		return nil, 0, errorrepo.NewError("DB000031")
	}
	var insertFields []string
	var updateValues [][]any
	if updateInfo.DataStruct != nil {
		updateValues = make([][]any, 0)
		dynamic := common.CreateInterface(updateInfo.DataStruct, updateInfo.Fields)
		insertFields = dynamic.RowFields
		for _, vi := range updateInfo.Values {
			v, err := dynamic.CreateValues(vi[0])
			if err != nil {
				return nil, -1, err
			}
			updateValues = append(updateValues, v)
			log.Log.Debugf("Row   fields: %#v", insertFields)
			log.Log.Debugf("Value fields: %#v", updateValues)
		}
	} else {
		updateValues = updateInfo.Values
	}
	updateCmd, whereFields := dbsql.GenerateUpdate(pg.IndexNeeded(), name, updateInfo)
	if len(updateInfo.Returning) > 0 {
		updateCmd += " RETURNING "
		for i, r := range updateInfo.Returning {
			if i > 0 {
				updateCmd += ","
			}
			updateCmd += r
		}
	}

	returning = make([][]any, 0)
	for i, v := range updateValues {
		av := v
		whereClause := dbsql.CreateWhere(i, updateInfo, whereFields)
		ic := updateCmd + whereClause
		log.Log.Debugf("Update call: %s", ic)
		log.Log.Debugf("Update values: %d -> %#v tx=%v %v", len(v), v, tx, ctx)
		if len(updateInfo.Returning) > 0 {
			row := tx.QueryRow(ctx, updateCmd, av...)
			if updateInfo.DataStruct != nil {
				log.Log.Debugf("Use data struct for returning")
				rv, err := scanStruct(row, updateInfo)
				if err != nil {
					trErr := pg.EndTransaction(false)
					log.Log.Debugf("Error insert CMD: %v of %s and cmd %s trErr=%v",
						err, name, updateCmd, trErr)
					return nil, 0, err
				}
				returning = append(returning, rv)
			} else {
				rv, err := scanRow(row, len(updateInfo.Returning))
				if err != nil {
					trErr := pg.EndTransaction(false)
					log.Log.Debugf("Error insert CMD: %v of %s and cmd %s trErr=%v",
						err, name, updateCmd, trErr)
					return nil, 0, err
				}
				returning = append(returning, rv)
			}
		} else {
			res, err := tx.Exec(ctx, ic, v...)
			if err != nil {
				log.Log.Debugf("Update error: %s -> %v", ic, err)
				pg.EndTransaction(false)
				return nil, 0, err
			}
			rowsAffected += res.RowsAffected()
		}
		log.Log.Debugf("Rows affected %d", rowsAffected)
	}
	log.Log.Debugf("Update done")

	if !transaction {
		err = pg.EndTransaction(true)
		if err != nil {
			return nil, -1, err
		}
	}
	return returning, rowsAffected, nil
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
	defer rows.Close()
	for rows.Next() {
		if rows.Err() != nil {
			log.Log.Debugf("Batch SQL error: %v", rows.Err())
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
	defer rows.Close()
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	result := make([][]interface{}, 0)
	for rows.Next() {
		if rows.Err() != nil {
			log.Log.Errorf("Batch SQL error: %v", rows.Err())
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
	log.Log.Debugf("%s: Query posgres database", pg.ID().String())
	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}

	db := dbOpen.(*pgxpool.Conn)
	ctx := context.Background()
	defer pg.Close()
	selectCmd := search.Search
	if selectCmd == "" {
		return errorrepo.NewError("DB000034")
	}
	log.Log.Debugf("%s: Query: %s Parameters: %#v", pg.ID().String(), selectCmd, search.Parameters)
	rows, err := db.Query(ctx, selectCmd, search.Parameters...)
	if err != nil {
		log.Log.Debugf("%s: Query error: %v", pg.ID().String(), err)
		return err
	}
	log.Log.Debugf("%s: Query executed", pg.ID().String())
	defer rows.Close()
	if search.DataStruct == nil {
		_, err = pg.ParseRows(search, rows, fct)
	} else {
		search.TypeInfo = common.CreateInterface(search.DataStruct, search.Fields)
		_, err = pg.ParseStruct(search, rows, fct)
	}
	log.Log.Debugf("%s: Query parsed", pg.ID().String())

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
		log.Log.Debugf("%s Open with transaction enabled", pg.ID().String())
		pg.openDB, err = pg.open()
		if err != nil {
			log.Log.Debugf("Error opening connection for transaction: %v", err)
			return nil, nil, err
		}
	}
	log.Log.Debugf("%s Start transaction opened", pg.ID().String())
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
	log.Log.Debugf("%s Start transaction begin (pg=%p/tx=%p)", pg.ID().String(), pg, pg.tx)
	pg.Transaction = true
	return pg.tx, pg.ctx, nil
}

// Commit commit the transaction
func (pg *PostGres) Commit() error {
	log.Log.Debugf("%s Commit transaction", pg.ID().String())
	return pg.EndTransaction(true)
}

// Rollback rollback the transaction
func (pg *PostGres) Rollback() error {
	log.Log.Debugf("%s Rollback transaction", pg.ID().String())
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

	log.Log.Debugf("%s Start stream for %s for %s", pg.ID().String(), search.Fields[0], search.TableName)
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
		rows.Close()
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
