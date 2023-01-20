/*
* Copyright 2022 Thorsten A. Knieling
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
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/tknie/flynn/common"
	def "github.com/tknie/flynn/common"
	"github.com/tknie/flynn/dbsql"
	"github.com/tknie/log"
)

type PostGres struct {
	def.CommonDatabase
	openDB       any
	dbURL        string
	dbTableNames []string
	user         string
	password     string
}

func New(id def.RegDbID, url string) (def.Database, error) {
	pg := &PostGres{def.CommonDatabase{RegDbID: id}, nil,
		url, nil, "", ""}
	// err := pg.check()
	// if err != nil {
	// 	return nil, err
	// }
	return pg, nil
}

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

func (pg *PostGres) Reference() (string, string) {
	return "pgx", pg.dbURL
}

func (pg *PostGres) IndexNeeded() bool {
	return true
}

func (pg *PostGres) ByteArrayAvailable() bool {
	return true
}

func (pg *PostGres) ID() def.RegDbID {
	return pg.RegDbID
}

func (pg *PostGres) URL() string {
	return pg.dbURL
}
func (pg *PostGres) Maps() ([]string, error) {
	if pg.dbTableNames == nil {
		err := pg.Ping()
		if err != nil {
			return nil, err
		}
	}
	return pg.dbTableNames, nil
}

func (pg *PostGres) Open() (dbOpen any, err error) {
	var db *sql.DB
	if !pg.IsTransaction() || pg.openDB == nil {
		db, err = sql.Open("pgx", pg.generateURL())
		if err != nil {
			return
		}
		pg.openDB = db
	} else {
		db = pg.openDB.(*sql.DB)
	}
	log.Log.Debugf("Open database %s", pg.dbURL)
	return db, nil
}

func (pg *PostGres) Close() {
	if pg.openDB != nil {
		pg.openDB.(*sql.DB).Close()
		pg.openDB = nil
		log.Log.Debugf("Close database")
	} else {
		log.Log.Debugf("Close not opened database")
	}
}

func (pg *PostGres) Ping() error {

	dbOpen, err := pg.Open()
	if err != nil {
		return err
	}
	defer pg.Close()

	db := dbOpen.(*sql.DB)

	pg.dbTableNames = make([]string, 0)

	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
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

func (pg *PostGres) Delete(name string, remove *def.Entries) error {
	return dbsql.Delete(pg, name, remove)
}

func (pg *PostGres) GetTableColumn(tableName string) ([]string, error) {
	dbOpen, err := pg.Open()
	if err != nil {
		return nil, err
	}
	defer pg.Close()

	db := dbOpen.(*sql.DB)
	// rows, err := db.Query(`SELECT table_schema, table_name, column_name, data_type
	// FROM INFORMATION_SCHEMA.COLUMNS
	rows, err := db.Query(`SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '` + strings.ToLower(tableName) + `'`)
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

func (pg *PostGres) Query(search *def.Query, f def.ResultFunction) (*common.Result, error) {
	log.Log.Debugf("Query postgres database")
	dbOpen, err := pg.Open()
	if err != nil {
		return nil, err
	}
	defer pg.Close()

	db := dbOpen.(*sql.DB)
	selectCmd := search.Select()

	log.Log.Debugf("Query: %s", selectCmd)
	rows, err := db.Query(selectCmd)
	if err != nil {
		return nil, err
	}
	if search.DataStruct == nil {
		return search.ParseRows(rows, f)
	}
	return search.ParseStruct(rows, f)
}

func (pg *PostGres) CreateTable(name string, columns any) error {
	return dbsql.CreateTable(pg, name, columns)
}

func (pg *PostGres) DeleteTable(name string) error {
	return dbsql.DeleteTable(pg, name)
}

func (pg *PostGres) Insert(name string, insert *def.Entries) error {
	return dbsql.Insert(pg, name, insert)
}

func (pg *PostGres) Update(name string, insert *def.Entries) error {
	return dbsql.Update(pg, name, insert)
}

func (pg *PostGres) BatchSQL(batch string) error {
	return dbsql.BatchSQL(pg, batch)
}
