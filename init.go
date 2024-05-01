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

package flynn

import (
	"os"
	"sync/atomic"

	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/adabas"
	"github.com/tknie/flynn/common"
	"github.com/tknie/flynn/mysql"
	"github.com/tknie/flynn/oracle"
	"github.com/tknie/flynn/postgres"
	"github.com/tknie/log"
)

var globalRegID = common.RegDbID(0)

// Handler Handle database driver with a database URL returning a
// reference id for the driver path to database
func Handle(p ...string) (common.RegDbID, error) {
	l := len(p) - 1
	if l < 0 {
		return 0, errorrepo.NewError("DB000012")
	}
	log.Log.Debugf("Register %v", p[0])
	r, passwd, err := common.NewReference(p[l])
	if err != nil {
		return 0, err
	}
	if l > 1 {
		r.SetType(p[0])
	}
	if r.Driver == common.NoType {
		return 0, errorrepo.NewError("DB000013")
	}
	return Handler(r, passwd)
}

// Handler Register database driver with a database URL returning a
// reference id for the driver path to database
func Handler(dbref *common.Reference, password string) (common.RegDbID, error) {
	if dbref == nil {
		return 0, errorrepo.NewError("DB000014")
	}
	id := common.RegDbID(atomic.AddUint64((*uint64)(&globalRegID), 1))

	if log.IsDebugLevel() {
		p := "*******"
		if os.Getenv("FLYNN_TRACE_PASSWORD") == "TRUE" {
			p = password
		}
		log.Log.Debugf("Register database with passwordx %s", p)
	}
	var db common.Database
	var err error
	switch dbref.Driver {
	case common.PostgresType:
		db, err = postgres.NewInstance(id, dbref, password)
	case common.MysqlType:
		db, err = mysql.NewInstance(id, dbref, password)
	case common.AdabasType:
		db, err = adabas.NewInstance(id, dbref, password)
	case common.OracleType:
		db, err = oracle.NewInstance(id, dbref, password)
	default:
		return 0, errorrepo.NewError("DB065535")
	}
	if err != nil {
		return 0, err
	}
	common.RegisterDbClient(db)
	log.Log.Debugf("%s Register db type on db=%p driver=%d,len=%d: %v", db.ID().String(),
		db, dbref.Driver, len(common.Databases), common.DBHelper())
	return db.ID(), nil
}

// Maps database tables,views and/or maps usable for queries
func Maps() []string {
	databaseMaps := make([]string, 0)
	for _, db := range common.Databases {
		database := db.Clone()
		log.Log.Debugf("Map found " + database.URL())
		subMaps, err := database.Maps()
		if err != nil {
			log.Log.Errorf("%s Error reading sub maps: %v", database.ID().String(), err)
			continue
		}
		databaseMaps = append(databaseMaps, subMaps...)
	}
	return databaseMaps
}
