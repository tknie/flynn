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

package db

import (
	"sync/atomic"

	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/adabas"
	def "github.com/tknie/flynn/common"
	"github.com/tknie/flynn/mysql"
	"github.com/tknie/flynn/postgres"
	"github.com/tknie/log"
)

var globalRegID = def.RegDbID(0)

// Register database driver with a database URL returning a
// reference id for the driver path to database
func Register(typeName, url string) (def.RegDbID, error) {
	id := def.RegDbID(atomic.AddUint64((*uint64)(&globalRegID), 1))

	var db def.Database
	var err error
	switch typeName {
	case "postgres":
		db, err = postgres.New(id, url)
	case "mysql":
		db, err = mysql.New(id, url)
	case "adabas":
		db, err = adabas.New(id, url)
	default:
		return 0, errorrepo.NewError("DB065535")
	}
	if err != nil {
		return 0, err
	}
	def.Databases = append(def.Databases, db)
	return db.ID(), nil
}

// Maps database tables,views and/or maps usable for queries
func Maps() []string {
	databaseMaps := make([]string, 0)
	for _, database := range def.Databases {
		log.Log.Debugf(database.URL())
		subMaps, err := database.Maps()
		if err != nil {
			log.Log.Errorf("Error reading sub maps: %v", err)
			continue
		}
		databaseMaps = append(databaseMaps, subMaps...)
	}
	return databaseMaps
}

// Unregister unregister registry id for the driver
func Unregister(id def.RegDbID) error {
	for i, d := range def.Databases {
		if d.ID() == id {
			newDatabases := make([]def.Database, 0)
			if i > 0 {
				newDatabases = append(newDatabases, def.Databases[0:i-1]...)
			}
			if len(def.Databases)-1 > i {
				newDatabases = append(newDatabases, def.Databases[i+1:]...)
			}
			def.Databases = newDatabases
			return nil
		}
	}
	return errorrepo.NewError("DB000001")
}
