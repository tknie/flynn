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

package flynn

import (
	"sync/atomic"

	"github.com/tknie/errorrepo"
	"github.com/tknie/flynn/adabas"
	"github.com/tknie/flynn/common"
	"github.com/tknie/flynn/mysql"
	"github.com/tknie/flynn/postgres"
	"github.com/tknie/log"
)

var globalRegID = common.RegDbID(0)

// RegisterDatabase Register database driver with a database URL returning a
// reference id for the driver path to database
func RegisterDatabase(dbref *common.Reference, password string) (common.RegDbID, error) {
	id := common.RegDbID(atomic.AddUint64((*uint64)(&globalRegID), 1))

	var db common.Database
	var err error
	switch dbref.TypeName {
	case "postgres":
		db, err = postgres.NewInstance(id, dbref, password)
	case "mysql":
		db, err = mysql.NewInstance(id, dbref, password)
	case "adabas":
		db, err = adabas.NewInstance(id, dbref, password)
	default:
		return 0, errorrepo.NewError("DB065535")
	}
	if err != nil {
		return 0, err
	}
	common.Databases = append(common.Databases, db)
	log.Log.Debugf("Register db type %s on %d", dbref.TypeName, db.ID())
	return db.ID(), nil
}

// Register database driver with a database URL returning a
// reference id for the driver path to database
func Register(typeName, url string) (common.RegDbID, error) {
	id := common.RegDbID(atomic.AddUint64((*uint64)(&globalRegID), 1))

	var db common.Database
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
	common.Databases = append(common.Databases, db)
	log.Log.Debugf("Register db type %s on %d", typeName, db.ID())
	return db.ID(), nil
}

// Maps database tables,views and/or maps usable for queries
func Maps() []string {
	databaseMaps := make([]string, 0)
	for _, database := range common.Databases {
		log.Log.Debugf("Map found " + database.URL())
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
func Unregister(id common.RegDbID) error {
	for i, d := range common.Databases {
		if d.ID() == id {
			log.Log.Debugf("Unregister db %d", d.ID())
			id.Close()
			newDatabases := make([]common.Database, 0)
			if i > 0 {
				newDatabases = append(newDatabases, common.Databases[0:i-1]...)
			}
			if len(common.Databases)-1 > i {
				newDatabases = append(newDatabases, common.Databases[i+1:]...)
			}
			common.Databases = newDatabases
			return nil
		}
	}
	return errorrepo.NewError("DB000001")
}
