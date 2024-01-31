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

package common

import (
	"sync"

	"github.com/tknie/errorrepo"
	"github.com/tknie/log"
)

var Databases = make([]Database, 0)
var handlerLock sync.Mutex

func searchDataDriver(id RegDbID) (Database, error) {
	if id == 0 {
		return nil, errorrepo.NewError("DB000010")
	}
	log.Log.Debugf("search DataDriver %s in %d entries", id, len(Databases))
	for _, d := range Databases {
		log.Log.Debugf("Check id %d", d.ID())
		if d.ID() == id {
			d.Used()
			return d, nil
		}
	}
	log.Log.Debugf("DataDriver id not found")
	return nil, errorrepo.NewError("DB000002", id)
}
