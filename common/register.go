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

package common

var Databases = make([]Database, 0)

func searchDataDriver(id RegDbID) (Database, error) {
	for _, d := range Databases {
		if d.ID() == id {
			return d, nil
		}
	}
	return nil, NewError(2)
}
