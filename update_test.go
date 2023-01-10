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
	"testing"

	"github.com/stretchr/testify/assert"
	def "github.com/tknie/db/common"
)

func TestUpdateInit(t *testing.T) {
	columns := make([]*def.Column, 0)
	columns = append(columns, &def.Column{Name: "ID", DataType: def.Alpha, Length: 10})
	columns = append(columns, &def.Column{Name: "Name", DataType: def.Alpha, Length: 200})

	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)
	list := make([]any, 0)
	x.Update(testStructTable, &def.Entries{Fields: []string{"ID", "Name"},
		Values: [][]any{list}})
}
