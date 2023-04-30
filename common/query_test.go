/*
* Copyright 2023 Thorsten A. Knieling
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	InitLog(t)

	q := Query{}
	selectCmd := q.Select()
	assert.Equal(t, "", selectCmd)

	q.TableName = "ABC"
	selectCmd = q.Select()
	assert.Equal(t, "SELECT * FROM ABC", selectCmd)

	q.Fields = []string{"field1", "field2"}
	q.Limit = 10
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC LIMIT 10", selectCmd)

	q.Order = []string{"fieldOrder:ASC"}
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC ORDER BY fieldOrder ASC LIMIT 10", selectCmd)

	q.Search = "id='10'"
	q.Limit = 0
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC WHERE id='10' ORDER BY fieldOrder ASC", selectCmd)

	q.Order = []string{"aaa:asc", "bbb:asc", "dddd:desc"}
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC WHERE id='10' ORDER BY aaa ASC,bbb ASC,dddd DESC", selectCmd)

}
