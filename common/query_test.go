/*
* Copyright 2023-2024 Thorsten A. Knieling
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
	selectCmd, err := q.Select()
	assert.Error(t, err)
	assert.Equal(t, "", selectCmd)

	q.TableName = "ABC"
	selectCmd, err = q.Select()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM ABC tn", selectCmd)

	q.Fields = []string{"field1", "field2"}
	q.Limit = "10"
	selectCmd, err = q.Select()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT field1,field2 FROM ABC tn LIMIT 10", selectCmd)

	q.Order = []string{"fieldOrder:ASC"}
	selectCmd, err = q.Select()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT field1,field2 FROM ABC tn ORDER BY fieldOrder ASC LIMIT 10", selectCmd)

	q.Search = "id='10'"
	q.Limit = "0"
	selectCmd, err = q.Select()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT field1,field2 FROM ABC tn WHERE id='10' ORDER BY fieldOrder ASC LIMIT 0", selectCmd)

	q.Order = []string{"aaa:asc", "bbb:asc", "dddd:desc"}
	q.Limit = "ALL"
	selectCmd, err = q.Select()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT field1,field2 FROM ABC tn WHERE id='10' ORDER BY aaa ASC,bbb ASC,dddd DESC LIMIT ALL", selectCmd)

}
