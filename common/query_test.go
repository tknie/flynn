package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	q := Query{}
	selectCmd := q.Select()
	assert.Equal(t, "", selectCmd)

	q.TableName = "ABC"
	selectCmd = q.Select()
	assert.Equal(t, "SELECT * FROM ABC", selectCmd)

	q.Fields = []string{"field1", "field2"}
	q.Limit = 10
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC LIMIT = 10", selectCmd)

	q.Order = []string{"fieldOrder:ASC"}
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC LIMIT = 10 ORDER BY fieldOrder ASC", selectCmd)

	q.Search = "id='10'"
	q.Limit = 0
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC WHERE id='10' ORDER BY fieldOrder ASC", selectCmd)

	q.Order = []string{"aaa:asc", "bbb:asc", "dddd:desc"}
	selectCmd = q.Select()
	assert.Equal(t, "SELECT field1,field2 FROM ABC WHERE id='10' ORDER BY aaa ASC,bbb ASC,dddd DESC", selectCmd)

}
