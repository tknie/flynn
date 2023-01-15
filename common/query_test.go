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
	assert.Equal(t, "select * from ABC", selectCmd)

	q.Fields = []string{"field1", "field2"}
	q.Limit = 10
	selectCmd = q.Select()
	assert.Equal(t, "select field1,field2 from ABC limit = 10", selectCmd)

	q.Order = []string{"fieldOrder:ASC"}
	selectCmd = q.Select()
	assert.Equal(t, "select field1,field2 from ABC limit = 10 order by fieldOrder ASC", selectCmd)

	q.Search = "id='10'"
	q.Limit = 0
	selectCmd = q.Select()
	assert.Equal(t, "select field1,field2 from ABC where id='10' order by fieldOrder ASC", selectCmd)

	q.Order = []string{"aaa:asc", "bbb:asc", "dddd:desc"}
	selectCmd = q.Select()
	assert.Equal(t, "select field1,field2 from ABC where id='10' order by aaa asc,bbb asc,dddd desc", selectCmd)

}
