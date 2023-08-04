package oracle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
)

func TestOracle(t *testing.T) {
	o, err := NewInstance(common.RegDbID(1),
		&common.Reference{Host: "abc", Port: 12345, Database: "SchemaXXX"}, "AA")
	assert.NoError(t, err)
	assert.Equal(t, "(DESCRIPTION =(ADDRESS_LIST =(ADDRESS =(PROTOCOL = TCP)(HOST = abc)(PORT = 12345)))(CONNECT_DATA=(SERVICE_NAME = SchemaXXX))", o.URL())
}
