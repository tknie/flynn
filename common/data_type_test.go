package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataType(t *testing.T) {
	assert.Equal(t, "INTEGER", Number.SqlType())
	assert.Equal(t, "TEXT", Text.SqlType())
	assert.Equal(t, "VARCHAR(19)", Alpha.SqlType(19))
	assert.Equal(t, "INTEGER", Integer.SqlType())
	assert.Equal(t, "DATE", Date.SqlType())
	assert.Equal(t, "BINARY(10)", Bytes.SqlType(false, 10))
	assert.Equal(t, "BYTEA", Bytes.SqlType(true, 10))
}
