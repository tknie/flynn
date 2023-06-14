package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReferenceURL(t *testing.T) {
	ref, err := NewReference("host:123")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Host: "host", Port: 123}, ref)
	ref, err = NewReference("localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Host: "localhost", Port: 5432, Database: "bitgarten"}, ref)
	ref, err = NewReference("postgres://admin@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin", Database: "bitgarten"}, ref)
	ref, err = NewReference("postgres://admin:test123@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin", Database: "bitgarten"}, ref)
	ref, err = NewReference("postgres://admin:<password>@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin", Database: "bitgarten"}, ref)
	ref, err = NewReference("jdbc:mysql://localhost:3306/sonoo")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "localhost", Port: 3306, Database: "sonoo"}, ref)
	ref, err = NewReference("admin:test123@tcp(host:123)/datab")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Host: "host", Port: 123, User: "admin", Database: "datab"}, ref)
	ref.SetType("mysql")
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "host", Port: 123, User: "admin", Database: "datab"}, ref)
}

func TestReferenceFailuer(t *testing.T) {
	_, err := NewReference("aaxx")
	assert.Error(t, err)
	_, err = NewReference("axa@aaxx")
	assert.Error(t, err)
	assert.Equal(t, "URL parse error (match only 0)", err.Error())
}
