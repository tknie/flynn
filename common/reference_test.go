package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReferenceURL(t *testing.T) {
	ref, _, err := NewReference("host:123")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Host: "host", Port: 123}, ref)
	ref, _, err = NewReference("localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Host: "localhost", Port: 5432, Database: "bitgarten"}, ref)
	var p string
	ref, p, err = NewReference("postgres://admin@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, "", p)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin", Database: "bitgarten"}, ref)
	ref, p, err = NewReference("postgres://admin:test123@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, "test123", p)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin", Database: "bitgarten"}, ref)
	ref, p, err = NewReference("postgres://admin:test123@test.example.com:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, "test123", p)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "test.example.com", Port: 5432, User: "admin", Database: "bitgarten"}, ref)
	ref, p, err = NewReference("postgres://admin:<password>@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin", Database: "bitgarten"}, ref)
	assert.Equal(t, "<password>", p)
	ref, p, err = NewReference("jdbc:mysql://localhost:3306/sonoo")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "localhost", Port: 3306, Database: "sonoo"}, ref)
	assert.Equal(t, "", p)
	ref, p, err = NewReference("admin:test123@tcp(host:123)/datab")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "host", Port: 123, User: "admin", Database: "datab"}, ref)
	assert.Equal(t, "test123", p)
	ref.SetType("mysql")
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "host", Port: 123, User: "admin", Database: "datab"}, ref)
	ref, _, err = NewReference("adatcp://adahost:123")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: AdabasType, Host: "adahost", Port: 123, Database: "4"}, ref)
	ref, p, err = NewReference("postgres://<user>:<password>@lion:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "lion", Port: 5432, Database: "bitgarten",
		User: "<user>"}, ref)
	assert.Equal(t, "<password>", p)
	ref, p, err = NewReference("postgres://admin:axx@localhost:5432/bitgarten?sslmode=require")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "lion", Port: 5432, Database: "bitgarten",
		User: "<user>", Options: []string{"sslmod=require"}}, ref)
	assert.Equal(t, "axx", p)

}

func TestReferenceFailuer(t *testing.T) {
	_, p, err := NewReference("aaxx")
	assert.Error(t, err)
	assert.Empty(t, p)
	_, p, err = NewReference("axa@aaxx")
	assert.Error(t, err)
	assert.Empty(t, p)
	assert.Equal(t, "URL parse error (match only 0)", err.Error())
}
