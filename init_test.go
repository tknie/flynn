package db

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	def "github.com/tknie/db/definition"
)

func TestInitDatabases(t *testing.T) {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPort := os.Getenv("POSTGRES_PORT")
	postgresPassword := os.Getenv("POSTGRES_PWD")
	if !assert.NotEmpty(t, postgresHost) {
		return
	}
	assert.NotEmpty(t, postgresPort)
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return
	}
	pg := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", postgresPassword, postgresHost, port, "Bitgarten")
	x, err := Register("postgres", pg)
	assert.NoError(t, err)
	assert.True(t, x > 0)
	assert.Len(t, databases, 1)
	err = Unregister(x)
	if !assert.NoError(t, err) {
		return
	}
	x, err = Register("postgres", pg)
	assert.NoError(t, err)
	assert.True(t, x > 0)
	pg2 := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", postgresPassword, postgresHost, port, "Bitgarten")
	x2, err := Register("postgres", pg2)
	assert.NoError(t, err)
	assert.True(t, x2 > 0)
	assert.Len(t, databases, 2)
	err = Unregister(x)
	assert.NoError(t, err)
	assert.Len(t, databases, 1)
	err = Unregister(x2)
	assert.NoError(t, err)
	assert.Len(t, databases, 0)
}

func TestInitWrongDatabases(t *testing.T) {
	postgresPort := os.Getenv("POSTGRES_PORT")
	assert.NotEmpty(t, postgresPort)
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return
	}
	pg := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", "Test123", "abs", port, "Bitgarten")
	x, err := Register("postgres", pg)
	assert.Error(t, err)
	assert.Equal(t, def.RegDbID(0), x)
	assert.Len(t, databases, 0)
}
