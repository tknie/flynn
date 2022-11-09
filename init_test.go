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
}
