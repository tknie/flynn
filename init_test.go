package db

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitDatabases(t *testing.T) {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPort := os.Getenv("POSTGRES_PORT")
	if !assert.NotEmpty(t, postgresHost) {
		return
	}
	assert.NotEmpty(t, postgresPort)
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return
	}
	pg := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", "Test123", postgresHost, port, "Bitgarten")
	x, err := Register("postgres", pg)
	assert.NoError(t, err)
	assert.NotNil(t, x)
}
