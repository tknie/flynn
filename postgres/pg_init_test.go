package postgres

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresInit(t *testing.T) {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPort := os.Getenv("POSTGRES_PORT")
	postgresPassword := os.Getenv("POSTGRES_PWD")
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return
	}
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", postgresPassword, postgresHost, port, "Bitgarten")
	pg, err := New(1, url)
	assert.NoError(t, err)
	assert.NotNil(t, pg)
	m, err := pg.Maps()
	assert.NoError(t, err)
	assert.Equal(t, []string{"albums", "albumpictures", "picturelocations", "pictures"}, m)
}
