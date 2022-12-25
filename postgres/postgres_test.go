package postgres

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func PostgresTable(t *testing.T) string {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPort := os.Getenv("POSTGRES_PORT")
	postgresPassword := os.Getenv("POSTGRES_PWD")
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return ""
	}
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", postgresPassword, postgresHost, port, "Bitgarten")
	return url
}

func TestPostgresInit(t *testing.T) {
	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	m, err := pg.Maps()
	assert.NoError(t, err)
	assert.Equal(t, []string{"teststructtabledata", "albums", "albumpictures",
		"picturelocations", "pictures", "testtabledata"}, m)
}

func TestPostgresTableColumns(t *testing.T) {
	url := PostgresTable(t)
	pg, err := New(1, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, pg) {
		return
	}
	m, err := pg.GetTableColumn("Albums")
	assert.NoError(t, err)
	assert.Equal(t, []string{"id", "created", "updated_at",
		"directory", "title", "description", "option", "thumbnail",
		"albumtype", "albumkey"}, m)

}
