package db

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	def "github.com/tknie/db/common"
)

func postgresTarget(t *testing.T) (string, error) {
	postgresHost := os.Getenv("POSTGRES_HOST")
	postgresPort := os.Getenv("POSTGRES_PORT")
	postgresPassword := os.Getenv("POSTGRES_PWD")
	if !assert.NotEmpty(t, postgresHost) {
		return "", fmt.Errorf("Postgres Host not set")
	}
	assert.NotEmpty(t, postgresPort)
	port, err := strconv.Atoi(postgresPort)
	if !assert.NoError(t, err) {
		return "", fmt.Errorf("Postgres Port not set")
	}
	pg := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", "admin", postgresPassword, postgresHost, port, "Bitgarten")

	return pg, nil
}

func mysqlTarget(t *testing.T) (string, error) {
	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlPort := os.Getenv("MYSQL_PORT")
	mysqlPassword := os.Getenv("MYSQL_PWD")
	if !assert.NotEmpty(t, mysqlHost) {
		return "", fmt.Errorf("MySQL Host not set")
	}
	assert.NotEmpty(t, mysqlPort)
	port, err := strconv.Atoi(mysqlPort)
	if !assert.NoError(t, err) {
		return "", fmt.Errorf("Postgres Port not set")
	}
	pg := fmt.Sprintf("%s:%s@%s:%d/%s", "admin", mysqlPassword, mysqlHost, port, "Bitgarten")

	return pg, nil
}

func adabasTarget(t *testing.T) (string, error) {
	adabasHost := os.Getenv("ADABAS_HOST")
	adabasPort := os.Getenv("ADABAS_PORT")
	// adabasPassword := os.Getenv("ADABAS_PWD")
	if !assert.NotEmpty(t, adabasHost) {
		return "", fmt.Errorf("Adabas Host not set")
	}
	assert.NotEmpty(t, adabasPort)
	port, err := strconv.Atoi(adabasPort)
	if !assert.NoError(t, err) {
		return "", fmt.Errorf("Adabaas Port not set")
	}
	ada := fmt.Sprintf("acj;map;config=[adatcp://%s:%d,4]", adabasHost, port)

	return ada, nil
}

func TestInitDatabases(t *testing.T) {
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}
	x, err := Register("postgres", pg)
	assert.NoError(t, err)
	assert.True(t, x > 0)
	assert.Len(t, def.Databases, 1)
	err = Unregister(x)
	if !assert.NoError(t, err) {
		return
	}
	x, err = Register("postgres", pg)
	assert.NoError(t, err)
	assert.True(t, x > 0)
	pg2, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}
	x2, err := Register("postgres", pg2)
	assert.NoError(t, err)
	assert.True(t, x2 > 0)
	assert.Len(t, def.Databases, 2)
	err = Unregister(x)
	assert.NoError(t, err)
	assert.Len(t, def.Databases, 1)
	err = Unregister(x2)
	assert.NoError(t, err)
	assert.Len(t, def.Databases, 0)
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
	assert.Len(t, def.Databases, 0)
}
