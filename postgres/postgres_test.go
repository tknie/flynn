/*
* Copyright 2022 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package postgres

import (
	"fmt"
	"os"
	"sort"
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
	pg.DeleteTable("TESTTABLE")
	m, err := pg.GetTableColumn("Albums")
	sort.Strings(m)
	assert.NoError(t, err)
	assert.Equal(t, []string{"albumkey", "albumtype",
		"created", "description", "directory", "id",
		"option", "published", "thumbnail", "title",
		"updated_at"}, m)

}
