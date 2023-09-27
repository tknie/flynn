//go:build !flynn_noadabas
// +build !flynn_noadabas

/*
* Copyright 2022-2023 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package adabas

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
)

func TestAdabasInit(t *testing.T) {
	adabasHost := os.Getenv("ADABAS_HOST")
	adabasPort := os.Getenv("ADABAS_PORT")
	// postgresPassword := os.Getenv("ADABAS_PWD")
	port, err := strconv.Atoi(adabasPort)
	if !assert.NoError(t, err) {
		return
	}
	url := fmt.Sprintf("acj;map;config=[adatcp://%s:%d,4]", adabasHost, port)
	ada, err := New(10, url)
	assert.NoError(t, err)
	if !assert.NotNil(t, ada) {
		return
	}
	m, err := ada.Maps()
	sort.Strings(m)
	assert.NoError(t, err)
	assert.Equal(t, []string{"ADABAS_MAP", "Album", "Albums", "Picture",
		"PictureBinary", "PictureData", "PictureMetadata",
		"TESTTABLE"}, m)
}

func TestAdaSearch(t *testing.T) {
	e := &common.Entries{Fields: []string{"aaa"},
		Values: [][]any{{"XXX"}}}
	search := createSearch(e)
	assert.Equal(t, "aaa=XXX", search)

	e.Fields = []string{"%aaa"}
	e.Values = [][]any{{"XXX%"}}
	search = createSearch(e)
	assert.Equal(t, "aaa=['XXX'0x00:'XXX'0xff]", search)

}
