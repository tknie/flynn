/*
* Copyright 2022-2024 Thorsten A. Knieling
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
 */

package flynn

import (
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
	"github.com/tknie/log"
)

func TestBinarySearchPgRows(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	log.Log.Debugf("Binary test")

	x, err := Handle("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Pictures",
		Search: "ChecksumPicture='B64F1DDF5683608579998E618545E497        '",
		Fields: []string{"Thumbnail"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 1)
		image := *(result.Rows[0].(*[]byte))
		counter++
		switch counter {
		case 1:
			assert.Len(t, image, 10074)
			assert.Equal(t, "B885CA8F7EB9364557C0CA12283C7823", fmt.Sprintf("%X", md5.Sum(image)))
		default:
			assert.NotEqual(t, "blabla", image)
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestBinarySearchMariaRows(t *testing.T) {
	InitLog(t)
	log.Log.Debugf("TEST: %s", t.Name())

	mysql, err := mysqlTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	log.Log.Debugf("Binary test")

	x, err := Handle("mysql", mysql)
	if !assert.NoError(t, err) {
		return
	}
	defer x.FreeHandler()

	q := &common.Query{TableName: "Pictures",
		Search: "ChecksumPicture='B64F1DDF5683608579998E618545E497        '",
		Fields: []string{"Thumbnail"}}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 1)
		image := *(result.Rows[0].(*[]byte))
		counter++
		switch counter {
		case 1:
			assert.Len(t, image, 10074)
			assert.Equal(t, "B885CA8F7EB9364557C0CA12283C7823", fmt.Sprintf("%X", md5.Sum(image)))
		default:
			assert.NotEqual(t, "blabla", image)
		}

		return nil
	})
	assert.NoError(t, err)
}
