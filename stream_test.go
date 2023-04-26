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

package db

import (
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
)

func TestPgStream(t *testing.T) {
	initLog()
	pgInstance, passwd, err := postgresTargetInstance(t)
	if !assert.NoError(t, err) {
		return
	}

	x, err := RegisterDatabase("postgres", pgInstance, passwd)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &common.Query{TableName: "Pictures",
		Search:     "checksumpicture='02E88E36FF888D0344B633B329AE8C5E'",
		Descriptor: true,
		Limit:      1,
		Fields:     []string{"Media"},
	}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.Len(t, result.Fields, 1)
		ns := result.Rows[0].([]uint8)
		chkMd5 := fmt.Sprintf("%X", md5.Sum(ns))
		assert.Equal(t, "02E88E36FF888D0344B633B329AE8C5E", chkMd5)
		counter++
		assert.True(t, counter == 1)
		ns = ns[9:1033]
		fmt.Printf("XXXX %X %d\n", md5.Sum(ns), len(ns))
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, counter)
	length := 0
	data := make([]byte, 0)
	err = x.Stream(q, func(search *common.Query, stream *common.Stream) error {
		length += len(stream.Data)
		data = append(data, stream.Data...)
		// fmt.Printf("XXXX %X %d\n", md5.Sum(data), len(data))
		return nil
	})
	chkMd5 := fmt.Sprintf("%X", md5.Sum(data))
	assert.Equal(t, "02E88E36FF888D0344B633B329AE8C5E", chkMd5)

	assert.NoError(t, err)
	assert.Equal(t, 927518, length)
}
