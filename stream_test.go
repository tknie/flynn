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

package flynn

import (
	"crypto/md5"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
)

var testBlocksize = 65536
var checksumPictureTest = []struct {
	chksum string
	length int
	count  int
}{{"02E88E36FF888D0344B633B329AE8C5E", 927518, 927518/testBlocksize + 1},
	{"4CA51423A6E4850514760FCD7F1B1EB2", 402404, 402404/testBlocksize + 1},
	{"86B3B97B2A90F128B06437A78FD5B63A", 703794, 703794/testBlocksize + 1},
	{"6041C33476C4C49859106647C733A0E3", 518002, 518002/testBlocksize + 1},
	{"A34E983D50EF3264567EF27EEB24DE2E", 158005189, 158005189/testBlocksize + 1}}

func TestPgStreamPartial(t *testing.T) {
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
		return nil
	})
	chkMd5 := fmt.Sprintf("%X", md5.Sum(data))
	assert.Equal(t, "02E88E36FF888D0344B633B329AE8C5E", chkMd5)

	assert.NoError(t, err)
	assert.Equal(t, 927518, length)
}

func TestPgStreamAbort(t *testing.T) {
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

	length := 0
	data := make([]byte, 0)
	err = x.Stream(q, func(search *common.Query, stream *common.Stream) error {
		length += len(stream.Data)
		data = append(data, stream.Data...)
		if length > 10000 {
			return fmt.Errorf("aborted")
		}
		return nil
	})
	chkMd5 := fmt.Sprintf("%X", md5.Sum(data))
	assert.Equal(t, "FF18D3948B21012D7044A60855659952", chkMd5)

	assert.Error(t, err)
	assert.Equal(t, 12287, length)
}

func TestPgStreamListTest(t *testing.T) {
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

	for _, p := range checksumPictureTest {
		fmt.Println("Checking read of ", p.chksum, "...")
		q := &common.Query{TableName: "Pictures",
			Search:     "checksumpicture='" + p.chksum + "'",
			Descriptor: true,
			Limit:      1,
			Blocksize:  65536,
			Fields:     []string{"Media"},
		}

		count := 0
		data := make([]byte, 0)
		err = x.Stream(q, func(search *common.Query, stream *common.Stream) error {
			data = append(data, stream.Data...)
			assert.Len(t, data, 65536)
			count++
			return nil
		})
		chkMd5 := fmt.Sprintf("%X", md5.Sum(data))
		assert.Equal(t, p.chksum, chkMd5)

		assert.NoError(t, err)
		assert.Equal(t, p.count, count)
		assert.Equal(t, p.length, len(data))
	}
}

func TestPgQueryListTest(t *testing.T) {
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

	for _, p := range checksumPictureTest {
		q := &common.Query{TableName: "Pictures",
			Search: "checksumpicture='" + p.chksum + "'",
			Fields: []string{"Media"},
		}

		_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
			assert.Len(t, result.Fields, 1)
			ns := result.Rows[0].([]uint8)
			chkMd5 := fmt.Sprintf("%X", md5.Sum(ns))
			assert.Equal(t, p.chksum, chkMd5)
			assert.Equal(t, p.length, len(ns))
			return nil
		})

		assert.NoError(t, err)
	}
}
