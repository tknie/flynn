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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tknie/flynn/common"
)

func TestSearchSecPgRows(t *testing.T) {
	InitLog(t)
	pg, err := postgresTarget(t)
	if !assert.NoError(t, err) {
		return
	}

	pg += "?sslmode=require&sslrootcert=files/root.crt"
	x, err := Register("postgres", pg)
	if !assert.NoError(t, err) {
		return
	}
	defer Unregister(x)

	q := &common.Query{TableName: "Albums",
		Search: "",
		Fields: []string{"Title", "created"},
		Order:  []string{"Title:ASC"},
	}
	counter := 0
	_, err = x.Query(q, func(search *common.Query, result *common.Result) error {
		assert.NotNil(t, search)
		assert.NotNil(t, result)
		assert.Len(t, result.Fields, 2)
		fmt.Println("RESULT:", result.Rows)
		ns := result.Rows[0].(string)
		ts := result.Rows[1].(time.Time)
		counter++
		switch counter {
		case 1:
			assert.Equal(t, "1.Hälfte Sommerferien 2019 sind vorbei", ns)
			assert.Equal(t, "2023-03-15 14:54:51.305585 +0000 UTC", ts.String())
		case 10:
			assert.Equal(t, "Fasching 2019", ns)
			assert.Equal(t, "2023-03-15 14:54:51.849488 +0000 UTC", ts.String())
		case 48:
			assert.Equal(t, "Weihnachtszeit 2019", ns)
			assert.Equal(t, "2023-03-15 14:54:53.617203 +0000 UTC", ts.String())
		default:
			assert.NotEqual(t, "blabla", ns)
		}

		return nil
	})
	assert.NoError(t, err)
}
