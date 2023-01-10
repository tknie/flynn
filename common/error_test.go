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

package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {

	err := NewError(1)
	assert.Nil(t, err.(*Error).err)
	assert.Equal(t, "DB001: ID not found", err.Error())
	err = NewError(4, "xyz")
	assert.Nil(t, err.(*Error).err)
	assert.Equal(t, "DB004: Test message xyz", err.Error())
	err = fmt.Errorf("abc")
	xerr := NewError(3, err)
	assert.Equal(t, "DB003: Error db open: abc", xerr.Error())
	assert.Error(t, xerr.(*Error).err)
	assert.Equal(t, err, xerr.(*Error).err)
	err = NewError(65535)
	assert.Equal(t, "DB65535: not implemented", err.Error())
}
