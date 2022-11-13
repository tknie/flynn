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

}
