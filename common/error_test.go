package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrors(t *testing.T) {

	err := NewError(1)
	assert.Equal(t, "DB001: ID not found", err.Error())
	err = NewError(4, "xyz")
	assert.Equal(t, "DB004: Test message xyz", err.Error())
	err = NewError(3, fmt.Errorf("abc"))
	assert.Equal(t, "DB003: Error db open: abc", err.Error())

}
