package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type SubStruct struct {
	ABC   string
	Nr    uint64
	Value int64
	Doub  float64
	//	AA    complex128
	DoIt bool
}

type SubStruct3 struct {
	ABC   string `dbsql:"XYZ"`
	DEF   string `dbsql:"UUU"`
	Nr    uint64 `dbsql:"ID:IDENTITY(1, 1)"`
	Value int64
	Doub  float64
	//	AA    complex128
	DoIt bool
}

type GlobStruct struct {
	Test string
	Sub  *SubStruct
}

type GlobStruct2 struct {
	Test string
	Sub  SubStruct
}

type GlobStruct3 struct {
	Test string
	Sub  SubStruct3
}

type ArrayStruct struct {
	Test [3]string
	Sub  *SubStruct
}

type SliceStruct struct {
	Test []string
	Sub  *SubStruct
}

func TestDataType(t *testing.T) {
	assert.Equal(t, "INTEGER", Number.SqlType())
	assert.Equal(t, "TEXT", Text.SqlType())
	assert.Equal(t, "VARCHAR(19)", Alpha.SqlType(19))
	assert.Equal(t, "INTEGER", Integer.SqlType())
	assert.Equal(t, "DATE", Date.SqlType())
	assert.Equal(t, "BINARY(10)", Bytes.SqlType(false, 10))
	assert.Equal(t, "BYTEA", Bytes.SqlType(true, 10))
}

func TestDataTypeStruct(t *testing.T) {
	x := struct {
		St  string
		Int int
	}{"aaa", 1}
	s, err := SqlDataType(&x)
	assert.NoError(t, err)
	assert.Equal(t, "St VARCHAR(255), Int INTEGER", s)
	y := struct {
		XSt  string
		XInt int
		Xstr struct {
			Xii uint64
		}
	}{"aaa", 1, struct{ Xii uint64 }{2}}
	s, err = SqlDataType(&y)
	assert.NoError(t, err)
	assert.Equal(t, "XSt VARCHAR(255), XInt INTEGER, Xii INTEGER", s)
	global := &GlobStruct{}
	s, err = SqlDataType(global)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), ABC VARCHAR(255), Nr INTEGER, Value INTEGER, Doub DECIMAL(10,5), DoIt BIT(1)", s)
	global2 := &GlobStruct2{}
	s, err = SqlDataType(global2)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), ABC VARCHAR(255), Nr INTEGER, Value INTEGER, Doub DECIMAL(10,5), DoIt BIT(1)", s)
	global3 := &GlobStruct3{}
	s, err = SqlDataType(global3)
	assert.NoError(t, err)
	assert.Equal(t, "Test VARCHAR(255), XYZ VARCHAR(255), UUU VARCHAR(255), ID INTEGER IDENTITY(1, 1), Value INTEGER, Doub DECIMAL(10,5), DoIt BIT(1)", s)
	slice := &SliceStruct{}
	s, err = SqlDataType(slice)
	assert.Error(t, err)
	assert.Equal(t, "DB009: Slice types are not supported", err.Error())
	assert.Equal(t, "", s)
	arr := &ArrayStruct{}
	s, err = SqlDataType(arr)
	assert.Error(t, err)
	assert.Equal(t, "DB008: Array types are not supported", err.Error())
	assert.Equal(t, "", s)
}
