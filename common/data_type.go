package common

import (
	"fmt"
)

type DataType byte

const (
	None DataType = iota
	Alpha
	Text
	Unicode
	Integer
	Decimal
	Number
	Bit
	Bytes
	CurrentTimestamp
	Date
	BLOB
)

var sqlTypes = []string{"", "VARCHAR(%d)", "TEXT", "UNICODE(%d)", "INTEGER",
	"DECIMAL(%d,%d)", "INTEGER", "BIT(%d)", "BINARY(%d)",
	"TIMESTAMP(%s)", "DATE", "BLOB(%d)"}

func (dt DataType) SqlType(arg ...any) string {
	if dt == Bytes {
		if arg[0].(bool) {
			return "BYTEA"
		} else {
			return fmt.Sprintf("BINARY(%d)", arg[1:]...)
		}
	}
	return fmt.Sprintf(sqlTypes[dt], arg...)
}
