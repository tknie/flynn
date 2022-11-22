package common

type DataType byte

const (
	None DataType = iota
	Alpha
	Unicode
	Integer
	Number
	Byte
	Bytes
	Timestamp
	Date
)

var sqlTypes = []string{"", "VARCHAR", "UNICODE", "INTEGER", "NUMBER",
	"BYTE"}

func (dt DataType) SqlType() string {
	return sqlTypes[dt]
}
