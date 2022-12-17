package common

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
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

func SqlDataType(columns any) (string, error) {
	x := reflect.TypeOf(columns)
	if x.Kind() == reflect.Pointer {
		x = x.Elem()
	}
	switch x.Kind() {
	case reflect.Struct:
		var buffer bytes.Buffer
		for i := 0; i < x.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := x.Field(i)
			s, err := sqlDataTypeStructField(f)
			if err != nil {
				return "", err
			}
			buffer.WriteString(s)
		}
		return buffer.String(), nil
	}
	return "", NewError(5, "", fmt.Sprintf("%T", columns))
}

func sqlDataTypeStructField(field reflect.StructField) (string, error) {
	x := field.Type
	if x.Kind() == reflect.Pointer {
		x = x.Elem()
	}
	switch x.Kind() {
	case reflect.Struct:
		var buffer bytes.Buffer
		for i := 0; i < x.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := x.Field(i)
			s, err := sqlDataTypeStructFieldDataType(f)
			if err != nil {
				return "", err
			}
			buffer.WriteString(s)
		}
		return buffer.String(), nil
	default:
		return sqlDataTypeStructFieldDataType(field)
	}
	// return "", NewError(5, field.Name, x.Kind())
}

func sqlDataTypeStructFieldDataType(sf reflect.StructField) (string, error) {
	t := sf.Type
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	name := sf.Name
	additional := ""
	if tagName, ok := sf.Tag.Lookup("dbsql"); ok {
		tagField := strings.Split(tagName, ":")
		name = tagField[0]
		if len(tagField) > 1 {
			additional = " " + tagField[1]
		}
		if len(tagField) > 2 {
			if tagField[2] == "SERIAL" {
				return name + " SERIAL UNIQUE", nil
			}
		}
	}
	fmt.Println(name)
	switch t.Kind() {
	case reflect.String:
		return name + " " + Alpha.SqlType(255) + additional, nil
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64:
		return name + " " + Integer.SqlType() + additional, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		return name + " " + Integer.SqlType() + additional, nil
	case reflect.Float32, reflect.Float64:
		return name + " " + Decimal.SqlType(10, 5) + additional, nil
	case reflect.Bool:
		return name + " " + Bit.SqlType(1) + additional, nil
	case reflect.Complex64, reflect.Complex128:
		return "", NewError(7)
	case reflect.Struct:
		var buffer bytes.Buffer
		ty := t
		for i := 0; i < ty.NumField(); i++ {
			if i > 0 {
				buffer.WriteString(", ")
			}
			f := ty.Field(i)
			fmt.Println("Struct Field: " + f.Name)
			s, err := sqlDataTypeStructFieldDataType(f)
			if err != nil {
				return "", err
			}

			buffer.WriteString(s)
		}
		buffer.WriteString(additional)
		return buffer.String(), nil
	case reflect.Array:
		fmt.Println("Arrays", t.Len())
		if t.Elem().Kind() == reflect.Uint8 {
			return fmt.Sprintf("%s BYTES(%d)", name, t.Len()), nil
		}
		return "", NewError(8, sf.Name)
	case reflect.Slice:
		return "", NewError(9, sf.Name)
	default:
		//		return SqlDataType(t)
		// + " CONSTRAINT " + t.Name +
		// 	" CHECK (" + t.Name + " > 0)"
	}
	return "", NewError(6, sf.Name, t.Kind())
}
