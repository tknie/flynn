/*
* Copyright 2022-2024 Thorsten A. Knieling
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReferenceURL(t *testing.T) {
	InitLog(t)

	ref, _, err := NewReference("host:123")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Host: "host", Port: 123, Options: []string(nil)}, ref)
	ref, _, err = NewReference("localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Host: "localhost", Port: 5432, Database: "bitgarten",
		Options: []string(nil)}, ref)
	var p string
	ref, p, err = NewReference("postgres://admin@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, "", p)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin",
		Database: "bitgarten", Options: []string(nil)}, ref)
	ref, p, err = NewReference("postgres://admin:test123@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, "test123", p)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin",
		Database: "bitgarten", Options: []string(nil)}, ref)
	ref, p, err = NewReference("postgres://admin:test123@test.example.com:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, "test123", p)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "test.example.com", Port: 5432, User: "admin",
		Database: "bitgarten", Options: []string(nil)}, ref)
	ref, p, err = NewReference("postgres://admin:<password>@localhost:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, User: "admin",
		Database: "bitgarten", Options: []string(nil)}, ref)
	assert.Equal(t, "<password>", p)
	ref, p, err = NewReference("jdbc:mysql://localhost:3306/sonoo")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "localhost", Port: 3306, Database: "sonoo"}, ref)
	assert.Equal(t, "", p)
	ref, p, err = NewReference("admin:test123@tcp(host:123)/datab")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "host", Port: 123, User: "admin", Database: "datab"}, ref)
	assert.Equal(t, "test123", p)
	ref.SetType("mysql")
	assert.Equal(t, &Reference{Driver: MysqlType, Host: "host", Port: 123, User: "admin", Database: "datab"}, ref)
	ref, _, err = NewReference("adatcp://adahost:123")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: AdabasType, Host: "adahost", Port: 123, Database: "4"}, ref)
	ref, p, err = NewReference("postgres://<user>:<password>@lion:5432/bitgarten")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "lion", Port: 5432, Database: "bitgarten",
		User: "<user>"}, ref)
	assert.Equal(t, "<password>", p)
	ref, p, err = NewReference("postgres://admin:axx@localhost:5432/bitgarten?sslmode=require")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, Database: "bitgarten",
		User: "admin", Options: []string{"sslmode=require"}}, ref)
	assert.Equal(t, "axx", p)
	ref, p, err = NewReference("postgres://localhost:5432/bitgarten?sslmode=require")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, Database: "bitgarten",
		User: "", Options: []string{"sslmode=require"}}, ref)
	assert.Equal(t, "", p)
	ref, p, err = NewReference("postgres://admin@localhost:5432/bitgarten?sslmode=require")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, Database: "bitgarten",
		User: "admin", Options: []string{"sslmode=require"}}, ref)
	assert.Equal(t, "", p)
	ref, p, err = NewReference("oracle://<user>:<password>@xaaaa:99989/schema")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: OracleType, Host: "xaaaa", Port: 99989, Database: "schema",
		User: "<user>"}, ref)
	assert.Equal(t, "<password>", p)
	ref, p, err = NewReference("postgres://admin:axx@localhost:5432/bitgarten?pool_max_conns=10")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: PostgresType, Host: "localhost", Port: 5432, Database: "bitgarten",
		User: "admin", Options: []string{"pool_max_conns=10"}}, ref)
	assert.Equal(t, "axx", p)

	ref, p, err = NewReference("oracle://user='orauser' password='osspaass' CONNECTSTRING=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: OracleType, Host: "", Port: 0, Database: "",
		User: "orauser", Options: []string{"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))"}}, ref)
	assert.Equal(t, "osspaass", p)

	ref, p, err = NewReference("oracle://user='orauser' password='osspaass' CONNECTSTRING='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))'")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: OracleType, Host: "", Port: 0, Database: "",
		User: "orauser", Options: []string{"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))"}}, ref)
	assert.Equal(t, "osspaass", p)

	ref, p, err = NewReference("oracle://user='orauser' password='osspaass' CONNECTSTRING='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST= abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))'")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: OracleType, Host: "", Port: 0, Database: "",
		User: "orauser", Options: []string{"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST= abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))"}}, ref)
	assert.Equal(t, "osspaass", p)

}

func TestReferenceOracleParse(t *testing.T) {
	InitLog(t)

	ref, p, err := NewReference("oracle://user='orauser' password='osspaass' CONNECTSTRING='(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST= abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))'")
	assert.NoError(t, err)
	assert.Equal(t, &Reference{Driver: OracleType, Host: "", Port: 0, Database: "",
		User: "orauser", Options: []string{"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST= abc)(PORT=12345)))(CONNECT_DATA=(SERVICE_NAME=SchemaXXX)))"}}, ref)
	assert.Equal(t, "osspaass", p)

}

func TestReferenceFailuer(t *testing.T) {
	InitLog(t)

	_, p, err := NewReference("aaxx")
	assert.Error(t, err)
	assert.Empty(t, p)
	_, p, err = NewReference("axa@aaxx")
	assert.Error(t, err)
	assert.Empty(t, p)
	assert.Equal(t, "DB000018: URL parse error (match only 0)", err.Error())
}
