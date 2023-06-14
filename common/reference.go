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

package common

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type ReferenceType byte

const (
	NoType ReferenceType = iota
	MysqlType
	PostgresType
	AdabasType
)

var referenceTypeName = []string{"No", "Mysql", "Postgres", "Adabas"}

func (rt ReferenceType) String() string {
	return referenceTypeName[rt]
}

type Reference struct {
	Driver   ReferenceType
	Host     string
	Port     int
	User     string
	Database string
}

func NewReference(url string) (*Reference, string, error) {
	var re = regexp.MustCompile(`(?m)((\w*)://)?((\w+)(:(\S+))?@)?(tcp\()?(\w[\w.]*):(\d+)\)?(/(\w+))?`)

	match := re.FindStringSubmatch(url)

	if len(match) < 10 {
		return nil, "", fmt.Errorf("URL parse error (match only %d)", len(match))
	}
	p, err := strconv.Atoi(match[9])
	if err != nil {
		return nil, "", fmt.Errorf("Reference url port error: %v", err)
	}
	ref := &Reference{Driver: checkType(match[2]),
		Host: match[8], Port: p, User: match[4], Database: match[11]}
	for i, match := range re.FindStringSubmatch(url) {
		fmt.Println(match, "found at index", i)
	}
	passwd := match[6]
	return ref, passwd, nil
}

func checkType(t string) ReferenceType {
	switch strings.ToLower(t) {
	case "postgres":
		return PostgresType
	case "mysql":
		return MysqlType
	case "acj":
		return AdabasType
	}
	return NoType
}

func (r *Reference) SetType(t string) {
	r.Driver = checkType(t)
}
