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

const referenceRegexp = `(?m)((\w*)://)?(([\w<>]+)(:(\S+))?@)?(tcp\()?(\w[\w.]*):(\d+)\)?(/(\w+))?\??(.*)`

type ReferenceType byte

const (
	NoType ReferenceType = iota
	MysqlType
	PostgresType
	AdabasType
)

var referenceTypeName = []string{"No valid Type", "Mysql", "Postgres", "Adabas"}

func (rt ReferenceType) String() string {
	return referenceTypeName[rt]
}

type Reference struct {
	Driver   ReferenceType
	Host     string
	Port     int
	User     string
	Database string
	Options  []string
}

// NewReference new reference of database link
func NewReference(url string) (*Reference, string, error) {
	var re = regexp.MustCompile(referenceRegexp)

	match := re.FindStringSubmatch(url)

	if len(match) < 10 {
		return nil, "", fmt.Errorf("URL parse error (match only %d)", len(match))
	}
	p, err := strconv.Atoi(match[9])
	if err != nil {
		return nil, "", fmt.Errorf("Reference url port error: %v", err)
	}
	ref := &Reference{Driver: ParseTypeName(match[2]),
		Host: match[8], Port: p, User: match[4], Database: match[11]}
	/*for i, match := range re.FindStringSubmatch(url) {
		fmt.Println(match, "found at index", i)
	}*/
	passwd := match[6]
	switch {
	case ref.Driver == NoType && strings.Contains(url, "@tcp("):
		ref.Driver = MysqlType
	case ref.Driver == AdabasType && ref.Database == "":
		ref.Database = "4"
	}
	if len(match) == 13 {
		ref.Options = strings.Split(match[12], "&")
	}
	return ref, passwd, nil
}

func (ref *Reference) OptionString() string {
	options := ""
	for _, o := range ref.Options {
		if options == "" {
			options += "?"
		} else {
			options += "&"
		}
		options += o
	}
	return options
}

// ParseTypeName parse type string to internal type
func ParseTypeName(t string) ReferenceType {
	switch strings.ToLower(t) {
	case "postgres":
		return PostgresType
	case "mysql":
		return MysqlType
	case "acj", "adatcp":
		return AdabasType
	}
	return NoType
}

func (r *Reference) SetType(t string) {
	r.Driver = ParseTypeName(t)
}
