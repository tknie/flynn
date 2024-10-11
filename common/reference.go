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
	"regexp"
	"strconv"
	"strings"

	"github.com/tknie/errorrepo"
	"github.com/tknie/log"
)

const referenceRegexp = `(?m)((\w*)://)?(([\w<>]+)(:(\S+))?@)?(tcp\()?(\w[\w.]*):(\d+)\)?(/(\w+))?\??(.*)`

type ReferenceType byte

const (
	NoType ReferenceType = iota
	MysqlType
	PostgresType
	AdabasType
	OracleType
)

var referenceTypeName = []string{"No valid Type", "Mysql", "Postgres", "Adabas", "Oracle"}

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

func ParseUrl(url string) (*Reference, string, error) {
	var re = regexp.MustCompile(referenceRegexp)

	match := re.FindStringSubmatch(url)

	if len(match) < 10 {
		return nil, "", errorrepo.NewError("DB000018", len(match))
	}
	p, err := strconv.Atoi(match[9])
	if err != nil {
		return nil, "", errorrepo.NewError("DB000019", err)
	}
	ref := &Reference{Driver: ParseTypeName(match[2]),
		Host: match[8], Port: p, User: match[4], Database: match[11]}
	/*for i, match := range re.FindStringSubmatch(url) {
		fmt.Println(match, "found at index", i)
	}*/
	if len(match) == 13 && match[12] != "" {
		ref.Options = strings.Split(match[12], "&")
	}
	return ref, match[6], nil
}

func Trim(value string) string {
	cleanedValue := value
	cleanedValue = strings.Trim(cleanedValue, "'")
	cleanedValue = strings.Trim(cleanedValue, "\"")
	return cleanedValue
}

func parseOracle(url string) (*Reference, string, error) {
	ref := &Reference{Driver: OracleType}
	currentUrl := strings.TrimPrefix(url, "oracle://")
	password := ""
	log.Log.Debugf("Parse Oracle: %s", currentUrl)
	for {
		begin := 0
		end := 0
		index := strings.IndexAny(currentUrl, "=")
		parameterName := strings.ToLower(currentUrl[begin:index])
		currentUrl = currentUrl[index+1:]
		log.Log.Debugf("REST: %s", currentUrl)
		quote := 0
		index = 0
		switch {
		case currentUrl[quote] == '"':
			end = strings.IndexAny(currentUrl[1:], "\"") + 1
			index = 1
		case currentUrl[quote] == '\'':
			end = strings.IndexAny(currentUrl[1:], "'") + 1
			index = 1
		default:
			end = strings.IndexAny(currentUrl, " ")
			quote = -1
		}
		log.Log.Debugf("Part %s:%s", index, end)
		value := currentUrl[index:end]
		switch {
		case parameterName == "user":
			ref.User = Trim(value)
		case parameterName == "password":
			password = Trim(value)
		case parameterName == "connectstring":
			ref.Options = make([]string, 1)
			ref.Options[0] = Trim(value)
		}
		if len(currentUrl) < end+2+quote {
			break
		}
		currentUrl = currentUrl[end+2+quote:]
		log.Log.Debugf("REST NEXT: %s", currentUrl)
	}

	// var re = regexp.MustCompile(`(?m)(\w+)=([^\s]+)`)
	// str := url
	// str = strings.TrimPrefix(str, "oracle://")
	// log.Log.Debugf("Parse %s", str)
	// match := re.FindAllStringSubmatch(str, -1)
	// log.Log.Debugf("Match %v", match)
	// for _, list := range match {
	// 	parameterName := strings.ToLower(list[1])
	// 	switch {
	// 	case parameterName == "user":
	// 		ref.User = Trim(list[2])
	// 	case parameterName == "password":
	// 		password = Trim(list[2])
	// 	case parameterName == "connectstring":
	// 		ref.Options = make([]string, 1)
	// 		ref.Options[0] = Trim(list[2])
	// 	}
	// }

	return ref, password, nil
}

// NewReference new reference of database link
func NewReference(url string) (*Reference, string, error) {
	if strings.Contains(strings.ToLower(url), "connectstring=") {
		log.Log.Debugf("Parse oracle %s", url)
		return parseOracle(url)
	}
	log.Log.Debugf("Parse common %s", url)
	ref, passwd, err := ParseUrl(url)
	if err != nil {
		return nil, "", err
	}
	switch {
	case ref.Driver == NoType && strings.Contains(url, "@tcp("):
		ref.Driver = MysqlType
	case ref.Driver == AdabasType && ref.Database == "":
		ref.Database = "4"
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
	case "oracle":
		return OracleType
	}
	return NoType
}

func (r *Reference) SetType(t string) {
	r.Driver = ParseTypeName(t)
}
