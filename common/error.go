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
	"embed"
	"path"

	"github.com/tknie/errorrepo"
)

//go:embed messages
var embedFiles embed.FS

func init() {
	fss, err := embedFiles.ReadDir("messages")
	if err != nil {
		panic("Internal config load error: " + err.Error())
	}
	for _, f := range fss {
		if f.Type().IsRegular() {
			byteValue, err := embedFiles.ReadFile("messages/" + f.Name())
			if err != nil {
				panic("Internal config load error: " + err.Error())
			}
			lang := path.Ext(f.Name())
			errorrepo.RegisterMessage(lang[1:], string(byteValue))
		}
	}
}
