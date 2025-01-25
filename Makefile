#
# Copyright 2022-2024 Thorsten A. Knieling
#
# SPDX-License-Identifier: Apache-2.0
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#

GO                 = go
GOARCH            ?= $(shell $(GO) env GOARCH)
GOOS              ?= $(shell $(GO) env GOOS)
GOEXE             ?= $(shell $(GO) env GOEXE)
GOBIN             ?= $(HOME)/go/bin

DATE              ?= $(shell date +%FT%T%z)

BIN                = $(CURDIR)/bin/$(GOOS)_$(GOARCH)
BINTOOLS           = $(CURDIR)/bin/tools/$(GOOS)_$(GOARCH)
BINTESTS           = $(CURDIR)/bin/tests/$(GOOS)_$(GOARCH)

VERSION            = v0.8

OBJECTS            = *.go postgres/*.go mysql/*.go adabas/*.go common/*.go

TESTPKGSDIR        = postgres adabas common
include $(CURDIR)/make/common.mk


