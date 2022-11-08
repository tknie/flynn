#
# Copyright 2022 Thorsten A. Knieling
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

include $(CURDIR)/make/common.mk

.PHONY: clean
clean: ; $(info $(M) cleaning…)    @ ## Cleanup everything
	@rm -rf bin pkg logs test promote

