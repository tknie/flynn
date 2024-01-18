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

GO_TAGS     = $(if $(TAGS),$(TAGS),db)

TEST_RUN    = $(if $(TEST),-test.run $(TEST),)
TIMEOUT     = 2000
LOGPATH     = $(CURDIR)/logs

export LOGPATH

.PHONY: all
all: prepare fmt lint lib $(EXECS) test-build

.PHONY: clean
clean: ; $(info $(M) cleaning…)    @ ## Cleanup everything
	@rm -rf bin pkg logs test promote

.PHONY: cleanCache
cleanCache: clean ; $(info $(M) cleaning GO cache…)    @ ## Cleanup everything
	@$(GO) clean -cache -testcache -modcache

exec: $(EXECS)

lib: $(LIBS) $(CEXEC)

prepare: $(LOGPATH) $(CURLOGPATH) $(BIN) $(BINTOOLS)
	@echo "Build architecture ${GOARCH} ${GOOS} network=${WCPHOST} GOFLAGS=$(GO_FLAGS) GOEXE=$(GOEXE)"

$(LOGPATH):
	mkdir $(LOGPATH)

$(LIBS): | ; $(info $(M) building libraries…) @ ## Build program binary
	$Q cd $(CURDIR) && \
	    CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(CGO_EXT_LDFLAGS)" $(GO) build $(GO_FLAGS) \
		-buildmode=c-shared \
		-ldflags '-X $(PACKAGE)/common.Version=$(VERSION) -X $(PACKAGE)/common.BuildVersion=$(VERSION) -X $(PACKAGE)/common.BuildDate=$(DATE)' \
		-o $(BIN)/$(GOOS)/$@.so $@.go

$(EXECS): $(OBJECTS) ; $(info $(M) building executable…) @ ## Build program binary
	$Q cd $(CURDIR) && \
	    CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(CGO_EXT_LDFLAGS)" $(GO) build $(GO_FLAGS) \
		-ldflags '-X $(PACKAGE)/common.Version=$(VERSION) -X $(PACKAGE)/common.BuildDate=$(DATE)' \
		-o $@$(GOEXE) ./$(@:$(BIN)/%=%)

$(BIN):
	@mkdir -p $@
$(BIN)/%: $(BIN) ; $(info $(M) building $(REPOSITORY)…)
	$Q tmp=$$(mktemp -d); \
		(GO111MODULE=off GOPATH=$$tmp CGO_CFLAGS= CGO_LDFLAGS= \
		go get $(REPOSITORY) && cp $$tmp/bin/* $(BIN)/.) || ret=$$?; \
		# (GOPATH=$$tmp go clean -modcache ./...); \
		rm -rf $$tmp ; exit $$ret

$(BINTOOLS):
	@mkdir -p $@
$(BINTOOLS)/%: ; $(info $(M) building tool $(BINTOOLS) on $(REPOSITORY)…)
	$Q tmp=$$(mktemp -d); \
		(GOPATH=$$tmp CGO_CFLAGS= CGO_LDFLAGS= \
		go get $(REPOSITORY) && find $$tmp/bin -type f -exec cp {} $(BINTOOLS)/. \;) || ret=$$?; \
		(GOPATH=$$tmp go clean -modcache ./...);
#		rm -rf $$tmp ; exit $$ret

$(GOBIN):
	@mkdir -p $@
$(GOBIN)/%: ; $(info $(M) building tool $(BINTOOLS) on $(REPOSITORY)…)
	$Q \
		(GOBIN= BIN= GOPATH= CGO_CFLAGS= CGO_LDFLAGS= \
		go install $(REPOSITORY)@latest);

.PHONY: lint
lint: | $(GOLINT) ; $(info $(M) running golint…) @ ## Run golint
	$Q cd $(CURDIR) && ret=0 && for pkg in $(PKGS); do \
		test -z "$$($(GOLINT) $$pkg | tee /dev/stderr)" || ret=1 ; \
	 done ; exit $$ret

.PHONY: fmt
fmt: ; $(info $(M) running fmt…) @ ## Run go fmt on all source files
	@ret=0 && for d in $$($(GO) list -f '{{.Dir}}' ./... | grep -v /vendor/); do \
		$(GO) fmt  $$d/*.go || ret=$$? ; \
	 done ; exit $$ret

test-build: prepare ; $(info $(M) building $(NAME:%=% )tests…) @ ## Build tests
	$(GO) test -c -o $(BINTESTS)/db.test$(GOEXE) -tags $(GO_TAGS) .
	$Q cd $(CURDIR) && for pkg in $(TESTPKGSDIR); do echo "Build $$pkg in $(CURDIR)"; \
	 LD_LIBRARY_PATH="$(LD_LIBRARY_PATH):$(ACLDIR)/lib" \
	 DYLD_LIBRARY_PATH="$(DYLD_LIBRARY_PATH):$(ACLDIR)/lib" \
	 CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(CGO_EXT_LDFLAGS)" \
	 TESTFILES=$(TESTFILES) GO_ADA_MESSAGES=$(MESSAGES) LOGPATH=$(LOGPATH) REFERENCES=$(REFERENCES) \
	 $(GO) test -c -o $(BINTESTS)/$$pkg.test$(GOEXE) -tags $(GO_TAGS) ./$$pkg; done

TEST_TARGETS := test-default test-bench test-short test-verbose test-json test-race test-sanitizer
.PHONY: $(TEST_TARGETS) check test tests
test-bench:   ARGS=-run=__absolutelynothing__ -bench=. ## Run benchmarks
test-short:   ARGS=-short        ## Run only short tests
test-verbose: ARGS=-v            ## Run tests in verbose mode with coverage reporting
test-json:    ARGS=-json         ## Run tests in json mode
test-race:    ARGS=-race         ## Run tests with race detector
test-sanitizer:  ARGS=-msan      ## Run tests with race detector
$(TEST_TARGETS): NAME=$(MAKECMDGOALS:test-%=%)
$(TEST_TARGETS): test
check test tests: fmt lint ; $(info $(M) running $(NAME:%=% )tests…) @ ## Run tests
	$Q cd $(CURDIR) && LD_LIBRARY_PATH="$(LD_LIBRARY_PATH):$(ACLDIR)/lib" \
		DYLD_LIBRARY_PATH="$(DYLD_LIBRARY_PATH):$(ACLDIR)/lib" \
	    CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(CGO_EXT_LDFLAGS)" \
	    TESTFILES=$(TESTFILES) GO_ADA_MESSAGES=$(MESSAGES) LOGPATH=$(LOGPATH) REFERENCES=$(REFERENCES) \
	    $(GO) test -timeout $(TIMEOUT)s $(TEST_RUN) -tags $(GO_TAGS) $(ARGS) ./...

TEST_XML_TARGETS := test-xml-bench
.PHONY: $(TEST_XML_TARGETS) test-xml
test-xml-bench:     ARGS=-run=__absolutelynothing__ -bench=. ## Run benchmarks
$(TEST_XML_TARGETS): NAME=$(MAKECMDGOALS:test-xml-%=%)
$(TEST_XML_TARGETS): test-xml
test-xml: prepare fmt lint $(TESTOUTPUT) | $(GOTESTSUM) ; $(info $(M) running $(NAME:%=% )tests…) @ ## Run tests with xUnit output
	$Q cd $(CURDIR) && 2>&1 TESTFILES=$(TESTFILES) GO_ADA_MESSAGES=$(MESSAGES) LOGPATH=$(LOGPATH) \
	    REFERENCES=$(REFERENCES) LD_LIBRARY_PATH="$(LD_LIBRARY_PATH):$(ACLDIR)/lib" \
	    CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(CGO_EXT_LDFLAGS)" \
	    ENABLE_DEBUG=$(ENABLE_DEBUG) WCPHOST=$(WCPHOST) ADATCPHOST=$(ADATCPHOST) ADAMFDBID=$(ADAMFDBID) \
	    $(GOTESTSUM) --junitfile $(TESTOUTPUT)/tests.xml --raw-command -- $(CURDIR)/scripts/test.sh $(ARGS) ||:

COVERAGE_MODE = atomic
COVERAGE_PROFILE = $(COVERAGE_DIR)/profile.out
COVERAGE_XML = $(COVERAGE_DIR)/coverage.xml
COVERAGE_HTML = $(COVERAGE_DIR)/index.html
.PHONY: test-coverage test-coverage-tools
test-coverage-tools: | $(GOCOVMERGE) $(GOCOV) $(GOCOVXML)
test-coverage: COVERAGE_DIR := $(CURDIR)/test/coverage
test-coverage: fmt lint test-coverage-tools ; $(info $(M) running coverage tests…) @ ## Run coverage tests
	$Q mkdir -p $(COVERAGE_DIR)/coverage
	$Q echo "Work on test packages: $(TESTPKGS)"
	$Q cd $(CURDIR) && for pkg in $(TESTPKGS); do echo "Coverage for $$pkg"; \
		TESTFILES=$(TESTFILES) GO_ADA_MESSAGES=$(MESSAGES) LOGPATH=$(LOGPATH) \
	    REFERENCES=$(REFERENCES) LD_LIBRARY_PATH="$(LD_LIBRARY_PATH):$(ACLDIR)/lib" \
	    DYLD_LIBRARY_PATH="$(DYLD_LIBRARY_PATH):$(ACLDIR)/lib" \
	    CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(CGO_EXT_LDFLAGS)" \
	    ENABLE_DEBUG=$(ENABLE_DEBUG) WCPHOST=$(WCPHOST) ADATCPHOST=$(ADATCPHOST) ADAMFDBID=$(ADAMFDBID) \
		$(GO) test -count=1 \
			-coverpkg=$$($(GO) list -f '{{ join .Deps "\n" }}' $$pkg | \
					grep '^$(PACKAGE)/' | grep -v '^$(PACKAGE)/vendor/' | \
					tr '\n' ',')$$pkg \
			-covermode=$(COVERAGE_MODE) -timeout $(TIMEOUT)s $(GO_FLAGS) \
			-coverprofile="$(COVERAGE_DIR)/coverage/`echo $$pkg | tr "/" "-"`.cover" $$pkg ;\
	 done
	$Q echo "Start coverage analysis"
	$Q $(GOCOVMERGE) $(COVERAGE_DIR)/coverage/*.cover > $(COVERAGE_PROFILE)
	$Q $(GO) tool cover -html=$(COVERAGE_PROFILE) -o $(COVERAGE_HTML)
	$Q $(GOCOV) convert $(COVERAGE_PROFILE) | $(GOCOVXML) > $(COVERAGE_XML)

.PHONY: vendor-update
vendor-update:
	@echo "Uses GO modules"
	CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS) $(CGO_EXT_LDFLAGS)" $(GO) get -d -u ./...

.PHONY: version
version:
	@echo "Version: $(VERSION)"
