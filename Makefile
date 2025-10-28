.PHONY: test integration lint build clean

test:
	go test -v -race -cover ./...

DS9_TEST_PROJECT ?= integration-testing-476513
DS9_TEST_DATABASE ?= ds9-test
DS9_TEST_LOCATION ?= us-central1

integration:
	@echo "==> Setting up integration test environment"
	@echo "    Project: $(DS9_TEST_PROJECT)"
	@echo "    Database: $(DS9_TEST_DATABASE)"
	@echo "    Location: $(DS9_TEST_LOCATION)"
	@echo ""
	@echo "==> Checking if test database exists..."
	@if gcloud firestore databases describe --database=$(DS9_TEST_DATABASE) --project=$(DS9_TEST_PROJECT) >/dev/null 2>&1; then \
		echo "    Database already exists, skipping creation"; \
	else \
		echo "    Database does not exist, creating..."; \
		if ! gcloud firestore databases create --database=$(DS9_TEST_DATABASE) \
			--location=$(DS9_TEST_LOCATION) \
			--type=datastore-mode \
			--project=$(DS9_TEST_PROJECT); then \
			echo ""; \
			echo "ERROR: Failed to create database $(DS9_TEST_DATABASE)"; \
			echo "Please check:"; \
			echo "  1. Project exists: $(DS9_TEST_PROJECT)"; \
			echo "  2. You have permission: datastore.databases.create"; \
			echo "  3. Cloud Firestore API is enabled"; \
			exit 1; \
		fi; \
		echo "    Database created successfully"; \
	fi
	@echo ""
	@echo "==> Running integration tests..."
	@DS9_TEST_PROJECT=$(DS9_TEST_PROJECT) go test -v -race -tags=integration -timeout=5m ./... || \
		(echo ""; echo "==> Tests failed, cleaning up..."; \
		 gcloud firestore databases delete --database=$(DS9_TEST_DATABASE) --project=$(DS9_TEST_PROJECT) --quiet 2>/dev/null; \
		 exit 1)
	@echo ""
	@echo "==> Cleaning up test database..."
	@gcloud firestore databases delete --database=$(DS9_TEST_DATABASE) --project=$(DS9_TEST_PROJECT) --quiet
	@echo "==> Integration tests complete!"

lint:
	go vet ./...
	gofmt -s -w .
	test -z "$$(gofmt -s -l .)"

build:
	go build ./...

clean:
	go clean -cache -testcache

all: lint test build

# BEGIN: lint-install .
# http://github.com/codeGROOVE-dev/lint-install

.PHONY: lint
lint: _lint

LINT_ARCH := $(shell uname -m)
LINT_OS := $(shell uname)
LINT_OS_LOWER := $(shell echo $(LINT_OS) | tr '[:upper:]' '[:lower:]')
LINT_ROOT := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

# shellcheck and hadolint lack arm64 native binaries: rely on x86-64 emulation
ifeq ($(LINT_OS),Darwin)
	ifeq ($(LINT_ARCH),arm64)
		LINT_ARCH=x86_64
	endif
endif

LINTERS :=
FIXERS :=

GOLANGCI_LINT_CONFIG := $(LINT_ROOT)/.golangci.yml
GOLANGCI_LINT_VERSION ?= v2.5.0
GOLANGCI_LINT_BIN := $(LINT_ROOT)/out/linters/golangci-lint-$(GOLANGCI_LINT_VERSION)-$(LINT_ARCH)
$(GOLANGCI_LINT_BIN):
	mkdir -p $(LINT_ROOT)/out/linters
	rm -rf $(LINT_ROOT)/out/linters/golangci-lint-*
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(LINT_ROOT)/out/linters $(GOLANGCI_LINT_VERSION)
	mv $(LINT_ROOT)/out/linters/golangci-lint $@

LINTERS += golangci-lint-lint
golangci-lint-lint: $(GOLANGCI_LINT_BIN)
	find . -name go.mod -execdir "$(GOLANGCI_LINT_BIN)" run -c "$(GOLANGCI_LINT_CONFIG)" \;

FIXERS += golangci-lint-fix
golangci-lint-fix: $(GOLANGCI_LINT_BIN)
	find . -name go.mod -execdir "$(GOLANGCI_LINT_BIN)" run -c "$(GOLANGCI_LINT_CONFIG)" --fix \;

.PHONY: _lint $(LINTERS)
_lint:
	@exit_code=0; \
	for target in $(LINTERS); do \
		$(MAKE) $$target || exit_code=1; \
	done; \
	exit $$exit_code

.PHONY: fix $(FIXERS)
fix:
	@exit_code=0; \
	for target in $(FIXERS); do \
		$(MAKE) $$target || exit_code=1; \
	done; \
	exit $$exit_code

# END: lint-install .
