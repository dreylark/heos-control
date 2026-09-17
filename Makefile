.PHONY: lint verify generate generate-check build dev-init db-up migrate run integration coverage image image-test chart-check workflow-format workflow-check smoke qualify down
.DEFAULT_GOAL := verify

SQLC = go -C tools/sqlc tool sqlc
OAPI = go -C tools/oapi-codegen tool oapi-codegen -config ../../api/oapi-codegen.yaml
YAMLFMT = go -C tools/yamlfmt tool yamlfmt -conf ../../.yamlfmt
VERSION ?= $(shell cat VERSION)
COMMIT ?= $(shell git rev-parse HEAD)$(shell git diff --quiet || echo -dirty)
SOURCE ?=

generate:
	$(SQLC) generate -f ../../sqlc.yaml
	$(OAPI) -o ../../internal/api/api.gen.go ../../api/openapi.yaml

generate-check:
	$(SQLC) diff -f ../../sqlc.yaml
	@set -eu; output=$$(mktemp); trap 'rm -f "$$output"' EXIT; \
	$(OAPI) -o "$$output" ../../api/openapi.yaml; \
	diff -u internal/api/api.gen.go "$$output"

lint: generate-check
	@test -z "$$(gofmt -l cmd internal scripts tests)"
	go vet ./...

verify: lint
	go test ./...
	go test -race ./...
	go build ./...

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)" -o bin/heos-control ./cmd/heos-control

dev-init:
	go run ./scripts/devsetup

db-up: dev-init
	docker compose up -d --wait postgres

migrate:
	go run ./cmd/heos-control migrate -config .local/owner.json

run:
	go run ./cmd/heos-control serve -config .local/runtime.json

integration:
	HEOS_TEST_OWNER_CONFIG=$$(pwd)/.local/test-owner.json HEOS_TEST_RUNTIME_CONFIG=$$(pwd)/.local/test-runtime.json go test -race -count=1 -tags=integration ./...

coverage:
	rm -rf coverage
	mkdir coverage
	test -r "$${HEOS_TEST_OWNER_CONFIG:-$$PWD/.local/test-owner.json}"
	test -r "$${HEOS_TEST_RUNTIME_CONFIG:-$$PWD/.local/test-runtime.json}"
	go list ./cmd/... ./internal/... > coverage/scope.txt
	go version > coverage/toolchain.txt
	git rev-parse HEAD > coverage/revision.txt
	@if [ -n "$$(git status --porcelain)" ]; then echo dirty >> coverage/revision.txt; fi
	HEOS_TEST_OWNER_CONFIG=$${HEOS_TEST_OWNER_CONFIG:-$$PWD/.local/test-owner.json} \
	HEOS_TEST_RUNTIME_CONFIG=$${HEOS_TEST_RUNTIME_CONFIG:-$$PWD/.local/test-runtime.json} \
	go test -v -race -count=1 -tags=integration -covermode=atomic \
	-coverpkg="$$(paste -sd, coverage/scope.txt)" -coverprofile=coverage/coverage.raw.out ./cmd/... ./internal/...
	@set -eu; status=0; go run ./scripts/coverage || status=$$?; \
	go tool cover -func=coverage/coverage.out > coverage/coverage.functions.txt; \
	go tool cover -html=coverage/coverage.out -o coverage/coverage.html; \
	exit "$$status"

image:
	docker build --target runtime --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) --build-arg SOURCE="$(SOURCE)" -t heos-control:dev .

image-test:
	docker build --target test -t heos-control:test .

chart-check:
	go test -tags=chart -count=1 ./tests/chart

workflow-format:
	$(YAMLFMT) ../../.github/workflows ../../.github/actions ../../.github/actionlint.yaml ../../.github/dependabot.yaml ../../.golangci.yaml

workflow-check:
	$(YAMLFMT) -lint ../../.github/workflows ../../.github/actions ../../.github/actionlint.yaml ../../.github/dependabot.yaml ../../.golangci.yaml
	@command -v shellcheck >/dev/null || { echo "shellcheck is required for workflow-check; install it and add it to PATH" >&2; exit 1; }
	go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
	shellcheck scripts/*.sh
	node --test scripts/ci/*.test.cjs
	go test -v -count=1 -tags=ci ./tests/ci

smoke: build
	bash scripts/smoke.sh

qualify: build
	go run ./scripts/qualify

down:
	docker compose --profile app --profile tools --profile tests down
