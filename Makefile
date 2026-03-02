BINARY      := meridian-operator
MCP_BINARY  := meridian-mcp
IMG         := ghcr.io/project-meridian/operator:latest
MCP_IMG     := ghcr.io/project-meridian/meridian-mcp:latest
GOPATH      := $(shell go env GOPATH 2>/dev/null)

.PHONY: all build build-mcp test test-mcp test-all lint lint-mcp \
        generate manifests install run docker-build docker-build-mcp \
        helm-install helm-uninstall mcp-tidy release-dry-run

all: build build-mcp

## ── MCP Server ──────────────────────────────────────────────────────────────

## Build the MCP server binary
build-mcp:
	cd mcp && go build -o ../bin/$(MCP_BINARY) ./cmd/meridian-mcp

## Run MCP server locally (stdio)
run-mcp:
	cd mcp && go run ./cmd/meridian-mcp --transport stdio --namespace meridian

## Run MCP server locally (SSE on :8080)
run-mcp-sse:
	cd mcp && go run ./cmd/meridian-mcp --transport sse --addr :8080 --namespace meridian

## Run MCP server tests
test-mcp:
	cd mcp && go test ./... -v -race -coverprofile=../coverage-mcp.out

## Tidy MCP server dependencies
mcp-tidy:
	cd mcp && go mod tidy

## Lint MCP server
lint-mcp:
	cd mcp && golangci-lint run ./...

## Build MCP server Docker image
docker-build-mcp:
	docker build -t $(MCP_IMG) -f mcp/Dockerfile \
		--build-arg BINARY=$(MCP_BINARY) mcp/

## ── Operator ────────────────────────────────────────────────────────────────

## Build the operator binary
build:
	cd operator && go build -o ../bin/$(BINARY) ./...

## Run operator tests
test:
	cd operator && go test ./... -v -coverprofile=../coverage.out

## Run all tests (operator + MCP)
test-all: test test-mcp

## Lint operator
lint:
	cd operator && golangci-lint run ./...

## Generate controller boilerplate (deepcopy, etc.)
generate:
	cd operator && controller-gen object:headerFile="../hack/boilerplate.go.txt" paths="./..."

## Generate CRD manifests
manifests:
	cd operator && controller-gen crd rbac:roleName=meridian-manager-role webhook paths="./..." output:crd:artifacts:config=../config/crd

## Install CRDs into the cluster
install: manifests
	kubectl apply -f config/crd/

## Run operator locally against current kubeconfig
run: generate manifests
	cd operator && go run ./main.go --namespace=meridian

## Build operator Docker image
docker-build:
	docker build -t $(IMG) -f operator/Dockerfile .

docker-push:
	docker push $(IMG)

## ── Helm ────────────────────────────────────────────────────────────────────

helm-install:
	helm install meridian charts/meridian --namespace meridian --create-namespace

helm-uninstall:
	helm uninstall meridian --namespace meridian

## ── Release ─────────────────────────────────────────────────────────────────

## Dry-run GoReleaser to validate release config
release-dry-run:
	goreleaser release --snapshot --clean
