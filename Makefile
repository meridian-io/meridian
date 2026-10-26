BINARY      := meridian-operator
MCP_BINARY  := meridian
IMG         := ghcr.io/meridian-io/trino-operator:dev
MCP_IMG     := ghcr.io/meridian-io/trino-mcp-server:dev
CHART       := charts/meridian
HELM        ?= helm
KUBECONFORM ?= kubeconform
GOPATH      := $(shell go env GOPATH 2>/dev/null)
CONTROLLER_GEN_VERSION := v0.20.1
CONTROLLER_GEN ?= $(GOPATH)/bin/controller-gen

.PHONY: all build build-mcp test test-mcp test-all lint lint-mcp \
        generate manifests verify-manifests install run docker-build docker-build-mcp \
        helm-lint helm-install helm-uninstall mcp-tidy release-dry-run fmt controller-gen e2e

all: build build-mcp

## ── MCP Server ──────────────────────────────────────────────────────────────

## Build the MCP server binary
build-mcp:
	cd mcp && go build -o ../bin/$(MCP_BINARY) ./cmd/meridian-mcp

## Run MCP server locally (stdio)
run-mcp:
	cd mcp && go run ./cmd/meridian-mcp --transport stdio --namespace meridian

## Run MCP server locally over HTTP (http://localhost:8080/mcp). Needs MERIDIAN_MCP_TOKEN
run-mcp-http:
	cd mcp && go run ./cmd/meridian-mcp --transport http --addr :8080 --namespace meridian

## Run MCP server tests
test-mcp:
	cd mcp && go test ./... -v -race -coverprofile=../coverage-mcp.out

## Tidy MCP server dependencies
mcp-tidy:
	cd mcp && go mod tidy

## Lint MCP server
lint-mcp:
	cd mcp && golangci-lint run ./...

## Build MCP server Docker image (linux binary for the host architecture)
docker-build-mcp:
	cd mcp && CGO_ENABLED=0 GOOS=linux go build -o ../bin/linux/$(MCP_BINARY) ./cmd/meridian-mcp
	docker build -t $(MCP_IMG) -f mcp/Dockerfile bin/linux

## ── Operator ────────────────────────────────────────────────────────────────

## Build the operator binary
build:
	cd operator && go build -o ../bin/$(BINARY) .

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
	cd operator && $(CONTROLLER_GEN) object:headerFile="../hack/boilerplate.go.txt" paths="./..."

## Generate CRD and RBAC manifests (source of truth: operator/config) and copy them into the Helm chart
manifests:
	cd operator && $(CONTROLLER_GEN) crd rbac:roleName=meridian-operator paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac
	rm -f $(CHART)/crds/*.yaml
	cp operator/config/crd/bases/*.yaml $(CHART)/crds/
	cp operator/config/rbac/role.yaml $(CHART)/files/operator-clusterrole.yaml

## Fail if generated manifests differ from what is committed (used in CI)
verify-manifests: manifests
	@git diff --exit-code -- operator/config $(CHART)/crds $(CHART)/files || (echo "CRD/RBAC manifests are out of date — run 'make manifests' and commit the result" && exit 1)
	@test -z "$$(git status --porcelain -- operator/config $(CHART)/crds $(CHART)/files)" || (echo "untracked generated files — run 'make manifests' and commit the result" && git status --short -- operator/config $(CHART)/crds $(CHART)/files && exit 1)

## Install the pinned controller-gen
controller-gen:
	go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

## Install CRDs into the cluster
install: manifests
	kubectl apply -f operator/config/crd/bases/

## Run operator locally against current kubeconfig
run: generate manifests
	cd operator && go run ./main.go --namespace=meridian

## Build operator Docker image (linux binary for the host architecture)
docker-build:
	cd operator && CGO_ENABLED=0 GOOS=linux go build -o ../bin/linux/$(BINARY) .
	docker build -t $(IMG) -f operator/Dockerfile bin/linux

docker-push:
	docker push $(IMG)

## ── End-to-end ──────────────────────────────────────────────────────────────

## Run the end-to-end suite on a new kind cluster (meridian-e2e) with real Trino.
## Needs kind, kubectl, helm and docker. E2E_POOL_SIZE=1 for machines with < 12 GB for Docker.
e2e:
	hack/e2e/run.sh

## ── Helm ────────────────────────────────────────────────────────────────────

## Lint the chart and validate rendered manifests against the Kubernetes schemas
helm-lint:
	$(HELM) lint $(CHART) --strict
	$(HELM) lint $(CHART) --strict -f hack/helm/ci-values.yaml
	$(HELM) template meridian $(CHART) -n meridian | $(KUBECONFORM) -strict -summary -skip ServiceMonitor -
	$(HELM) template meridian $(CHART) -n meridian -f hack/helm/ci-values.yaml | $(KUBECONFORM) -strict -summary -skip ServiceMonitor -

helm-install:
	$(HELM) upgrade --install meridian $(CHART) --namespace meridian --create-namespace

helm-uninstall:
	$(HELM) uninstall meridian --namespace meridian

## ── Formatting ──────────────────────────────────────────────────────────────

## Fail if any Go file is not gofmt-formatted
fmt:
	@out=$$(gofmt -l mcp operator e2e); test -z "$$out" || (echo "gofmt needed:"; echo "$$out"; exit 1)

## ── Release ─────────────────────────────────────────────────────────────────

## Dry-run GoReleaser to validate release config
release-dry-run:
	goreleaser release --snapshot --clean
