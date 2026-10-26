#!/usr/bin/env bash
# End-to-end suite: creates a kind cluster, builds and loads this checkout's
# images, installs the last published chart, upgrades it to this checkout, and
# runs the scenarios in e2e/. Used by `make e2e` and by CI.
#
# Environment:
#   E2E_PREVIOUS_VERSION  chart version to install first and upgrade from.
#                         Default: the latest published chart. "none" = fresh install.
#   E2E_KEEP=1            keep the kind cluster afterwards (for debugging)
#   E2E_ARTIFACTS         where logs go when the suite fails (default: e2e-artifacts)
#   E2E_RUN               go test -run filter
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CLUSTER=meridian-e2e
CONTEXT="kind-${CLUSTER}"
NS=meridian
OPERATOR_IMAGE=ghcr.io/meridian-io/trino-operator:e2e
MCP_IMAGE=ghcr.io/meridian-io/trino-mcp-server:e2e
TRINO_IMAGE=${E2E_TRINO_IMAGE:-trinodb/trino:435}
ARTIFACTS=${E2E_ARTIFACTS:-${ROOT}/e2e-artifacts}
CHART_REF=oci://ghcr.io/meridian-io/charts/meridian

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

for cmd in kind kubectl helm docker go; do
  command -v "$cmd" >/dev/null || { echo "missing required tool: $cmd" >&2; exit 1; }
done

if [[ -z "${E2E_PREVIOUS_VERSION:-}" ]]; then
  E2E_PREVIOUS_VERSION=$(helm show chart "$CHART_REF" 2>/dev/null | awk '/^version:/ {print $2}')
  [[ -n "$E2E_PREVIOUS_VERSION" ]] || { echo "could not find the latest published chart; set E2E_PREVIOUS_VERSION" >&2; exit 1; }
fi
[[ "$E2E_PREVIOUS_VERSION" == "none" ]] && E2E_PREVIOUS_VERSION=""

collect() {
  log "collecting diagnostics into ${ARTIFACTS}"
  mkdir -p "$ARTIFACTS"
  local k=(kubectl --context "$CONTEXT")
  "${k[@]}" get clusters,clusterpools,deploy,pods,svc -n "$NS" -o wide >"$ARTIFACTS/resources.txt" 2>&1 || true
  "${k[@]}" get clusters,clusterpools -n "$NS" -o yaml >"$ARTIFACTS/meridian-resources.yaml" 2>&1 || true
  "${k[@]}" describe pods -n "$NS" >"$ARTIFACTS/pods-describe.txt" 2>&1 || true
  "${k[@]}" get events -n "$NS" --sort-by=.lastTimestamp >"$ARTIFACTS/events.txt" 2>&1 || true
  for c in operator mcp; do
    "${k[@]}" logs -n "$NS" -l "app.kubernetes.io/component=$c" --all-containers --prefix --tail=-1 \
      >"$ARTIFACTS/$c.log" 2>&1 || true
    "${k[@]}" logs -n "$NS" -l "app.kubernetes.io/component=$c" --all-containers --prefix --previous --tail=-1 \
      >"$ARTIFACTS/$c-previous.log" 2>/dev/null || true
  done
  "${k[@]}" logs -n "$NS" -l "meridian.io/role=coordinator" --prefix --tail=200 \
    >"$ARTIFACTS/trino-coordinators.log" 2>&1 || true
}

cleanup() {
  if [[ "${E2E_KEEP:-}" == "1" ]]; then
    log "keeping kind cluster ${CLUSTER} (kubectl --context ${CONTEXT})"
  else
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}

log "creating kind cluster ${CLUSTER}"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --config "$ROOT/hack/e2e/kind-config.yaml" --wait 2m
trap cleanup EXIT

log "pulling Trino into the node (${TRINO_IMAGE})"
# Pull inside the node in the background while the images build.
docker exec "${CLUSTER}-control-plane" crictl pull "$TRINO_IMAGE" >/dev/null &
pull_trino=$!
docker exec "${CLUSTER}-control-plane" crictl pull busybox:1.36 >/dev/null

log "building images from this checkout"
make -C "$ROOT" docker-build IMG="$OPERATOR_IMAGE"
make -C "$ROOT" docker-build-mcp MCP_IMG="$MCP_IMAGE"
kind load docker-image --name "$CLUSTER" "$OPERATOR_IMAGE" "$MCP_IMAGE"
wait "$pull_trino"

log "running the suite (upgrade from: ${E2E_PREVIOUS_VERSION:-fresh install})"
status=0
(
  cd "$ROOT/e2e"
  E2E_KUBE_CONTEXT="$CONTEXT" E2E_NAMESPACE="$NS" \
    E2E_OPERATOR_IMAGE="$OPERATOR_IMAGE" E2E_MCP_IMAGE="$MCP_IMAGE" \
    E2E_PREVIOUS_VERSION="$E2E_PREVIOUS_VERSION" E2E_TRINO_IMAGE="$TRINO_IMAGE" \
    go test -tags e2e -count=1 -v -timeout 45m ${E2E_RUN:+-run "$E2E_RUN"} ./...
) || status=$?

if [[ $status -ne 0 ]]; then
  collect
fi
exit $status
