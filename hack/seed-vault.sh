#!/usr/bin/env bash
# Seeds the local Vault dev server with test secrets for credential rotation.
# Prerequisites: Vault dev container running (docker compose up -d vault)
#
# Usage: ./hack/seed-vault.sh
set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://localhost:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-root}"

export VAULT_ADDR VAULT_TOKEN

green() { printf '\033[32m%s\033[0m\n' "$*"; }

green "==> Seeding Vault at $VAULT_ADDR"

# KV v2 is enabled by default at "secret/" in dev mode.
vault kv put secret/trino/mysql \
  "connector.name=mysql" \
  "connection-url=jdbc:mysql://host.docker.internal:3306/testdb" \
  "connection-user=root" \
  "connection-password=trino_test"

green "    secret/trino/mysql written"

vault kv put secret/trino/tpch \
  "connector.name=tpch"

green "    secret/trino/tpch written"

green "==> Done. Verify with:"
echo "  VAULT_ADDR=$VAULT_ADDR VAULT_TOKEN=$VAULT_TOKEN vault kv get secret/trino/mysql"
