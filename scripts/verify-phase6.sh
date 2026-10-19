#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
: "${LEDGER_TEST_DATABASE_URL:?Set a disposable Ledger PostgreSQL URL}"
: "${ORDER_TEST_DATABASE_URL:?Set a disposable Order PostgreSQL URL}"
: "${PROJECTION_TEST_MONGODB_URI:?Set a disposable MongoDB replica-set URI}"
# Migration tests deliberately alter checksums; packages sharing PostgreSQL
# must execute serially. Race detection still covers their internal concurrency.
go test -race -p 1 -count=1 ./api/... ./orderservice/... ./ledgerservice/... ./projectionservice/...
go vet ./api/... ./orderservice/... ./ledgerservice/... ./projectionservice/...
