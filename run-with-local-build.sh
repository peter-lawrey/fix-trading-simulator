#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
mvn clean verify
for role in broker exchange; do
    (cd "$role-front-end" && npm ci && npm run build -- --prod)
done
exec docker compose up --build
