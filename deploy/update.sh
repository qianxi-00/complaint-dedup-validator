#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p app/runtime runtime
docker compose up -d --force-recreate --remove-orphans api worker
docker compose ps
