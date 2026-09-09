#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
mkdir -p app/runtime runtime
docker compose run --rm migrate
docker compose up -d --no-deps --force-recreate --remove-orphans api
docker compose ps
