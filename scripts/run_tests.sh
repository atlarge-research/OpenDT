#!/bin/sh
set -eu

# Execute the pytest suite inside the Dockerized application container.
# This mirrors the runtime environment the orchestrator uses in production.
if command -v docker-compose >/dev/null 2>&1; then
  docker-compose run --rm opendt pytest "$@"
else
  docker compose run --rm opendt pytest "$@"
fi
