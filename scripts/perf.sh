#!/usr/bin/env bash
# Drive load against the running docker-compose cluster, then watch Grafana (http://localhost:3000).
# Bring the cluster up first: `docker compose up`. Flags pass through to the perf tool, e.g.:
#   ./scripts/perf.sh --duration 300
#   ./scripts/perf.sh --readers 0 --duration 300
#   ./scripts/perf.sh --files 8 --readers 2 --duration 300
#   ./scripts/perf.sh --read-sealed --duration 300
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose run --rm --no-deps loadgen perf --meta node1:9100 "$@"
