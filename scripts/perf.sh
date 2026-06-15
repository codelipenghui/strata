#!/usr/bin/env bash
# Drive load against the running docker-compose cluster, then watch Grafana (http://localhost:3000).
# Bring the cluster up first: `docker compose up`. Flags pass through to the perf tool, e.g.:
#   ./scripts/perf.sh --workload write --record-size 65536 --files 8 --window 64 --duration 120
#   ./scripts/perf.sh --workload write --fsync --duration 300
#   ./scripts/perf.sh --workload read  --read-size 65536 --readers 8 --duration 120
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose run --rm --no-deps loadgen perf --meta node1:9200 "$@"
