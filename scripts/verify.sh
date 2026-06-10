#!/usr/bin/env bash
# Full verification pyramid (tech design §16 / IMPLEMENTATION_PLAN.md).
#   ./scripts/verify.sh           unit + integration (no Docker needed; embedded ZK)
#   ./scripts/verify.sh --chaos   also run the testcontainers chaos suite (needs Docker)
#   ./scripts/verify.sh --tlc     also model-check tla/ChunkReplication.tla
set -euo pipefail
cd "$(dirname "$0")/.."

CHAOS=false
TLC=false
for arg in "$@"; do
  case "$arg" in
    --chaos) CHAOS=true ;;
    --tlc) TLC=true ;;
    --all) CHAOS=true; TLC=true ;;
  esac
done

echo "==> unit + integration tests (all modules)"
mvn -q install -DskipTests
mvn -q test

if $CHAOS; then
  if docker info >/dev/null 2>&1; then
    echo "==> chaos suite (toxiproxy + containerized zookeeper)"
    mvn -q -pl strata-it test -Dtest=ChaosTest "-DexcludedGroups="
  else
    echo "==> SKIP chaos: docker daemon not running" >&2
    exit 1
  fi
fi

if $TLC; then
  echo "==> TLA+ model checking"
  ./scripts/tlc.sh | tail -3
fi

echo "==> ALL GREEN"
