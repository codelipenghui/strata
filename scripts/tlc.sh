#!/usr/bin/env bash
# Model-check tla/ChunkReplication.tla with TLC. Fetches tla2tools.jar on first run.
set -euo pipefail
cd "$(dirname "$0")/../tla"

JAR=tla2tools.jar
if [ ! -f "$JAR" ]; then
  echo "fetching tla2tools.jar..."
  curl -fsSL -o "$JAR" https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
fi

exec java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto -deadlock ChunkReplication.tla
