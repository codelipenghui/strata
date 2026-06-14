#!/usr/bin/env bash
# Build the strata-server runtime image: package the fat jar, then assemble the container.
#   ./scripts/build-image.sh [tag]      (default tag: strata-server:local)
set -euo pipefail
cd "$(dirname "$0")/.."

TAG="${1:-strata-server:local}"

echo "==> packaging fat jar (mvn -pl strata-server -am -DskipTests package)"
mvn -q -pl strata-server -am -DskipTests package

echo "==> building image $TAG"
docker build -t "$TAG" .

echo "==> done"
docker images "$TAG" --format '{{.Repository}}:{{.Tag}}  {{.Size}}'
