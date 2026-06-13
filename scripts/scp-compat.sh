#!/usr/bin/env bash
# Run SCP compatibility against released io.strata:strata-proto artifacts.
# Usage:
#   ./scripts/scp-compat.sh 0.1.0 0.1.1
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <released-version> [<released-version> ...]" >&2
  exit 2
fi

TMP_ROOT="${TMPDIR:-/tmp}/strata-scp-compat-$$"
mkdir -p "$TMP_ROOT"
trap 'rm -rf "$TMP_ROOT"' EXIT

echo "==> compile SCP compatibility peer"
mvn -q -pl strata-proto -am -DskipTests test-compile

for version in "$@"; do
  work="$TMP_ROOT/$version"
  mkdir -p "$work"
  cat > "$work/pom.xml" <<POM
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.strata.compat</groupId>
  <artifactId>scp-compat-$version</artifactId>
  <version>1</version>
  <dependencies>
    <dependency>
      <groupId>io.strata</groupId>
      <artifactId>strata-proto</artifactId>
      <version>$version</version>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>2.0.13</version>
      <scope>runtime</scope>
    </dependency>
  </dependencies>
</project>
POM

  echo "==> resolve released strata-proto $version"
  mvn -q -f "$work/pom.xml" dependency:build-classpath \
    -Dmdep.outputFile="$work/classpath.txt" -DincludeScope=runtime
  reference_cp="$(cat "$work/classpath.txt")"
  if [[ -z "$reference_cp" ]]; then
    echo "empty reference classpath for version $version" >&2
    exit 1
  fi

  echo "==> SCP released-artifact compatibility: $version"
  mvn -q -pl strata-proto -am \
    -Dtest=ScpReleasedArtifactCompatibilityTest \
    -Dstrata.compat.referenceVersion="$version" \
    -Dstrata.compat.referenceClasspath="$reference_cp" \
    -Dsurefire.failIfNoSpecifiedTests=false test
done

echo "==> SCP COMPAT GREEN"
