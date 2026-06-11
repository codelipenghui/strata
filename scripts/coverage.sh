#!/usr/bin/env bash
# Aggregate coverage runner for the full Strata verification pyramid.
#   ./scripts/coverage.sh                 unit + integration coverage report
#   ./scripts/coverage.sh --chaos         also include chaos tests (requires Docker)
#   ./scripts/coverage.sh --perf          also include perf smoke test
#   ./scripts/coverage.sh --tlc           also run the TLA+ model checker
#   ./scripts/coverage.sh --all           include chaos, perf, and TLA+
#   ./scripts/coverage.sh --enforce-100   fail unless all JaCoCo counters are fully covered
set -euo pipefail
cd "$(dirname "$0")/.."

CHAOS=false
PERF=false
TLC=false
ENFORCE=false
for arg in "$@"; do
  case "$arg" in
    --chaos) CHAOS=true ;;
    --perf) PERF=true ;;
    --tlc) TLC=true ;;
    --all) CHAOS=true; PERF=true; TLC=true ;;
    --enforce-100) ENFORCE=true ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

echo "==> compile/install modules"
mvn -q -Pcoverage clean install -DskipTests

echo "==> unit + integration tests with coverage"
mvn -q -Pcoverage test

if $CHAOS; then
  if docker info >/dev/null 2>&1; then
    echo "==> chaos tests with coverage"
    mvn -q -Pcoverage -pl strata-it test -Dtest=ChaosTest "-DexcludedGroups="
  else
    echo "==> SKIP chaos: docker daemon not running" >&2
    exit 1
  fi
fi

if $PERF; then
  echo "==> perf smoke with coverage"
  mvn -Pcoverage -pl strata-it test -Dtest=PerfSmokeTest "-DexcludedGroups=" 2>&1 \
    | grep -E "===|write |read |Tests run"
fi

echo "==> aggregate JaCoCo report"
mvn -q -Pcoverage -pl strata-coverage -am jacoco:report-aggregate

CSV="strata-coverage/target/site/jacoco-aggregate/jacoco.csv"
HTML="strata-coverage/target/site/jacoco-aggregate/index.html"
if [[ ! -f "$CSV" ]]; then
  echo "missing aggregate coverage CSV: $CSV" >&2
  exit 1
fi

echo "==> aggregate coverage summary"
awk -F, '
  NR > 1 {
    im += $4;  ic += $5;
    bm += $6;  bc += $7;
    lm += $8;  lc += $9;
    cm += $10; cc += $11;
    mm += $12; mc += $13;
  }
  function pct(covered, missed) {
    total = covered + missed;
    return total == 0 ? "100.00" : sprintf("%.2f", covered * 100 / total);
  }
  END {
    printf("instruction: %s%% (%d missed, %d covered)\n", pct(ic, im), im, ic);
    printf("branch:      %s%% (%d missed, %d covered)\n", pct(bc, bm), bm, bc);
    printf("line:        %s%% (%d missed, %d covered)\n", pct(lc, lm), lm, lc);
    printf("complexity:  %s%% (%d missed, %d covered)\n", pct(cc, cm), cm, cc);
    printf("method:      %s%% (%d missed, %d covered)\n", pct(mc, mm), mm, mc);
    exit (im + bm + lm + cm + mm == 0) ? 0 : 3;
  }
' "$CSV" || COVERAGE_STATUS=$?
COVERAGE_STATUS=${COVERAGE_STATUS:-0}

if [[ "$COVERAGE_STATUS" -ne 0 ]]; then
  echo "==> top uncovered classes"
  awk -F, '
    NR > 1 {
      missed = $4 + $6 + $8 + $10 + $12;
      if (missed > 0) {
        printf("%8d  %s.%s\n", missed, $2, $3);
      }
    }
  ' "$CSV" | sort -nr | head -20
fi

if $TLC; then
  echo "==> TLA+ model checking"
  ./scripts/tlc.sh | tail -8
fi

echo "coverage report: $HTML"
if $ENFORCE && [[ "$COVERAGE_STATUS" -ne 0 ]]; then
  echo "coverage is below 100%" >&2
  exit "$COVERAGE_STATUS"
fi

echo "==> COVERAGE RUN COMPLETE"
