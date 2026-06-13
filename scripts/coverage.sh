#!/usr/bin/env bash
# Aggregate coverage runner for the full Strata verification pyramid.
#   ./scripts/coverage.sh                 unit + integration coverage report
#   ./scripts/coverage.sh --fault         also include embedded stress/fault tests
#   ./scripts/coverage.sh --chaos         also include Docker/Toxiproxy chaos tests (requires Docker)
#   ./scripts/coverage.sh --soak          also include the bounded stress/fault soak
#   ./scripts/coverage.sh --perf          also include perf smoke test
#   ./scripts/coverage.sh --tlc           also run the TLA+ model checker
#   ./scripts/coverage.sh --all           include fault, Docker chaos, perf, and TLA+
#   ./scripts/coverage.sh --enforce-100   fail unless all JaCoCo counters are fully covered
#   ./scripts/coverage.sh --skip-default --fault  include only the selected optional gate
#   ./scripts/coverage.sh --skip-default --fault --stress-only -Dstrata.stress.seed=<seed> -Dstrata.stress.batches=<n>
#                                           replay one full stress/fault matrix iteration with coverage
#   ./scripts/coverage.sh --skip-default --fault -Dstrata.stress.case=<case> -Dstrata.stress.seed=<seed> -Dstrata.stress.batches=<n>
#                                           replay one exact stress/fault artifact case (StressFaultTest only)
#   ./scripts/coverage.sh --skip-default --fault -Dtest=<TestClass#method>
#                                           replay one targeted fault test with coverage and audit its artifacts
#   ./scripts/coverage.sh --skip-default --chaos -Dtest=<TestClass#method> -Dstrata.external.seed=<seed>
#                                           replay one targeted chaos test with coverage and audit its artifacts
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/correctness-artifacts.sh

FAULT_TESTS="StressFaultTest,ProcessCrashRecoveryTest,MetadataProcessFailoverTest,FailureRecoveryTest,MetadataFailoverTest,OpenQuorumFailureTest,RepairAndRetentionTest,RecoveryCatchUpTest,RecoveryDivergenceTest"

RUN_DEFAULT=true
FAULT=false
STRESS_ONLY=false
CHAOS=false
SOAK=false
PERF=false
TLC=false
ENFORCE=false
MAVEN_ARGS=()
ARTIFACT_AUDIT_SELF_TESTED=false

usage() {
  sed -n 's/^#   //p' "$0"
}

run_artifact_audit_self_test() {
  if ! $ARTIFACT_AUDIT_SELF_TESTED; then
    bash scripts/correctness-artifacts.sh --self-test
    ARTIFACT_AUDIT_SELF_TESTED=true
  fi
}

is_exact_stress_replay() {
  local stress_case
  stress_case=$(stress_case_filter)

  case "$stress_case" in
    ""|all|ALL|All) return 1 ;;
    *) return 0 ;;
  esac
}

stress_case_filter() {
  local stress_case=""
  local arg
  for arg in "${MAVEN_ARGS[@]}"; do
    case "$arg" in
      -Dstrata.stress.case=*) stress_case="${arg#-Dstrata.stress.case=}" ;;
    esac
  done
  printf '%s\n' "$stress_case"
}

is_targeted_test_replay() {
  [[ -n "$(test_filter)" ]]
}

test_filter() {
  local test=""
  local arg
  for arg in "${MAVEN_ARGS[@]}"; do
    case "$arg" in
      -Dtest=*) test="${arg#-Dtest=}" ;;
    esac
  done
  printf '%s\n' "$test"
}

for arg in "$@"; do
  case "$arg" in
    --skip-default) RUN_DEFAULT=false ;;
    --fault|--stress) FAULT=true ;;
    --stress-only) FAULT=true; STRESS_ONLY=true ;;
    --chaos) CHAOS=true ;;
    --soak) SOAK=true ;;
    --perf) PERF=true ;;
    --tlc) TLC=true ;;
    --all) FAULT=true; CHAOS=true; PERF=true; TLC=true ;;
    --enforce-100) ENFORCE=true ;;
    -D*) MAVEN_ARGS+=("$arg") ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown argument: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! $RUN_DEFAULT && ! $FAULT && ! $CHAOS && ! $SOAK && ! $TLC && ! $PERF; then
  echo "nothing selected: remove --skip-default or select an optional gate" >&2
  usage >&2
  exit 2
fi

if $ENFORCE && ! $RUN_DEFAULT && ! $FAULT && ! $CHAOS && ! $SOAK && ! $PERF; then
  echo "--enforce-100 requires at least one coverage-producing gate" >&2
  usage >&2
  exit 2
fi

if $RUN_DEFAULT; then
  echo "==> compile/install modules"
  mvn -q -Pcoverage clean install -DskipTests

  echo "==> unit + integration tests with coverage"
  mvn -q -Pcoverage test
fi

if $FAULT; then
  run_artifact_audit_self_test
  prepare_correctness_artifacts
  fault_tests="$FAULT_TESTS"
  if is_exact_stress_replay; then
    echo "==> embedded stress/fault exact replay with coverage"
    fault_tests="StressFaultTest"
  elif $STRESS_ONLY; then
    echo "==> embedded stress/fault matrix replay with coverage"
    fault_tests="StressFaultTest"
  elif is_targeted_test_replay; then
    echo "==> embedded fault targeted replay with coverage"
    fault_tests="$(test_filter)"
  else
    echo "==> embedded stress/fault tests with coverage"
  fi
  mvn -q -Pcoverage -pl strata-it -am -Dtest="$fault_tests" -DexcludedGroups=perf,soak \
    -Dsurefire.failIfNoSpecifiedTests=false "${MAVEN_ARGS[@]}" test
  audit_correctness_artifacts
  if is_exact_stress_replay; then
    require_exact_stress_fault_artifact "$(stress_case_filter)"
  elif $STRESS_ONLY; then
    require_stress_fault_case_artifacts
  elif is_targeted_test_replay; then
    :
  else
    require_full_fault_gate_artifacts
  fi
fi

if $CHAOS; then
  if docker info >/dev/null 2>&1; then
    run_artifact_audit_self_test
    prepare_correctness_artifacts
    chaos_tests="ChaosTest,ExternalNemesisTest"
    if is_targeted_test_replay; then
      echo "==> Docker chaos targeted replay with coverage"
      chaos_tests="$(test_filter)"
    else
      echo "==> Docker chaos tests with coverage"
    fi
    mvn -q -Pcoverage,chaos -pl strata-it -am -Dtest="$chaos_tests" \
      -Dsurefire.failIfNoSpecifiedTests=false "${MAVEN_ARGS[@]}" test
    audit_correctness_artifacts
    if ! is_targeted_test_replay; then
      require_chaos_gate_artifacts
    fi
  else
    echo "==> SKIP chaos: docker daemon not running" >&2
    exit 1
  fi
fi

if $SOAK; then
  run_artifact_audit_self_test
  prepare_correctness_artifacts
  echo "==> bounded stress/fault soak with coverage"
  mvn -q -Pcoverage,soak -pl strata-it -am -Dtest=StressFaultSoakTest \
    -Dsurefire.failIfNoSpecifiedTests=false "${MAVEN_ARGS[@]}" test
  audit_correctness_artifacts
  require_soak_gate_artifacts
fi

if $PERF; then
  echo "==> perf smoke with coverage"
  mvn -Pcoverage -pl strata-it -am -Dtest=PerfSmokeTest \
    -Dsurefire.failIfNoSpecifiedTests=false "-DexcludedGroups=" "${MAVEN_ARGS[@]}" test 2>&1 \
    | grep -E "===|write |read |Tests run"
fi

if $RUN_DEFAULT || $FAULT || $CHAOS || $SOAK || $PERF; then
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
  echo "coverage report: $HTML"
else
  COVERAGE_STATUS=0
  echo "==> SKIP aggregate JaCoCo report: no coverage-producing gate selected"
fi

if $TLC; then
  echo "==> TLA+ model checking"
  ./scripts/tlc.sh | tail -8
fi

if $ENFORCE && [[ "$COVERAGE_STATUS" -ne 0 ]]; then
  echo "coverage is below 100%" >&2
  exit "$COVERAGE_STATUS"
fi

echo "==> COVERAGE RUN COMPLETE"
