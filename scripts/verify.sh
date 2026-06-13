#!/usr/bin/env bash
# Full verification pyramid (tech design §16 / IMPLEMENTATION_PLAN.md).
#   ./scripts/verify.sh           unit + integration (no Docker needed; embedded ZK)
#   ./scripts/verify.sh --fault   also run embedded stress/fault and process-crash tests
#   ./scripts/verify.sh --chaos   also run the Docker/Toxiproxy chaos suite (needs Docker)
#   ./scripts/verify.sh --soak    also run the bounded stress/fault soak
#   ./scripts/verify.sh --tlc     also model-check tla/ChunkReplication.tla
#   ./scripts/verify.sh --perf    also run the perf smoke benchmark (prints latency/throughput)
#   ./scripts/verify.sh --compat  run current SCP compatibility + metadata-store conformance
#   ./scripts/verify.sh --compat --compat-version=0.1.0
#                                    also run SCP compatibility against released artifacts
#   ./scripts/verify.sh --all     run fault, Docker chaos, perf, and TLA+ (soak stays explicit)
#   ./scripts/verify.sh --skip-default --fault   run only the selected optional gate
#   ./scripts/verify.sh --skip-default --fault --stress-only -Dstrata.stress.seed=<seed> -Dstrata.stress.batches=<n>
#                                    replay one full stress/fault matrix iteration
#   ./scripts/verify.sh --skip-default --fault -Dstrata.stress.case=<case> -Dstrata.stress.seed=<seed> -Dstrata.stress.batches=<n>
#                                    replay one exact stress/fault artifact case (StressFaultTest only)
#   ./scripts/verify.sh --skip-default --fault -Dtest=<TestClass#method>
#                                    replay one targeted fault test and audit its artifacts
#   ./scripts/verify.sh --skip-default --chaos -Dstrata.external.seed=<seed>
#                                    replay the external child-JVM nemesis schedules
#   ./scripts/verify.sh --skip-default --chaos -Dtest=<TestClass#method> -Dstrata.external.seed=<seed>
#                                    replay one targeted chaos test and audit its artifacts
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/correctness-artifacts.sh

FAULT_TESTS="StressFaultTest,ProcessCrashRecoveryTest,MetadataProcessFailoverTest,FailureRecoveryTest,MetadataFailoverTest,OpenQuorumFailureTest,RepairAndRetentionTest,RecoveryCatchUpTest,RecoveryDivergenceTest"
CURRENT_COMPAT_TESTS="MessageGoldenCorpusTest,ScpV0CompatibilityTest,ZkMetadataStoreConformanceTest,InMemoryMetadataStoreConformanceTest"

RUN_DEFAULT=true
FAULT=false
STRESS_ONLY=false
CHAOS=false
SOAK=false
TLC=false
PERF=false
COMPAT=false
MAVEN_ARGS=()
COMPAT_VERSIONS=()
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
    --tlc) TLC=true ;;
    --perf) PERF=true ;;
    --compat) COMPAT=true ;;
    --compat-version=*) COMPAT=true; COMPAT_VERSIONS+=("${arg#--compat-version=}") ;;
    --all) FAULT=true; CHAOS=true; TLC=true; PERF=true ;;
    -D*) MAVEN_ARGS+=("$arg") ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "unknown argument: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! $RUN_DEFAULT && ! $FAULT && ! $CHAOS && ! $SOAK && ! $TLC && ! $PERF && ! $COMPAT; then
  echo "nothing selected: remove --skip-default or select an optional gate" >&2
  usage >&2
  exit 2
fi

if $COMPAT && [[ ${#COMPAT_VERSIONS[@]} -eq 0 && -n "${STRATA_COMPAT_VERSIONS:-}" ]]; then
  IFS=', ' read -r -a COMPAT_VERSIONS <<< "$STRATA_COMPAT_VERSIONS"
fi

if $RUN_DEFAULT; then
  echo "==> unit + integration tests (all modules)"
  mvn -q install -DskipTests
  mvn -q test
fi

if $FAULT; then
  run_artifact_audit_self_test
  prepare_correctness_artifacts
  fault_tests="$FAULT_TESTS"
  if is_exact_stress_replay; then
    echo "==> embedded stress/fault exact case replay"
    fault_tests="StressFaultTest"
  elif $STRESS_ONLY; then
    echo "==> embedded stress/fault matrix replay"
    fault_tests="StressFaultTest"
  elif is_targeted_test_replay; then
    echo "==> embedded fault targeted replay"
    fault_tests="$(test_filter)"
  else
    echo "==> embedded stress/fault suite"
  fi
  mvn -q -pl strata-it -am -Dtest="$fault_tests" -DexcludedGroups=perf,soak \
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
      echo "==> Docker chaos targeted replay"
      chaos_tests="$(test_filter)"
    else
      echo "==> Docker chaos suite (toxiproxy + containerized zookeeper)"
    fi
    mvn -q -Pchaos -pl strata-it -am -Dtest="$chaos_tests" \
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
  echo "==> bounded stress/fault soak"
  mvn -q -Psoak -pl strata-it -am -Dtest=StressFaultSoakTest \
    -Dsurefire.failIfNoSpecifiedTests=false "${MAVEN_ARGS[@]}" test
  audit_correctness_artifacts
  require_soak_gate_artifacts
fi

if $PERF; then
  echo "==> perf smoke (write both durability modes + sequential read)"
  mvn -pl strata-it -am -Dtest=PerfSmokeTest \
    -Dsurefire.failIfNoSpecifiedTests=false "-DexcludedGroups=" "${MAVEN_ARGS[@]}" test 2>&1 \
    | grep -E "===|write |read |Tests run"
fi

if $COMPAT; then
  echo "==> current compatibility/conformance suite"
  mvn -q -pl strata-proto,strata-meta -am -Dtest="$CURRENT_COMPAT_TESTS" \
    -Dsurefire.failIfNoSpecifiedTests=false test

  if [[ ${#COMPAT_VERSIONS[@]} -gt 0 ]]; then
    echo "==> SCP released-artifact compatibility"
    ./scripts/scp-compat.sh "${COMPAT_VERSIONS[@]}"
  else
    echo "==> SKIP released-artifact compatibility: no versions selected"
  fi
fi

if $TLC; then
  echo "==> TLA+ model checking"
  ./scripts/tlc.sh | tail -3
fi

echo "==> ALL GREEN"
