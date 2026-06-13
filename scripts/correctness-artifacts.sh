#!/usr/bin/env bash

CORRECTNESS_ARTIFACT_ROOT="${CORRECTNESS_ARTIFACT_ROOT:-strata-it/target/correctness-artifacts}"
STRESS_FAULT_ARTIFACT_CASES=(
  rf3-aq2-single
  rf3-aq2-pooled
  rf3-aq2-fsync
  rf4-aq3-pooled
  rf4-aq3-fsync
)
PROCESS_CRASH_ARTIFACT_CASES=(
  rf3-aq2
  rf3-aq2-fsync
  rf4-aq3
  meta-failover-rf3-aq2-fsync
)

prepare_correctness_artifacts() {
  mkdir -p "$CORRECTNESS_ARTIFACT_ROOT"
  find "$CORRECTNESS_ARTIFACT_ROOT" -type f -name '*.txt' -delete
}

artifact_has_nonempty_key() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '
    index($0, key "=") == 1 {
      value = substr($0, length(key) + 2)
      gsub(/[[:space:]]/, "", value)
      if (value != "") {
        found = 1
      }
    }
    END {
      exit found ? 0 : 1
    }
  ' "$file"
}

artifact_value() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '
    index($0, key "=") == 1 {
      print substr($0, length(key) + 2)
      exit
    }
  ' "$file"
}

artifact_safe_name() {
  local value="$1"
  local out=""
  local i
  local c
  for ((i = 0; i < ${#value}; i++)); do
    c="${value:i:1}"
    case "$c" in
      [abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-]) out+="$c" ;;
      *) out+="-" ;;
    esac
  done
  printf '%s\n' "$out"
}

artifact_line_starts_with() {
  local file="$1"
  local prefix="$2"
  awk -v prefix="$prefix" '
    index($0, prefix) == 1 {
      found = 1
    }
    END {
      exit found ? 0 : 1
    }
  ' "$file"
}

artifact_line_contains() {
  local file="$1"
  local fragment="$2"
  grep -F -q -- "$fragment" "$file"
}

artifact_line_count_starting_with() {
  local file="$1"
  local prefix="$2"
  awk -v prefix="$prefix" '
    index($0, prefix) == 1 {
      count++
    }
    END {
      print count + 0
    }
  ' "$file"
}

require_artifact_keys() {
  local file="$1"
  shift
  local key
  for key in "$@"; do
    if ! artifact_has_nonempty_key "$file" "$key"; then
      echo "artifact missing non-empty $key: $file" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
  done
}

require_artifact_lines_starting_with() {
  local file="$1"
  shift
  local prefix
  for prefix in "$@"; do
    if ! artifact_line_starts_with "$file" "$prefix"; then
      echo "artifact missing line starting with $prefix: $file" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
  done
}

require_artifact_lines_containing() {
  local file="$1"
  shift
  local fragment
  for fragment in "$@"; do
    if ! artifact_line_contains "$file" "$fragment"; then
      echo "artifact missing line containing $fragment: $file" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
  done
}

require_artifact_line_count_starting_with() {
  local file="$1"
  local prefix="$2"
  local expected="$3"
  local actual
  actual=$(artifact_line_count_starting_with "$file" "$prefix")
  if [[ "$actual" != "$expected" ]]; then
    echo "artifact expected $expected lines starting with $prefix, found $actual: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

require_artifact_prefixed_lines_containing() {
  local file="$1"
  local prefix="$2"
  local fragment="$3"
  if ! awk -v prefix="$prefix" -v fragment="$fragment" '
    index($0, prefix) == 1 {
      found = 1
      if (index($0, fragment) == 0) {
        bad = 1
      }
    }
    END {
      exit found && !bad ? 0 : 1
    }
  ' "$file"; then
    echo "artifact lines starting with $prefix must contain $fragment: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

require_artifact_value() {
  local file="$1"
  local key="$2"
  local expected="$3"
  local actual
  actual=$(artifact_value "$file" "$key")
  if [[ "$actual" != "$expected" ]]; then
    echo "artifact $key must be $expected, found '$actual': $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

require_artifact_value_matches() {
  local file="$1"
  local key="$2"
  local pattern="$3"
  local actual
  actual=$(artifact_value "$file" "$key")
  if [[ ! "$actual" =~ $pattern ]]; then
    echo "artifact $key value '$actual' does not match $pattern: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

require_artifact_equal_keys() {
  local file="$1"
  local left="$2"
  local right="$3"
  local left_value
  local right_value
  left_value=$(artifact_value "$file" "$left")
  right_value=$(artifact_value "$file" "$right")
  if [[ "$left_value" != "$right_value" ]]; then
    echo "artifact $left=$left_value differs from $right=$right_value: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

require_replay_command_contains() {
  local file="$1"
  shift
  local command
  command=$(artifact_value "$file" "replayCommand")
  local fragment
  for fragment in "$@"; do
    if [[ "$command" != *"$fragment"* ]]; then
      echo "artifact replayCommand must contain '$fragment': $file" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
  done
}

require_ack_evidence() {
  local file="$1"
  local bytes_key="$2"
  local sha_key="$3"
  require_artifact_value_matches "$file" "$bytes_key" '^[1-9][0-9]*$'
  require_artifact_value_matches "$file" "$sha_key" '^[0-9a-f]{64}$'
}

require_descriptor_evidence() {
  local file="$1"
  local label="$2"
  local expected_file_state="$3"
  local expected_chunk_state="${4:-}"
  local chunk_count
  require_artifact_value "$file" "$label.fileState" "$expected_file_state"
  require_artifact_value_matches "$file" "$label.chunkCount" '^[1-9][0-9]*$'
  chunk_count=$(artifact_value "$file" "$label.chunkCount")
  require_artifact_line_count_starting_with "$file" "$label.chunk=" "$chunk_count"
  if [[ -n "$expected_chunk_state" ]]; then
    require_artifact_prefixed_lines_containing "$file" "$label.chunk=" " state=$expected_chunk_state "
  fi
  require_artifact_prefixed_lines_containing "$file" "$label.chunk=" " length="
  require_artifact_prefixed_lines_containing "$file" "$label.chunk=" " crc="
  require_artifact_prefixed_lines_containing "$file" "$label.chunk=" " writeEpoch="
  require_artifact_prefixed_lines_containing "$file" "$label.chunk=" " replicas=["
}

artifact_descriptor_replication_factor() {
  local file="$1"
  local label="$2"
  local policy
  policy=$(artifact_value "$file" "$label.policy")
  if [[ ! "$policy" =~ ^rf([1-9][0-9]*)-aq[1-9][0-9]*-fsync(true|false)$ ]]; then
    echo "artifact $label.policy value '$policy' does not expose replication factor: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
  printf '%s\n' "${BASH_REMATCH[1]}"
}

require_descriptor_full_replica_count() {
  local file="$1"
  local label="$2"
  local expected
  expected=$(artifact_descriptor_replication_factor "$file" "$label") || return 1

  local prefix="$label.chunk="
  local found=false
  local line
  while IFS= read -r line; do
    found=true
    local replicas="${line#*replicas=\[}"
    if [[ "$replicas" == "$line" || "$replicas" != *"]"* ]]; then
      echo "artifact $label chunk is missing parseable replicas list: $file: $line" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
    replicas="${replicas%%\]*}"

    local count=0
    local seen_node_ids="|"
    if [[ -n "${replicas//[[:space:]]/}" ]]; then
      local entries=()
      local entry
      IFS=',' read -r -a entries <<< "$replicas"
      for entry in "${entries[@]}"; do
        entry="${entry#"${entry%%[![:space:]]*}"}"
        entry="${entry%"${entry##*[![:space:]]}"}"
        if [[ -z "$entry" || "$entry" != *@* ]]; then
          echo "artifact $label chunk has unparseable replica entry '$entry': $file: $line" >&2
          ARTIFACT_AUDIT_FAILED=true
          return 1
        fi
        local node_id="${entry%%@*}"
        if [[ -z "$node_id" || "$seen_node_ids" == *"|$node_id|"* ]]; then
          echo "artifact $label chunk has duplicate or empty replica node id '$node_id': $file: $line" >&2
          ARTIFACT_AUDIT_FAILED=true
          return 1
        fi
        seen_node_ids+="$node_id|"
        count=$((count + 1))
      done
    fi

    if [[ "$count" != "$expected" ]]; then
      echo "artifact $label chunk has $count replicas, expected $expected from $label.policy: $file: $line" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
  done < <(awk -v prefix="$prefix" 'index($0, prefix) == 1 { print }' "$file")

  if ! $found; then
    echo "artifact missing $label chunk lines for replica-count audit: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

require_descriptor_length_sum_equals_key() {
  local file="$1"
  local label="$2"
  local expected_key="$3"
  local expected
  expected=$(artifact_value "$file" "$expected_key")
  if [[ ! "$expected" =~ ^[0-9]+$ ]]; then
    echo "artifact $expected_key value '$expected' is not a byte count: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi

  local prefix="$label.chunk="
  local found=false
  local sum=0
  local line
  while IFS= read -r line; do
    found=true
    local length="${line#* length=}"
    if [[ "$length" == "$line" ]]; then
      echo "artifact $label chunk is missing length field: $file: $line" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
    length="${length%% *}"
    if [[ ! "$length" =~ ^[0-9]+$ ]]; then
      echo "artifact $label chunk has invalid length '$length': $file: $line" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
    fi
    sum=$((sum + length))
  done < <(awk -v prefix="$prefix" 'index($0, prefix) == 1 { print }' "$file")

  if ! $found; then
    echo "artifact missing $label chunk lines for length-sum audit: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
  if [[ "$sum" != "$expected" ]]; then
    echo "artifact $label chunk length sum $sum differs from $expected_key=$expected: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

require_soak_iteration_evidence() {
  local file="$1"
  local iterations="$2"
  local batches
  batches=$(artifact_value "$file" "batches")
  require_artifact_line_count_starting_with "$file" "iteration=" "$iterations"
  require_artifact_line_count_starting_with "$file" "iterationReplayCommand=" "$iterations"
  require_artifact_line_count_starting_with "$file" "iterationPassed=" "$iterations"

  local i
  for ((i = 1; i <= iterations; i++)); do
    require_artifact_lines_starting_with "$file" "iteration=$i/$iterations seed="
    require_artifact_lines_starting_with "$file" \
      "iterationReplayCommand=$i ./scripts/verify.sh --skip-default --fault --stress-only "
    require_artifact_lines_starting_with "$file" "iterationPassed=$i seed="
    local seed
    seed=$(awk -v prefix="iteration=$i/$iterations seed=" '
      index($0, prefix) == 1 {
        print substr($0, length(prefix) + 1)
        exit
      }
    ' "$file")
    require_artifact_lines_starting_with "$file" \
      "iterationReplayCommand=$i ./scripts/verify.sh --skip-default --fault --stress-only -Dstrata.stress.seed=$seed -Dstrata.stress.batches=$batches"
    require_artifact_lines_starting_with "$file" "iterationPassed=$i seed=$seed"
  done
}

require_artifact_identity() {
  local file="$1"
  local artifact_scenario
  local scenario
  local artifact_id
  local artifact_path
  artifact_scenario=$(artifact_value "$file" "artifact.scenario")
  scenario=$(artifact_value "$file" "scenario")
  artifact_id=$(artifact_value "$file" "artifact.id")
  artifact_path=$(artifact_value "$file" "artifact.path")

  if [[ "$artifact_scenario" != "$scenario" ]]; then
    echo "artifact.scenario=$artifact_scenario differs from scenario=$scenario: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi

  local actual_basename
  local declared_basename
  local expected_basename
  actual_basename=$(basename "$file")
  declared_basename=$(basename "$artifact_path")
  expected_basename="$(artifact_safe_name "$artifact_scenario-$artifact_id").txt"

  if [[ "$declared_basename" != "$actual_basename" ]]; then
    echo "artifact.path basename $declared_basename differs from audited file $actual_basename: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
  if [[ "$actual_basename" != "$expected_basename" ]]; then
    echo "artifact filename $actual_basename does not match expected $expected_basename: $file" >&2
    ARTIFACT_AUDIT_FAILED=true
    return 1
  fi
}

artifact_matches_filters() {
  local file="$1"
  shift
  local filter
  for filter in "$@"; do
    local key="${filter%%=*}"
    local expected="${filter#*=}"
    local actual
    actual=$(artifact_value "$file" "$key")
    if [[ "$actual" != "$expected" ]]; then
      return 1
    fi
  done
}

require_correctness_artifact_matching() {
  local description="$1"
  shift
  local count=0
  local file
  while IFS= read -r -d '' file; do
    if artifact_matches_filters "$file" "$@"; then
      count=$((count + 1))
    fi
  done < <(find "$CORRECTNESS_ARTIFACT_ROOT" -type f -name '*.txt' -print0)

  if [[ "$count" -eq 0 ]]; then
    echo "missing correctness artifact for $description ($*): $CORRECTNESS_ARTIFACT_ROOT" >&2
    return 1
  fi
  if [[ "$count" -ne 1 ]]; then
    echo "expected exactly one correctness artifact for $description, found $count ($*): $CORRECTNESS_ARTIFACT_ROOT" >&2
    return 1
  fi
}

require_stress_fault_case_artifacts() {
  local failed=false
  local fault_case
  for fault_case in "${STRESS_FAULT_ARTIFACT_CASES[@]}"; do
    if ! require_correctness_artifact_matching "stress/fault case $fault_case" \
      "scenario=stress-fault" "case=$fault_case"; then
      failed=true
    fi
  done
  if $failed; then
    return 1
  fi
}

require_exact_stress_fault_artifact() {
  local fault_case="$1"
  require_correctness_artifact_matching "stress/fault exact case $fault_case" \
    "scenario=stress-fault" "case=$fault_case"
}

require_process_crash_artifacts() {
  local failed=false
  local crash_case
  for crash_case in "${PROCESS_CRASH_ARTIFACT_CASES[@]}"; do
    if ! require_correctness_artifact_matching "process crash case $crash_case" \
      "scenario=process-crash-recovery" "case=$crash_case"; then
      failed=true
    fi
  done
  if $failed; then
    return 1
  fi
}

require_full_fault_gate_artifacts() {
  local failed=false
  if ! require_stress_fault_case_artifacts; then
    failed=true
  fi
  if ! require_process_crash_artifacts; then
    failed=true
  fi
  if ! require_correctness_artifact_matching "process stress/fault schedule" \
    "scenario=process-stress-fault"; then
    failed=true
  fi
  if ! require_correctness_artifact_matching "full process cluster restart schedule" \
    "scenario=full-process-cluster-restart"; then
    failed=true
  fi
  if $failed; then
    return 1
  fi
}

require_chaos_gate_artifacts() {
  local failed=false
  if ! require_correctness_artifact_matching "external storage and metadata nemesis" \
    "scenario=external-nemesis"; then
    failed=true
  fi
  if ! require_correctness_artifact_matching "external concurrent client nemesis" \
    "scenario=external-concurrent-nemesis"; then
    failed=true
  fi
  if ! require_correctness_artifact_matching "external storage-control nemesis" \
    "scenario=external-control-nemesis"; then
    failed=true
  fi
  if $failed; then
    return 1
  fi
}

require_soak_gate_artifacts() {
  require_correctness_artifact_matching "stress/fault soak" \
    "scenario=stress-fault-soak"
}

audit_scenario_artifact() {
  local file="$1"
  local scenario
  scenario=$(artifact_value "$file" "scenario")
  ARTIFACT_AUDIT_FAILED=false

  case "$scenario" in
    stress-fault)
      require_artifact_keys "$file" \
        case seed batches writePolicy storageConnectionsPerEndpoint nodeCount \
        initialMetadataEndpoints initialStorage fileId leaderAvailableBeforeSeal \
        liveDescriptorVerifiedBeforeSeal sealedLength finalAckedBytes finalAckedSha256 \
        fullReplicaConsistencyAfterRepair finalDescriptor.fileState finalDescriptor.policy \
        finalDescriptor.chunkCount
      require_replay_command_contains "$file" \
        "./scripts/verify.sh --skip-default --fault" \
        "-Dstrata.stress.case=$(artifact_value "$file" "case")" \
        "-Dstrata.stress.seed=$(artifact_value "$file" "seed")" \
        "-Dstrata.stress.batches=$(artifact_value "$file" "batches")"
      require_artifact_equal_keys "$file" "writePolicy" "finalDescriptor.policy"
      require_artifact_value "$file" "fullReplicaConsistencyAfterRepair" "true"
      require_artifact_equal_keys "$file" "sealedLength" "finalAckedBytes"
      require_ack_evidence "$file" "finalAckedBytes" "finalAckedSha256"
      require_artifact_lines_starting_with "$file" \
        "batch=" "fault=" "openReadVerifiedBatch=" "liveDescriptorVerifiedBatch=" \
        "finalDescriptor.chunk="
      require_artifact_lines_containing "$file" " ackedBytes=" " ackedSha256="
      require_descriptor_evidence "$file" "finalDescriptor" "1" "SEALED"
      require_descriptor_full_replica_count "$file" "finalDescriptor"
      require_descriptor_length_sum_equals_key "$file" "finalDescriptor" "finalAckedBytes"
      ;;
    process-stress-fault)
      require_artifact_keys "$file" \
        seed batches writePolicy storageConnectionsPerEndpoint initialMetadataEndpoints \
        initialStorage fileId leaderAvailableBeforeSeal liveDescriptorVerifiedBeforeSeal \
        sealedLength finalAckedBytes finalAckedSha256 fullReplicaConsistencyAfterRepair \
        finalDescriptor.fileState finalDescriptor.policy finalDescriptor.chunkCount
      require_replay_command_contains "$file" \
        "./scripts/verify.sh --skip-default --fault" \
        "-Dtest=MetadataProcessFailoverTest#deterministicProcessFaultSchedulePreservesAckedBytes"
      require_artifact_equal_keys "$file" "writePolicy" "finalDescriptor.policy"
      require_artifact_value "$file" "fullReplicaConsistencyAfterRepair" "true"
      require_artifact_equal_keys "$file" "sealedLength" "finalAckedBytes"
      require_ack_evidence "$file" "finalAckedBytes" "finalAckedSha256"
      require_artifact_lines_starting_with "$file" \
        "batch=" "fault=" "openReadVerifiedBatch=" "liveDescriptorVerifiedBatch=" \
        "finalDescriptor.chunk="
      require_artifact_lines_containing "$file" " ackedBytes=" " ackedSha256="
      require_descriptor_evidence "$file" "finalDescriptor" "1" "SEALED"
      require_descriptor_full_replica_count "$file" "finalDescriptor"
      require_descriptor_length_sum_equals_key "$file" "finalDescriptor" "finalAckedBytes"
      ;;
    process-crash-recovery)
      require_artifact_keys "$file" \
        case recordCount metadataServiceCount metadataFailoverDuringRecovery \
        writePolicy storageConnectionsPerEndpoint nodeCount initialMetadataEndpoints \
        initialStorage fileId ackedBeforeCrashBytes ackedBeforeCrashSha256 \
        allOpenChunkReplicasKilled liveDescriptorVerifiedBeforeCrash \
        liveDescriptorVerifiedAfterRestart openChunkBeforeCrash restartedReplicaNodeIds \
        sealedLength finalAckedBytes finalAckedSha256 fullReplicaConsistencyAfterRepair \
        beforeCrashDescriptor.fileState beforeCrashDescriptor.policy \
        beforeCrashDescriptor.chunkCount finalDescriptor.fileState \
        finalDescriptor.policy finalDescriptor.chunkCount
      require_replay_command_contains "$file" "./scripts/verify.sh --skip-default --fault"
      if [[ "$(artifact_value "$file" "metadataFailoverDuringRecovery")" == "true" ]]; then
        require_replay_command_contains "$file" \
          "-Dtest=ProcessCrashRecoveryTest#storageProcessCrashThenMetadataFailoverBeforeRecoveryPreservesAckedOpenChunk"
      else
        require_replay_command_contains "$file" \
          "-Dtest=ProcessCrashRecoveryTest#forciblyKilledStorageProcessesRecoverAckedOpenChunkAcrossPolicies" \
          "-Dstrata.processCrash.case=$(artifact_value "$file" "case")"
      fi
      require_artifact_equal_keys "$file" "writePolicy" "beforeCrashDescriptor.policy"
      require_artifact_equal_keys "$file" "writePolicy" "finalDescriptor.policy"
      require_artifact_value "$file" "allOpenChunkReplicasKilled" "true"
      require_artifact_value "$file" "liveDescriptorVerifiedBeforeCrash" "true"
      require_artifact_value "$file" "liveDescriptorVerifiedAfterRestart" "true"
      require_artifact_value "$file" "fullReplicaConsistencyAfterRepair" "true"
      require_artifact_value_matches "$file" "metadataFailoverDuringRecovery" '^(true|false)$'
      if [[ "$(artifact_value "$file" "metadataFailoverDuringRecovery")" == "true" ]]; then
        require_artifact_keys "$file" "metadataLeaderAfterFault"
        require_artifact_lines_starting_with "$file" "fault=kill-metadata-leader-before-recovery"
      fi
      require_artifact_equal_keys "$file" "ackedBeforeCrashBytes" "finalAckedBytes"
      require_artifact_equal_keys "$file" "ackedBeforeCrashSha256" "finalAckedSha256"
      require_artifact_equal_keys "$file" "sealedLength" "finalAckedBytes"
      require_ack_evidence "$file" "ackedBeforeCrashBytes" "ackedBeforeCrashSha256"
      require_ack_evidence "$file" "finalAckedBytes" "finalAckedSha256"
      require_artifact_lines_starting_with "$file" \
        "fault=kill-open-replica-process" "fault=restarted-open-replica-process" \
        "beforeCrashDescriptor.chunk=" "finalDescriptor.chunk="
      require_descriptor_evidence "$file" "beforeCrashDescriptor" "0"
      require_descriptor_evidence "$file" "finalDescriptor" "1" "SEALED"
      require_descriptor_full_replica_count "$file" "finalDescriptor"
      require_descriptor_length_sum_equals_key "$file" "finalDescriptor" "finalAckedBytes"
      ;;
    full-process-cluster-restart)
      require_artifact_keys "$file" \
        seed writePolicy initialMetadataEndpoints initialStorage fileId ackedBytes ackedSha256 \
        allProcessesKilled restartedMetadataEndpoints restartedLeader restartedStorage \
        liveDescriptorVerifiedBeforeCrash liveDescriptorVerifiedAfterFullRestart sealedLength \
        finalAckedBytes finalAckedSha256 beforeRestartDescriptor.fileState \
        beforeRestartDescriptor.policy beforeRestartDescriptor.chunkCount \
        finalDescriptor.fileState finalDescriptor.policy finalDescriptor.chunkCount
      require_replay_command_contains "$file" \
        "./scripts/verify.sh --skip-default --fault" \
        "-Dtest=MetadataProcessFailoverTest#fullProcessClusterRestartRecoversAckedOpenChunkFromSameDisks"
      require_artifact_equal_keys "$file" "writePolicy" "beforeRestartDescriptor.policy"
      require_artifact_equal_keys "$file" "writePolicy" "finalDescriptor.policy"
      require_artifact_value "$file" "allProcessesKilled" "true"
      require_artifact_value "$file" "liveDescriptorVerifiedBeforeCrash" "true"
      require_artifact_value "$file" "liveDescriptorVerifiedAfterFullRestart" "true"
      require_artifact_equal_keys "$file" "ackedBytes" "finalAckedBytes"
      require_artifact_equal_keys "$file" "ackedSha256" "finalAckedSha256"
      require_artifact_equal_keys "$file" "sealedLength" "finalAckedBytes"
      require_ack_evidence "$file" "finalAckedBytes" "finalAckedSha256"
      require_artifact_lines_starting_with "$file" \
        "beforeRestartDescriptor.chunk=" "finalDescriptor.chunk="
      require_descriptor_evidence "$file" "beforeRestartDescriptor" "0"
      require_descriptor_evidence "$file" "finalDescriptor" "1" "SEALED"
      require_descriptor_full_replica_count "$file" "finalDescriptor"
      require_descriptor_length_sum_equals_key "$file" "finalDescriptor" "finalAckedBytes"
      ;;
    external-nemesis)
      require_artifact_keys "$file" \
        seed schedule initialLeader clientMetadataEndpoints proxiedStorageEndpoint \
        writePolicy storageConnectionsPerEndpoint initialStorage fileId \
        openReadVerifiedAfterStoragePartition liveDescriptorVerifiedAfterStoragePartition \
        sealedLength finalAckedBytes finalAckedSha256 fullReplicaConsistencyAfterRepair \
        finalDescriptor.fileState finalDescriptor.policy finalDescriptor.chunkCount
      require_replay_command_contains "$file" \
        "./scripts/verify.sh --skip-default --chaos" \
        "-Dstrata.external.seed=$(artifact_value "$file" "seed")" \
        "-Dtest=ExternalNemesisTest#childProcessStoragePartitionAndMetadataLeaderLossPreserveAckedBytes"
      require_artifact_equal_keys "$file" "writePolicy" "finalDescriptor.policy"
      require_artifact_value "$file" "openReadVerifiedAfterStoragePartition" "true"
      require_artifact_value "$file" "liveDescriptorVerifiedAfterStoragePartition" "true"
      require_artifact_value "$file" "fullReplicaConsistencyAfterRepair" "true"
      require_artifact_equal_keys "$file" "sealedLength" "finalAckedBytes"
      require_ack_evidence "$file" "finalAckedBytes" "finalAckedSha256"
      require_artifact_lines_starting_with "$file" "batch=" "fault=" "finalDescriptor.chunk="
      require_artifact_lines_containing "$file" " ackedBytes=" " ackedSha256="
      require_descriptor_evidence "$file" "finalDescriptor" "1" "SEALED"
      require_descriptor_full_replica_count "$file" "finalDescriptor"
      require_descriptor_length_sum_equals_key "$file" "finalDescriptor" "finalAckedBytes"
      ;;
    external-concurrent-nemesis)
      require_artifact_keys "$file" \
        seed schedule initialLeader clientMetadataEndpoints proxiedStorageEndpoint \
        writePolicy storageConnectionsPerEndpoint concurrentFiles initialStorage \
        primaryFileId secondaryFileId crossClientOpenReadVerifiedBeforePartition \
        longLivedReaderVerifiedBeforePartition crossClientOpenReadVerifiedAfterStoragePartition \
        longLivedReaderVerifiedAfterStoragePartition liveDescriptorVerifiedAfterStoragePartition \
        primarySealedLength primaryFinalAckedBytes primaryFinalAckedSha256 \
        primaryFullReplicaConsistencyAfterRepair secondarySealedLength \
        secondaryFinalAckedBytes secondaryFinalAckedSha256 \
        secondaryFullReplicaConsistencyAfterRepair primaryFinalDescriptor.fileState \
        primaryFinalDescriptor.policy primaryFinalDescriptor.chunkCount \
        secondaryFinalDescriptor.fileState secondaryFinalDescriptor.policy \
        secondaryFinalDescriptor.chunkCount
      require_replay_command_contains "$file" \
        "./scripts/verify.sh --skip-default --chaos" \
        "-Dstrata.external.seed=$(artifact_value "$file" "seed")" \
        "-Dtest=ExternalNemesisTest#concurrentClientFilesSurviveSharedStorageAndMetadataFaults"
      require_artifact_equal_keys "$file" "writePolicy" "primaryFinalDescriptor.policy"
      require_artifact_equal_keys "$file" "writePolicy" "secondaryFinalDescriptor.policy"
      require_artifact_value "$file" "concurrentFiles" "2"
      require_artifact_value "$file" "crossClientOpenReadVerifiedBeforePartition" "true"
      require_artifact_value "$file" "longLivedReaderVerifiedBeforePartition" "true"
      require_artifact_value "$file" "crossClientOpenReadVerifiedAfterStoragePartition" "true"
      require_artifact_value "$file" "longLivedReaderVerifiedAfterStoragePartition" "true"
      require_artifact_value "$file" "liveDescriptorVerifiedAfterStoragePartition" "true"
      require_artifact_value "$file" "primaryFullReplicaConsistencyAfterRepair" "true"
      require_artifact_value "$file" "secondaryFullReplicaConsistencyAfterRepair" "true"
      require_artifact_equal_keys "$file" "primarySealedLength" "primaryFinalAckedBytes"
      require_artifact_equal_keys "$file" "secondarySealedLength" "secondaryFinalAckedBytes"
      require_ack_evidence "$file" "primaryFinalAckedBytes" "primaryFinalAckedSha256"
      require_ack_evidence "$file" "secondaryFinalAckedBytes" "secondaryFinalAckedSha256"
      require_artifact_lines_starting_with "$file" \
        "batch=" "fault=" "primaryFinalDescriptor.chunk=" "secondaryFinalDescriptor.chunk="
      require_artifact_lines_containing "$file" " ackedBytes=" " ackedSha256="
      require_descriptor_evidence "$file" "primaryFinalDescriptor" "1" "SEALED"
      require_descriptor_evidence "$file" "secondaryFinalDescriptor" "1" "SEALED"
      require_descriptor_full_replica_count "$file" "primaryFinalDescriptor"
      require_descriptor_full_replica_count "$file" "secondaryFinalDescriptor"
      require_descriptor_length_sum_equals_key "$file" "primaryFinalDescriptor" "primaryFinalAckedBytes"
      require_descriptor_length_sum_equals_key "$file" "secondaryFinalDescriptor" "secondaryFinalAckedBytes"
      ;;
    external-control-nemesis)
      require_artifact_keys "$file" \
        seed schedule metadataLeader proxiedMetadataEndpoint writePolicy \
        storageConnectionsPerEndpoint initialStorage fileId spareNodeId spareDataEndpoint \
        sealedLength finalAckedBytes finalAckedSha256 initialDescriptor.fileState \
        initialDescriptor.policy initialDescriptor.chunkCount duringControlPartitionUnderReplicated \
        duringControlPartition.fileState duringControlPartition.policy \
        duringControlPartition.chunkCount fullReplicaConsistencyAfterRepair \
        finalDescriptor.fileState finalDescriptor.policy finalDescriptor.chunkCount
      require_replay_command_contains "$file" \
        "./scripts/verify.sh --skip-default --chaos" \
        "-Dstrata.external.seed=$(artifact_value "$file" "seed")" \
        "-Dtest=ExternalNemesisTest#storageControlPartitionDelaysRepairUntilHeartbeatPathHeals"
      require_artifact_equal_keys "$file" "writePolicy" "initialDescriptor.policy"
      require_artifact_equal_keys "$file" "writePolicy" "duringControlPartition.policy"
      require_artifact_equal_keys "$file" "writePolicy" "finalDescriptor.policy"
      require_artifact_value "$file" "duringControlPartitionUnderReplicated" "true"
      require_artifact_value "$file" "fullReplicaConsistencyAfterRepair" "true"
      require_artifact_equal_keys "$file" "sealedLength" "finalAckedBytes"
      require_ack_evidence "$file" "finalAckedBytes" "finalAckedSha256"
      require_artifact_lines_starting_with "$file" \
        "batch=" "fault=" "initialDescriptor.chunk=" "duringControlPartition.chunk=" \
        "finalDescriptor.chunk="
      require_artifact_lines_containing "$file" " ackedBytes=" " ackedSha256="
      require_descriptor_evidence "$file" "initialDescriptor" "1" "SEALED"
      require_descriptor_evidence "$file" "duringControlPartition" "1" "SEALED"
      require_descriptor_evidence "$file" "finalDescriptor" "1" "SEALED"
      require_descriptor_full_replica_count "$file" "initialDescriptor"
      require_descriptor_full_replica_count "$file" "finalDescriptor"
      require_descriptor_length_sum_equals_key "$file" "finalDescriptor" "finalAckedBytes"
      ;;
    stress-fault-soak)
      require_artifact_keys "$file" iterations batches baseSeed seedStep
      require_replay_command_contains "$file" \
        "./scripts/verify.sh --skip-default --soak" \
        "-Dstrata.soak.seed=$(artifact_value "$file" "baseSeed")" \
        "-Dstrata.soak.iterations=$(artifact_value "$file" "iterations")" \
        "-Dstrata.soak.batches=$(artifact_value "$file" "batches")"
      require_artifact_value_matches "$file" "iterations" '^[1-9][0-9]*$'
      require_artifact_value_matches "$file" "batches" '^[1-9][0-9]*$'
      require_soak_iteration_evidence "$file" "$(artifact_value "$file" "iterations")"
      ;;
    *)
      echo "unknown correctness artifact scenario '$scenario': $file" >&2
      ARTIFACT_AUDIT_FAILED=true
      return 1
      ;;
  esac

  if ${ARTIFACT_AUDIT_FAILED:-false}; then
    return 1
  fi
}

audit_correctness_artifacts() {
  local root="$CORRECTNESS_ARTIFACT_ROOT"
  if [[ ! -d "$root" ]]; then
    echo "correctness artifact directory missing: $root" >&2
    return 1
  fi

  local files=()
  local file
  while IFS= read -r -d '' file; do
    files+=("$file")
  done < <(find "$root" -type f -name '*.txt' -print0)

  if [[ ${#files[@]} -eq 0 ]]; then
    echo "no correctness artifacts produced under $root" >&2
    return 1
  fi

  local failed=false
  if grep -R -n -E '^(status=failed|failureStackTrace\.)' "$root"; then
    failed=true
  fi

  for file in "${files[@]}"; do
    local passed_count
    passed_count=$(grep -c '^status=passed$' "$file" || true)
    if [[ "$passed_count" != "1" ]]; then
      echo "artifact must contain exactly one status=passed: $file" >&2
      failed=true
    fi
    local key
    for key in artifact.scenario artifact.id artifact.path scenario replayCommand \
        runtime.javaVersion runtime.osName runtime.availableProcessors; do
      if ! artifact_has_nonempty_key "$file" "$key"; then
        echo "artifact missing non-empty $key: $file" >&2
        failed=true
      fi
    done
    if ! require_artifact_identity "$file"; then
      failed=true
    fi
    if ! audit_scenario_artifact "$file"; then
      failed=true
    fi
  done

  if $failed; then
    return 1
  fi
  echo "==> correctness artifacts audited (${#files[@]} files)"
}

write_artifact_header() {
  local file="$1"
  local scenario="$2"
  local id="$3"
  {
    printf 'artifact.scenario=%s\n' "$scenario"
    printf 'artifact.id=%s\n' "$id"
    printf 'artifact.path=%s\n' "$file"
    printf 'artifact.createdAtEpochMs=1\n'
    printf 'runtime.javaVersion=21\n'
    printf 'runtime.osName=Linux\n'
    printf 'runtime.availableProcessors=2\n'
    printf 'scenario=%s\n' "$scenario"
  } > "$file"
}

fixture_policy_for_case() {
  local case_name="$1"
  local fsync="false"
  if [[ "$case_name" == *fsync* ]]; then
    fsync="true"
  fi
  case "$case_name" in
    rf4-aq3*) printf 'rf4-aq3-fsync%s\n' "$fsync" ;;
    rf3-aq2* | meta-failover-rf3-aq2-fsync) printf 'rf3-aq2-fsync%s\n' "$fsync" ;;
    *)
      echo "unknown fixture policy case: $case_name" >&2
      return 1
      ;;
  esac
}

fixture_replication_factor_for_policy() {
  local policy="$1"
  if [[ ! "$policy" =~ ^rf([1-9][0-9]*)-aq[1-9][0-9]*-fsync(true|false)$ ]]; then
    echo "unknown fixture policy: $policy" >&2
    return 1
  fi
  printf '%s\n' "${BASH_REMATCH[1]}"
}

fixture_replicas() {
  local replication_factor="$1"
  local first_port="$2"
  local out="["
  local node
  for ((node = 1; node <= replication_factor; node++)); do
    if [[ "$node" -gt 1 ]]; then
      out+=", "
    fi
    out+="$node@127.0.0.1:$((first_port + node - 1))"
  done
  out+="]"
  printf '%s\n' "$out"
}

fixture_node_ids() {
  local count="$1"
  local out="["
  local node
  for ((node = 1; node <= count; node++)); do
    if [[ "$node" -gt 1 ]]; then
      out+=", "
    fi
    out+="$node"
  done
  out+="]"
  printf '%s\n' "$out"
}

write_stress_fault_fixture_artifact() {
  local root="$1"
  local fault_case="$2"
  local file="$root/stress-fault-$fault_case.txt"
  local sha="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local policy
  policy=$(fixture_policy_for_case "$fault_case")
  local replication_factor
  replication_factor=$(fixture_replication_factor_for_policy "$policy")
  local replicas
  replicas=$(fixture_replicas "$replication_factor" 2)
  write_artifact_header "$file" "stress-fault" "$fault_case"
  {
    printf 'case=%s\n' "$fault_case"
    printf 'seed=abc123\n'
    printf 'batches=30\n'
    printf 'replayCommand=./scripts/verify.sh --skip-default --fault -Dstrata.stress.case=%s -Dstrata.stress.seed=abc123 -Dstrata.stress.batches=30\n' "$fault_case"
    printf 'writePolicy=%s\n' "$policy"
    printf 'storageConnectionsPerEndpoint=1\n'
    printf 'nodeCount=5\n'
    printf 'initialMetadataEndpoints=[127.0.0.1:1]\n'
    printf 'initialStorage=[host-0#1@127.0.0.1:2]\n'
    printf 'fileId=file-1\n'
    printf 'leaderAvailableBeforeSeal=true\n'
    printf 'liveDescriptorVerifiedBeforeSeal=true\n'
    printf 'sealedLength=10\n'
    printf 'finalAckedBytes=10\n'
    printf 'finalAckedSha256=%s\n' "$sha"
    printf 'fullReplicaConsistencyAfterRepair=true\n'
    printf 'finalDescriptor.fileState=1\n'
    printf 'finalDescriptor.policy=%s\n' "$policy"
    printf 'finalDescriptor.chunkCount=1\n'
    printf 'batch=0 appendedRecords=1 ackedBytes=10 ackedSha256=%s\n' "$sha"
    printf 'fault=synthetic\n'
    printf 'openReadVerifiedBatch=0\n'
    printf 'liveDescriptorVerifiedBatch=0\n'
    printf 'finalDescriptor.chunk=file.0 state=SEALED length=10 crc=1 writeEpoch=1 replicas=%s\n' "$replicas"
    printf 'status=passed\n'
  } >> "$file"
}

write_process_stress_fixture_artifact() {
  local root="$1"
  local file="$root/process-stress-fault-process.txt"
  local sha="1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local replicas
  replicas=$(fixture_replicas 3 2)
  write_artifact_header "$file" "process-stress-fault" "process"
  {
    printf 'seed=def456\n'
    printf 'batches=24\n'
    printf 'replayCommand=./scripts/verify.sh --skip-default --fault -Dtest=MetadataProcessFailoverTest#deterministicProcessFaultSchedulePreservesAckedBytes\n'
    printf 'writePolicy=rf3-aq2-fsynctrue\n'
    printf 'storageConnectionsPerEndpoint=2\n'
    printf 'initialMetadataEndpoints=[127.0.0.1:1]\n'
    printf 'initialStorage=[host-0#1@127.0.0.1:2]\n'
    printf 'fileId=file-2\n'
    printf 'leaderAvailableBeforeSeal=true\n'
    printf 'liveDescriptorVerifiedBeforeSeal=true\n'
    printf 'sealedLength=11\n'
    printf 'finalAckedBytes=11\n'
    printf 'finalAckedSha256=%s\n' "$sha"
    printf 'fullReplicaConsistencyAfterRepair=true\n'
    printf 'finalDescriptor.fileState=1\n'
    printf 'finalDescriptor.policy=rf3-aq2-fsynctrue\n'
    printf 'finalDescriptor.chunkCount=1\n'
    printf 'batch=0 appendedRecords=1 ackedBytes=11 ackedSha256=%s\n' "$sha"
    printf 'fault=synthetic-process\n'
    printf 'openReadVerifiedBatch=0\n'
    printf 'liveDescriptorVerifiedBatch=0\n'
    printf 'finalDescriptor.chunk=file.0 state=SEALED length=11 crc=1 writeEpoch=1 replicas=%s\n' "$replicas"
    printf 'status=passed\n'
  } >> "$file"
}

write_process_crash_fixture_artifact() {
  local root="$1"
  local crash_case="$2"
  local file="$root/process-crash-recovery-$crash_case.txt"
  local sha="3123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local metadata_service_count=1
  local metadata_failover=false
  local policy
  policy=$(fixture_policy_for_case "$crash_case")
  local replication_factor
  replication_factor=$(fixture_replication_factor_for_policy "$policy")
  local before_crash_replicas
  before_crash_replicas=$(fixture_replicas "$replication_factor" 2)
  local final_replicas
  final_replicas=$(fixture_replicas "$replication_factor" 3)
  local replica_node_ids
  replica_node_ids=$(fixture_node_ids "$replication_factor")
  if [[ "$crash_case" == meta-failover-* ]]; then
    metadata_service_count=2
    metadata_failover=true
  fi
  write_artifact_header "$file" "process-crash-recovery" "$crash_case"
  {
    printf 'case=%s\n' "$crash_case"
    printf 'recordCount=180\n'
    printf 'metadataServiceCount=%s\n' "$metadata_service_count"
    printf 'metadataFailoverDuringRecovery=%s\n' "$metadata_failover"
    if [[ "$metadata_failover" == "true" ]]; then
      printf 'replayCommand=./scripts/verify.sh --skip-default --fault -Dtest=ProcessCrashRecoveryTest#storageProcessCrashThenMetadataFailoverBeforeRecoveryPreservesAckedOpenChunk\n'
    else
      printf 'replayCommand=./scripts/verify.sh --skip-default --fault -Dtest=ProcessCrashRecoveryTest#forciblyKilledStorageProcessesRecoverAckedOpenChunkAcrossPolicies -Dstrata.processCrash.case=%s\n' "$crash_case"
    fi
    printf 'writePolicy=%s\n' "$policy"
    printf 'storageConnectionsPerEndpoint=1\n'
    printf 'nodeCount=%s\n' "$replication_factor"
    printf 'initialMetadataEndpoints=[127.0.0.1:1]\n'
    printf 'initialStorage=[host-0#1@127.0.0.1:2]\n'
    printf 'fileId=file-4\n'
    printf 'ackedBeforeCrashBytes=13\n'
    printf 'ackedBeforeCrashSha256=%s\n' "$sha"
    printf 'allOpenChunkReplicasKilled=true\n'
    printf 'liveDescriptorVerifiedBeforeCrash=true\n'
    printf 'liveDescriptorVerifiedAfterRestart=true\n'
    printf 'openChunkBeforeCrash=file.0\n'
    printf 'restartedReplicaNodeIds=%s\n' "$replica_node_ids"
    printf 'sealedLength=13\n'
    printf 'finalAckedBytes=13\n'
    printf 'finalAckedSha256=%s\n' "$sha"
    printf 'fullReplicaConsistencyAfterRepair=true\n'
    printf 'beforeCrashDescriptor.fileState=0\n'
    printf 'beforeCrashDescriptor.policy=%s\n' "$policy"
    printf 'beforeCrashDescriptor.chunkCount=1\n'
    printf 'finalDescriptor.fileState=1\n'
    printf 'finalDescriptor.policy=%s\n' "$policy"
    printf 'finalDescriptor.chunkCount=1\n'
    printf 'fault=kill-open-replica-process nodeId=1 endpoint=127.0.0.1:2 openChunk=file.0\n'
    if [[ "$metadata_failover" == "true" ]]; then
      printf 'fault=kill-metadata-leader-before-recovery\n'
      printf 'metadataLeaderAfterFault=127.0.0.1:5\n'
    fi
    printf 'fault=restarted-open-replica-process nodeId=1 endpoint=127.0.0.1:3 openChunk=file.0\n'
    printf 'beforeCrashDescriptor.chunk=file.0 state=OPEN length=0 crc=0 writeEpoch=1 replicas=%s\n' "$before_crash_replicas"
    printf 'finalDescriptor.chunk=file.0 state=SEALED length=13 crc=1 writeEpoch=2 replicas=%s\n' "$final_replicas"
    printf 'status=passed\n'
  } >> "$file"
}

write_full_restart_fixture_artifact() {
  local root="$1"
  local file="$root/full-process-cluster-restart-restart.txt"
  local sha="2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local before_restart_replicas
  before_restart_replicas=$(fixture_replicas 3 2)
  local final_replicas
  final_replicas=$(fixture_replicas 3 4)
  write_artifact_header "$file" "full-process-cluster-restart" "restart"
  {
    printf 'seed=abc789\n'
    printf 'replayCommand=./scripts/verify.sh --skip-default --fault -Dtest=MetadataProcessFailoverTest#fullProcessClusterRestartRecoversAckedOpenChunkFromSameDisks\n'
    printf 'writePolicy=rf3-aq2-fsynctrue\n'
    printf 'initialMetadataEndpoints=[127.0.0.1:1]\n'
    printf 'initialStorage=[host-0#1@127.0.0.1:2]\n'
    printf 'fileId=file-3\n'
    printf 'ackedBytes=12\n'
    printf 'ackedSha256=%s\n' "$sha"
    printf 'allProcessesKilled=true\n'
    printf 'restartedMetadataEndpoints=[127.0.0.1:3]\n'
    printf 'restartedLeader=127.0.0.1:3\n'
    printf 'restartedStorage=[host-0#1@127.0.0.1:4]\n'
    printf 'liveDescriptorVerifiedBeforeCrash=true\n'
    printf 'liveDescriptorVerifiedAfterFullRestart=true\n'
    printf 'sealedLength=12\n'
    printf 'finalAckedBytes=12\n'
    printf 'finalAckedSha256=%s\n' "$sha"
    printf 'beforeRestartDescriptor.fileState=0\n'
    printf 'beforeRestartDescriptor.policy=rf3-aq2-fsynctrue\n'
    printf 'beforeRestartDescriptor.chunkCount=1\n'
    printf 'finalDescriptor.fileState=1\n'
    printf 'finalDescriptor.policy=rf3-aq2-fsynctrue\n'
    printf 'finalDescriptor.chunkCount=1\n'
    printf 'beforeRestartDescriptor.chunk=file.0 state=OPEN length=0 crc=0 writeEpoch=1 replicas=%s\n' "$before_restart_replicas"
    printf 'finalDescriptor.chunk=file.0 state=SEALED length=12 crc=1 writeEpoch=2 replicas=%s\n' "$final_replicas"
    printf 'status=passed\n'
  } >> "$file"
}

write_soak_fixture_artifact() {
  local root="$1"
  local file="$root/stress-fault-soak-base-abc-i3-b15.txt"
  write_artifact_header "$file" "stress-fault-soak" "base-abc-i3-b15"
  {
    printf 'iterations=3\n'
    printf 'batches=15\n'
    printf 'baseSeed=abc\n'
    printf 'seedStep=10\n'
    printf 'replayCommand=./scripts/verify.sh --skip-default --soak -Dstrata.soak.seed=abc -Dstrata.soak.iterations=3 -Dstrata.soak.batches=15\n'
    printf 'iteration=1/3 seed=abc\n'
    printf 'iterationReplayCommand=1 ./scripts/verify.sh --skip-default --fault --stress-only -Dstrata.stress.seed=abc -Dstrata.stress.batches=15\n'
    printf 'iterationPassed=1 seed=abc\n'
    printf 'iteration=2/3 seed=acc\n'
    printf 'iterationReplayCommand=2 ./scripts/verify.sh --skip-default --fault --stress-only -Dstrata.stress.seed=acc -Dstrata.stress.batches=15\n'
    printf 'iterationPassed=2 seed=acc\n'
    printf 'iteration=3/3 seed=adc\n'
    printf 'iterationReplayCommand=3 ./scripts/verify.sh --skip-default --fault --stress-only -Dstrata.stress.seed=adc -Dstrata.stress.batches=15\n'
    printf 'iterationPassed=3 seed=adc\n'
    printf 'status=passed\n'
  } >> "$file"
}

write_external_nemesis_fixture_artifact() {
  local root="$1"
  local file="$root/external-nemesis-ext-a.txt"
  local sha="4123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local replicas
  replicas=$(fixture_replicas 4 4)
  write_artifact_header "$file" "external-nemesis" "ext-a"
  {
    printf 'seed=exta\n'
    printf 'replayCommand=./scripts/verify.sh --skip-default --chaos -Dstrata.external.seed=exta -Dtest=ExternalNemesisTest#childProcessStoragePartitionAndMetadataLeaderLossPreserveAckedBytes\n'
    printf 'schedule=partition-storage,metadata-blackhole,kill-leader\n'
    printf 'initialLeader=127.0.0.1:1\n'
    printf 'clientMetadataEndpoints=[127.0.0.1:1, 127.0.0.1:2]\n'
    printf 'proxiedStorageEndpoint=127.0.0.1:3\n'
    printf 'writePolicy=rf4-aq3-fsynctrue\n'
    printf 'storageConnectionsPerEndpoint=2\n'
    printf 'initialStorage=[host-0#1@127.0.0.1:4]\n'
    printf 'fileId=file-ext-a\n'
    printf 'openReadVerifiedAfterStoragePartition=true\n'
    printf 'liveDescriptorVerifiedAfterStoragePartition=true\n'
    printf 'sealedLength=14\n'
    printf 'finalAckedBytes=14\n'
    printf 'finalAckedSha256=%s\n' "$sha"
    printf 'fullReplicaConsistencyAfterRepair=true\n'
    printf 'finalDescriptor.fileState=1\n'
    printf 'finalDescriptor.policy=rf4-aq3-fsynctrue\n'
    printf 'finalDescriptor.chunkCount=1\n'
    printf 'batch=0 appendedRecords=1 ackedBytes=14 ackedSha256=%s\n' "$sha"
    printf 'fault=partition-storage endpoint=127.0.0.1:3\n'
    printf 'finalDescriptor.chunk=file.0 state=SEALED length=14 crc=1 writeEpoch=1 replicas=%s\n' "$replicas"
    printf 'status=passed\n'
  } >> "$file"
}

write_external_concurrent_fixture_artifact() {
  local root="$1"
  local file="$root/external-concurrent-nemesis-ext-b.txt"
  local primary_sha="5123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local secondary_sha="6123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local replicas
  replicas=$(fixture_replicas 4 4)
  write_artifact_header "$file" "external-concurrent-nemesis" "ext-b"
  {
    printf 'seed=extb\n'
    printf 'replayCommand=./scripts/verify.sh --skip-default --chaos -Dstrata.external.seed=extb -Dtest=ExternalNemesisTest#concurrentClientFilesSurviveSharedStorageAndMetadataFaults\n'
    printf 'schedule=two-client-files,partition-storage,metadata-blackhole,kill-leader\n'
    printf 'initialLeader=127.0.0.1:1\n'
    printf 'clientMetadataEndpoints=[127.0.0.1:1, 127.0.0.1:2]\n'
    printf 'proxiedStorageEndpoint=127.0.0.1:3\n'
    printf 'writePolicy=rf4-aq3-fsynctrue\n'
    printf 'storageConnectionsPerEndpoint=2\n'
    printf 'concurrentFiles=2\n'
    printf 'initialStorage=[host-0#1@127.0.0.1:4]\n'
    printf 'primaryFileId=file-ext-b-primary\n'
    printf 'secondaryFileId=file-ext-b-secondary\n'
    printf 'crossClientOpenReadVerifiedBeforePartition=true\n'
    printf 'longLivedReaderVerifiedBeforePartition=true\n'
    printf 'crossClientOpenReadVerifiedAfterStoragePartition=true\n'
    printf 'longLivedReaderVerifiedAfterStoragePartition=true\n'
    printf 'liveDescriptorVerifiedAfterStoragePartition=true\n'
    printf 'primarySealedLength=15\n'
    printf 'primaryFinalAckedBytes=15\n'
    printf 'primaryFinalAckedSha256=%s\n' "$primary_sha"
    printf 'primaryFullReplicaConsistencyAfterRepair=true\n'
    printf 'secondarySealedLength=16\n'
    printf 'secondaryFinalAckedBytes=16\n'
    printf 'secondaryFinalAckedSha256=%s\n' "$secondary_sha"
    printf 'secondaryFullReplicaConsistencyAfterRepair=true\n'
    printf 'primaryFinalDescriptor.fileState=1\n'
    printf 'primaryFinalDescriptor.policy=rf4-aq3-fsynctrue\n'
    printf 'primaryFinalDescriptor.chunkCount=1\n'
    printf 'secondaryFinalDescriptor.fileState=1\n'
    printf 'secondaryFinalDescriptor.policy=rf4-aq3-fsynctrue\n'
    printf 'secondaryFinalDescriptor.chunkCount=1\n'
    printf 'batch=0 file=primary appendedRecords=1 ackedBytes=15 ackedSha256=%s secondaryAckedBytes=16 secondaryAckedSha256=%s\n' "$primary_sha" "$secondary_sha"
    printf 'fault=partition-storage endpoint=127.0.0.1:3\n'
    printf 'primaryFinalDescriptor.chunk=file.0 state=SEALED length=15 crc=1 writeEpoch=1 replicas=%s\n' "$replicas"
    printf 'secondaryFinalDescriptor.chunk=file.0 state=SEALED length=16 crc=1 writeEpoch=1 replicas=%s\n' "$replicas"
    printf 'status=passed\n'
  } >> "$file"
}

write_external_control_fixture_artifact() {
  local root="$1"
  local file="$root/external-control-nemesis-ext-c.txt"
  local sha="7123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  local full_replicas
  full_replicas=$(fixture_replicas 3 4)
  write_artifact_header "$file" "external-control-nemesis" "ext-c"
  {
    printf 'seed=extc\n'
    printf 'replayCommand=./scripts/verify.sh --skip-default --chaos -Dstrata.external.seed=extc -Dtest=ExternalNemesisTest#storageControlPartitionDelaysRepairUntilHeartbeatPathHeals\n'
    printf 'schedule=seal-rf3,start-spare-through-control-proxy,partition-spare-control,kill-replica\n'
    printf 'metadataLeader=127.0.0.1:1\n'
    printf 'proxiedMetadataEndpoint=127.0.0.1:2\n'
    printf 'writePolicy=rf3-aq2-fsynctrue\n'
    printf 'storageConnectionsPerEndpoint=2\n'
    printf 'initialStorage=[host-0#1@127.0.0.1:4]\n'
    printf 'fileId=file-ext-c\n'
    printf 'spareNodeId=4\n'
    printf 'spareDataEndpoint=127.0.0.1:5\n'
    printf 'sealedLength=17\n'
    printf 'finalAckedBytes=17\n'
    printf 'finalAckedSha256=%s\n' "$sha"
    printf 'initialDescriptor.fileState=1\n'
    printf 'initialDescriptor.policy=rf3-aq2-fsynctrue\n'
    printf 'initialDescriptor.chunkCount=1\n'
    printf 'duringControlPartitionUnderReplicated=true\n'
    printf 'duringControlPartition.fileState=1\n'
    printf 'duringControlPartition.policy=rf3-aq2-fsynctrue\n'
    printf 'duringControlPartition.chunkCount=1\n'
    printf 'fullReplicaConsistencyAfterRepair=true\n'
    printf 'finalDescriptor.fileState=1\n'
    printf 'finalDescriptor.policy=rf3-aq2-fsynctrue\n'
    printf 'finalDescriptor.chunkCount=1\n'
    printf 'batch=0 appendedRecords=1 ackedBytes=17 ackedSha256=%s\n' "$sha"
    printf 'fault=partition-storage-control nodeId=4\n'
    printf 'initialDescriptor.chunk=file.0 state=SEALED length=17 crc=1 writeEpoch=1 replicas=%s\n' "$full_replicas"
    printf 'duringControlPartition.chunk=file.0 state=SEALED length=17 crc=1 writeEpoch=1 replicas=[1@127.0.0.1:4]\n'
    printf 'finalDescriptor.chunk=file.0 state=SEALED length=17 crc=1 writeEpoch=1 replicas=%s\n' "$full_replicas"
    printf 'status=passed\n'
  } >> "$file"
}

write_full_fault_fixture_set() {
  local root="$1"
  local fault_case
  mkdir -p "$root"
  for fault_case in "${STRESS_FAULT_ARTIFACT_CASES[@]}"; do
    write_stress_fault_fixture_artifact "$root" "$fault_case"
  done
  for fault_case in "${PROCESS_CRASH_ARTIFACT_CASES[@]}"; do
    write_process_crash_fixture_artifact "$root" "$fault_case"
  done
  write_process_stress_fixture_artifact "$root"
  write_full_restart_fixture_artifact "$root"
}

write_chaos_fixture_set() {
  local root="$1"
  mkdir -p "$root"
  write_external_nemesis_fixture_artifact "$root"
  write_external_concurrent_fixture_artifact "$root"
  write_external_control_fixture_artifact "$root"
}

expect_artifact_audit_failure() {
  local description="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "expected artifact audit failure: $description" >&2
    return 1
  fi
}

run_correctness_artifact_self_test() {
  local root
  root=$(mktemp -d)
  local previous_root="${CORRECTNESS_ARTIFACT_ROOT:-}"
  CORRECTNESS_ARTIFACT_ROOT="$root"
  trap 'rm -rf "$root"; CORRECTNESS_ARTIFACT_ROOT="$previous_root"' RETURN

  write_full_fault_fixture_set "$root"
  audit_correctness_artifacts >/dev/null
  require_full_fault_gate_artifacts

  sed -i.bak 's/^finalDescriptor.chunkCount=1$/finalDescriptor.chunkCount=2/' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "descriptor chunk count mismatch" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's/state=SEALED/state=OPEN/' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "sealed descriptor chunk state mismatch" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  cp "$root/stress-fault-rf3-aq2-single.txt" "$root/stress-fault-rf3-aq2-single-duplicate.txt"
  expect_artifact_audit_failure "duplicate stress/fault case" require_full_fault_gate_artifacts
  rm -f "$root/stress-fault-rf3-aq2-single-duplicate.txt"

  sed -i.bak 's/^artifact.scenario=.*/artifact.scenario=wrong-scenario/' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "mismatched artifact scenario" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's|^artifact.path=.*|artifact.path=target/correctness-artifacts/wrong-name.txt|' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "mismatched artifact path" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's/ -Dstrata.stress.case=rf3-aq2-single//' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "missing exact stress/fault replay selector" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's/^sealedLength=10$/sealedLength=9/' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "sealed length differs from acked bytes" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's/finalDescriptor.chunk=file.0 state=SEALED length=10/finalDescriptor.chunk=file.0 state=SEALED length=9/' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "descriptor chunk lengths differ from acked bytes" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's/^finalDescriptor.policy=.*/finalDescriptor.policy=rf9-aq9-fsyncfalse/' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "descriptor policy differs from declared write policy" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's|replicas=\[1@127.0.0.1:2, 2@127.0.0.1:3, 3@127.0.0.1:4\]|replicas=[1@127.0.0.1:2, 2@127.0.0.1:3]|' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "descriptor replica count below replication factor" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  sed -i.bak 's|2@127.0.0.1:3|1@127.0.0.1:3|' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "descriptor duplicate replica node id" audit_correctness_artifacts
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"

  rm -f "$root/stress-fault-rf4-aq3-fsync.txt"
  expect_artifact_audit_failure "missing stress/fault case" require_full_fault_gate_artifacts
  write_stress_fault_fixture_artifact "$root" "rf4-aq3-fsync"

  rm -f "$root/process-crash-recovery-rf4-aq3.txt"
  expect_artifact_audit_failure "missing process crash case" require_full_fault_gate_artifacts

  rm -rf "$root"
  mkdir -p "$root"
  write_chaos_fixture_set "$root"
  audit_correctness_artifacts >/dev/null
  require_chaos_gate_artifacts

  sed -i.bak 's/ -Dtest=ExternalNemesisTest#storageControlPartitionDelaysRepairUntilHeartbeatPathHeals//' \
    "$root/external-control-nemesis-ext-c.txt"
  rm -f "$root/external-control-nemesis-ext-c.txt.bak"
  expect_artifact_audit_failure "missing external nemesis replay test selector" audit_correctness_artifacts
  write_external_control_fixture_artifact "$root"

  rm -f "$root/external-concurrent-nemesis-ext-b.txt"
  expect_artifact_audit_failure "missing external concurrent chaos artifact" require_chaos_gate_artifacts

  rm -rf "$root"
  mkdir -p "$root"
  write_stress_fault_fixture_artifact "$root" "rf3-aq2-single"
  sed -i.bak 's/^finalAckedSha256=.*/finalAckedSha256=not-a-sha/' \
    "$root/stress-fault-rf3-aq2-single.txt"
  rm -f "$root/stress-fault-rf3-aq2-single.txt.bak"
  expect_artifact_audit_failure "invalid acknowledged hash" audit_correctness_artifacts

  rm -rf "$root"
  mkdir -p "$root"
  write_soak_fixture_artifact "$root"
  audit_correctness_artifacts >/dev/null
  require_soak_gate_artifacts

  sed -i.bak '/^iterationPassed=2 /d' "$root/stress-fault-soak-base-abc-i3-b15.txt"
  rm -f "$root/stress-fault-soak-base-abc-i3-b15.txt.bak"
  expect_artifact_audit_failure "missing soak iteration pass evidence" audit_correctness_artifacts
  write_soak_fixture_artifact "$root"

  sed -i.bak 's/^iterationPassed=2 seed=.*/iterationPassed=2 failed/' \
    "$root/stress-fault-soak-base-abc-i3-b15.txt"
  rm -f "$root/stress-fault-soak-base-abc-i3-b15.txt.bak"
  expect_artifact_audit_failure "malformed soak iteration pass evidence" audit_correctness_artifacts
  write_soak_fixture_artifact "$root"

  sed -i.bak 's/-Dstrata.stress.seed=acc/-Dstrata.stress.seed=wrong/' \
    "$root/stress-fault-soak-base-abc-i3-b15.txt"
  rm -f "$root/stress-fault-soak-base-abc-i3-b15.txt.bak"
  expect_artifact_audit_failure "soak iteration replay seed mismatch" audit_correctness_artifacts
  write_soak_fixture_artifact "$root"

  sed -i.bak 's/-Dstrata.stress.batches=15/-Dstrata.stress.batches=14/' \
    "$root/stress-fault-soak-base-abc-i3-b15.txt"
  rm -f "$root/stress-fault-soak-base-abc-i3-b15.txt.bak"
  expect_artifact_audit_failure "soak iteration replay batch mismatch" audit_correctness_artifacts
  write_soak_fixture_artifact "$root"

  sed -i.bak '/^iterationReplayCommand=3 /d' "$root/stress-fault-soak-base-abc-i3-b15.txt"
  rm -f "$root/stress-fault-soak-base-abc-i3-b15.txt.bak"
  expect_artifact_audit_failure "missing soak iteration replay evidence" audit_correctness_artifacts

  echo "==> correctness artifact audit self-test passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  case "${1:-}" in
    --self-test)
      run_correctness_artifact_self_test
      ;;
    -h|--help)
      echo "usage: $0 --self-test"
      ;;
    *)
      echo "usage: $0 --self-test" >&2
      exit 2
      ;;
  esac
fi
