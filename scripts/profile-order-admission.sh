#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_dir=$(cd -- "${script_dir}/.." && pwd)
compose_file=${COMPOSE_FILE:-${project_dir}/ledgerservice/compose.yaml}
results_root=${RESULTS_ROOT:-${project_dir}/orderservice/loadtest/results}
rate=${RATE:-10000}
pool_size=${LEDGER_DB_MAX_CONNS:-48}
runs=${RUNS:-3}
warmup_duration=${WARMUP_DURATION:-2m}
duration=${DURATION:-10m}
cooldown_seconds=${COOLDOWN_SECONDS:-60}
user_count=${USER_COUNT:-10000}
preallocated_vus=${PREALLOCATED_VUS:-2000}
max_vus=${MAX_VUS:-10000}
engine_partitions=${ENGINE_PARTITIONS:-96}
available_atomic=${AVAILABLE_ATOMIC:-1000000000}
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
batch_id=${BATCH_ID:-phase0-pool-${pool_size}-rate-${rate}-${timestamp}}
batch_dir=${results_root}/${batch_id}

if [[ ${pool_size} != 48 ]]; then
  printf 'Phase 0 profile requires LEDGER_DB_MAX_CONNS=48 (received %s).\n' "${pool_size}" >&2
  exit 2
fi
if [[ ${rate} != 10000 ]]; then
  printf 'Phase 0 profile requires RATE=10000 (received %s).\n' "${rate}" >&2
  exit 2
fi
for value in "${rate}" "${runs}" "${user_count}" "${preallocated_vus}" "${max_vus}"; do
  if [[ ! ${value} =~ ^[1-9][0-9]*$ ]]; then
    printf 'Rate, runs, users, and VU limits must be positive integers.\n' >&2
    exit 2
  fi
done
if (( max_vus < preallocated_vus )); then
  printf 'MAX_VUS must be greater than or equal to PREALLOCATED_VUS.\n' >&2
  exit 2
fi

mkdir -p "${batch_dir}"

compose() {
  docker compose -f "${compose_file}" "$@"
}

psql_command() {
  compose exec -T postgres psql -X -q -v ON_ERROR_STOP=1 -U ledger -d cex_ledger "$@"
}

wait_for_service() {
  local url=$1
  local name=$2
  local attempt
  for attempt in $(seq 1 60); do
    if curl --silent --fail --max-time 2 "${url}" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  printf '%s did not become ready at %s.\n' "${name}" "${url}" >&2
  return 1
}

capture_environment() {
  local run_dir=$1
  {
    printf 'captured_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'batch_id=%s\nrate=%s\npool_size=%s\nruns=%s\n' "${batch_id}" "${rate}" "${pool_size}" "${runs}"
    printf 'warmup_duration=%s\nduration=%s\ncooldown_seconds=%s\n' "${warmup_duration}" "${duration}" "${cooldown_seconds}"
    printf 'user_count=%s\npreallocated_vus=%s\nmax_vus=%s\nengine_partitions=%s\n' "${user_count}" "${preallocated_vus}" "${max_vus}" "${engine_partitions}"
    printf 'git_commit=%s\n' "$(git -C "${project_dir}" rev-parse HEAD 2>/dev/null || printf unknown)"
    printf 'git_dirty_files=%s\n' "$(git -C "${project_dir}" status --short 2>/dev/null | wc -l)"
    printf 'kernel=%s\n' "$(uname -srmo)"
    printf 'logical_cpus=%s\n' "$(getconf _NPROCESSORS_ONLN)"
    awk '/^MemTotal:/ {printf "host_memory_kib=%s\n", $2}' /proc/meminfo
    docker version --format 'docker_server={{.Server.Version}}'
    docker compose version --short | awk '{print "docker_compose=" $0}'
  } > "${run_dir}/environment.txt"

  compose images > "${run_dir}/compose-images.txt"
  compose config > "${run_dir}/compose-config.yaml"
  psql_command -At -F $'\t' -c "
    SELECT name, setting, unit, source
    FROM pg_settings
    WHERE name IN (
      'block_size', 'checkpoint_completion_target', 'checkpoint_timeout',
      'effective_cache_size', 'fsync', 'full_page_writes', 'max_connections',
      'max_wal_size', 'shared_buffers', 'shared_preload_libraries',
      'synchronous_commit', 'track_io_timing', 'wal_buffers', 'wal_level',
      'work_mem'
    )
    ORDER BY name;
  " > "${run_dir}/postgres-settings.tsv"
  psql_command -At -c 'SELECT version();' > "${run_dir}/postgres-version.txt"
}

run_k6() {
  local run_id=$1
  local test_duration=$2
  local strict=$3
  local output_file=$4
  local summary_file=$5
  docker run --rm --network=host \
    -v "${project_dir}/orderservice/loadtest:/scripts:ro" \
    -v "${output_file%/*}:/output" \
    -e K6_SUMMARY_TREND_STATS='avg,min,med,max,p(90),p(95),p(99)' \
    grafana/k6 run \
    --summary-export "/output/${summary_file##*/}" \
    -e RATE="${rate}" \
    -e DURATION="${test_duration}" \
    -e BASE_URL=http://localhost:8083 \
    -e USER_COUNT="${user_count}" \
    -e ENGINE_PARTITIONS="${engine_partitions}" \
    -e PREALLOCATED_VUS="${preallocated_vus}" \
    -e MAX_VUS="${max_vus}" \
    -e STRICT_PROFILE="${strict}" \
    -e PROFILE_WARMUP="$([[ ${strict} == false ]] && printf true || printf false)" \
    -e RUN_ID="${run_id}" \
    /scripts/order-admission-distributed-users.js \
    > "${output_file}" 2>&1
}

monitor_pid=''
cleanup() {
  if [[ -n ${monitor_pid} ]] && kill -0 "${monitor_pid}" 2>/dev/null; then
    kill "${monitor_pid}" 2>/dev/null || true
    wait "${monitor_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

printf 'Writing Phase 0 profile to %s\n' "${batch_dir}"
compose up -d postgres kafka kafka-init > "${batch_dir}/infrastructure-start.log" 2>&1
overall_status=0

for run_number in $(seq 1 "${runs}"); do
  run_id=${batch_id}-run-${run_number}
  run_dir=${batch_dir}/run-${run_number}
  mkdir -p "${run_dir}"

  printf 'Run %d/%d: reset and seed\n' "${run_number}" "${runs}"
  make -C "${project_dir}" reset-load-data LEDGER_DB_MAX_CONNS="${pool_size}" > "${run_dir}/reset.log" 2>&1
  make -C "${project_dir}" seed-distributed-users USER_COUNT="${user_count}" AVAILABLE_ATOMIC="${available_atomic}" > "${run_dir}/seed.log" 2>&1
  wait_for_service http://localhost:8081/readyz 'Ledger Service'
  wait_for_service http://localhost:8083/readyz 'Order Service'

  psql_command -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements;'
  capture_environment "${run_dir}"

  printf 'Run %d/%d: warm up for %s\n' "${run_number}" "${runs}" "${warmup_duration}"
  run_k6 "${run_id}-warmup" "${warmup_duration}" false \
    "${run_dir}/warmup-output.txt" "${run_dir}/warmup-summary.json"

  psql_command -c 'SELECT pg_stat_statements_reset(); SELECT pg_stat_reset(); SELECT pg_stat_reset_shared('"'"'wal'"'"');' \
    > "${run_dir}/statistics-reset.txt"

  printf 'Run %d/%d: measure for %s\n' "${run_number}" "${runs}" "${duration}"
  RUN_ID="${run_id}" OUTPUT_DIR="${run_dir}/monitor" INTERVAL_SECONDS=2 \
    "${script_dir}/monitor-load-test.sh" > "${run_dir}/monitor.log" 2>&1 &
  monitor_pid=$!
  sleep 2

  k6_status=0
  run_k6 "${run_id}" "${duration}" true \
    "${run_dir}/k6-output.txt" "${run_dir}/k6-summary.json" || k6_status=$?

  cleanup
  monitor_pid=''

  psql_command -At -F $'\t' -c "
    SELECT queryid, calls, round(total_exec_time::numeric, 3),
           round(mean_exec_time::numeric, 3), rows, shared_blks_hit,
           shared_blks_read, temp_blks_written, wal_records, wal_bytes,
           regexp_replace(query, E'[\\n\\r\\t ]+', ' ', 'g')
    FROM pg_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'cex_ledger')
      AND userid = (SELECT usesysid FROM pg_user WHERE usename = 'ledger')
    ORDER BY total_exec_time DESC;
  " > "${run_dir}/pg-stat-statements-final.tsv"

  psql_command -At -F $'\t' -v run_id="${run_id}" \
    -f /dev/stdin < "${project_dir}/orderservice/loadtest/profile-integrity.sql" \
    > "${run_dir}/integrity.tsv"
  integrity_status=0
  if awk -F '\t' '$2 != 0 {print; failed=1} END {exit failed}' "${run_dir}/integrity.tsv" \
      > "${run_dir}/integrity-failures.tsv"; then
    printf 'PASS\n' > "${run_dir}/integrity-status.txt"
  else
    integrity_status=1
    printf 'FAIL\n' > "${run_dir}/integrity-status.txt"
  fi

  if (( k6_status != 0 || integrity_status != 0 )); then
    printf 'Run %d failed (k6=%d, integrity=%d). Results: %s\n' \
      "${run_number}" "${k6_status}" "${integrity_status}" "${run_dir}" >&2
    overall_status=1
  fi

  if (( run_number < runs && cooldown_seconds > 0 )); then
    printf 'Run %d/%d: cool down for %ss\n' "${run_number}" "${runs}" "${cooldown_seconds}"
    sleep "${cooldown_seconds}"
  fi
done

"${script_dir}/summarize-order-profile.sh" "${batch_dir}"
printf 'Phase 0 profile complete: %s\n' "${batch_dir}"
exit "${overall_status}"
