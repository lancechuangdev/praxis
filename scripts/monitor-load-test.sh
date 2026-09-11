#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_dir=$(cd -- "${script_dir}/.." && pwd)
compose_file=${COMPOSE_FILE:-${project_dir}/ledgerservice/compose.yaml}
interval=${INTERVAL_SECONDS:-2}
sample_limit=${SAMPLES:-0}
run_id=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
output_dir=${OUTPUT_DIR:-${project_dir}/orderservice/loadtest/results/monitor-${run_id}}

mkdir -p "${output_dir}"

activity_file=${output_dir}/pg_stat_activity.tsv
wal_file=${output_dir}/pg_stat_wal.tsv
database_file=${output_dir}/pg_stat_database.tsv
relations_file=${output_dir}/relation_sizes.tsv

printf 'sample_time\twait_type\twait_event\tstate\tconnections\n' > "${activity_file}"
printf 'sample_time\twal_records\twal_fpi\twal_bytes\twal_buffers_full\tstats_reset\n' > "${wal_file}"
printf 'sample_time\txact_commit\txact_rollback\ttup_inserted\ttup_updated\tblk_read_time_ms\tblk_write_time_ms\ttemp_files\ttemp_bytes\tdeadlocks\n' > "${database_file}"
printf 'sample_time\trelation\ttotal_bytes\ttable_bytes\tindexes_bytes\n' > "${relations_file}"

append_metrics() {
  local name=$1
  local url=$2
  local target=${output_dir}/${name}.prom
  printf '# sample_time %s\n' "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> "${target}"
  if ! curl --silent --show-error --fail --max-time 2 "${url}" >> "${target}"; then
    printf '# scrape_failed\n' >> "${target}"
  fi
  printf '\n' >> "${target}"
}

psql_query() {
  local sql=$1
  local target=$2
  docker compose -f "${compose_file}" exec -T postgres \
    psql -X -q -v ON_ERROR_STOP=1 -U ledger -d cex_ledger -At -F $'\t' \
    -c "${sql}" >> "${target}"
}

sample_postgres() {
  psql_query "
    SELECT clock_timestamp(),
           COALESCE(wait_event_type, 'CPU'),
           COALESCE(wait_event, 'running'),
           state,
           count(*)
    FROM pg_stat_activity
    WHERE datname = 'cex_ledger'
    GROUP BY wait_event_type, wait_event, state
    ORDER BY count(*) DESC;
  " "${activity_file}"

  psql_query "
    SELECT clock_timestamp(), wal_records, wal_fpi, wal_bytes,
           wal_buffers_full, stats_reset
    FROM pg_stat_wal;
  " "${wal_file}"

  psql_query "
    SELECT clock_timestamp(), xact_commit, xact_rollback,
           tup_inserted, tup_updated, blk_read_time, blk_write_time,
           temp_files, temp_bytes, deadlocks
    FROM pg_stat_database
    WHERE datname = 'cex_ledger';
  " "${database_file}"

  psql_query "
    SELECT clock_timestamp(), relname,
           pg_total_relation_size(relid),
           pg_relation_size(relid),
           pg_indexes_size(relid)
    FROM pg_catalog.pg_statio_user_tables
    ORDER BY pg_total_relation_size(relid) DESC;
  " "${relations_file}"
}

printf 'Writing monitoring samples to %s\n' "${output_dir}"
printf 'Press Ctrl+C to stop.\n'

sample_number=0
while (( sample_limit == 0 || sample_number < sample_limit )); do
  append_metrics ledger http://localhost:8081/metrics
  append_metrics order http://localhost:8083/metrics
  append_metrics matching http://localhost:8084/metrics
  sample_postgres
  sample_number=$((sample_number + 1))
  if (( sample_limit == 0 || sample_number < sample_limit )); then
    sleep "${interval}"
  fi
done

printf 'Captured %d sample(s) in %s\n' "${sample_number}" "${output_dir}"
