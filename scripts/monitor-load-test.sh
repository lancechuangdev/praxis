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
statements_file=${output_dir}/pg_stat_statements.tsv
container_file=${output_dir}/container_stats.tsv
host_cpu_file=${output_dir}/host_cpu.tsv
host_memory_file=${output_dir}/host_memory.tsv
host_load_file=${output_dir}/host_load.tsv

printf 'sample_time\twait_type\twait_event\tstate\tconnections\n' > "${activity_file}"
printf 'sample_time\twal_records\twal_fpi\twal_bytes\twal_buffers_full\tstats_reset\n' > "${wal_file}"
printf 'sample_time\txact_commit\txact_rollback\ttup_inserted\ttup_updated\tblk_read_time_ms\tblk_write_time_ms\ttemp_files\ttemp_bytes\tdeadlocks\n' > "${database_file}"
printf 'sample_time\trelation\ttotal_bytes\ttable_bytes\tindexes_bytes\n' > "${relations_file}"
printf 'sample_time\tqueryid\tcalls\ttotal_exec_time_ms\tmean_exec_time_ms\trows\tshared_blks_hit\tshared_blks_read\ttemp_blks_written\twal_records\twal_bytes\tquery\n' > "${statements_file}"
printf 'sample_time\tcontainer\tcpu_percent\tmemory_usage\tmemory_limit\tmemory_percent\tnetwork_io\tblock_io\tpids\n' > "${container_file}"
printf 'sample_time\tuser\tnice\tsystem\tidle\tiowait\tirq\tsoftirq\tsteal\ttotal\n' > "${host_cpu_file}"
printf 'sample_time\tmem_total_kib\tmem_available_kib\tmem_used_kib\tswap_total_kib\tswap_free_kib\n' > "${host_memory_file}"
printf 'sample_time\tload_1m\tload_5m\tload_15m\trunnable_tasks\ttotal_tasks\n' > "${host_load_file}"

statements_enabled=0
if docker compose -f "${compose_file}" exec -T postgres \
    psql -X -q -U ledger -d cex_ledger -At -c \
    "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')
       AND current_setting('shared_preload_libraries') LIKE '%pg_stat_statements%';" \
    | grep -qx t; then
  statements_enabled=1
else
  printf '# pg_stat_statements is not installed and preloaded; statement samples disabled\n' \
    >> "${statements_file}"
fi

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

  if (( statements_enabled == 1 )); then
    psql_query "
      SELECT clock_timestamp(), queryid, calls,
             round(total_exec_time::numeric, 3),
             round(mean_exec_time::numeric, 3), rows,
             shared_blks_hit, shared_blks_read, temp_blks_written,
             wal_records, wal_bytes,
             regexp_replace(query, E'[\\n\\r\\t ]+', ' ', 'g')
      FROM pg_stat_statements
      WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'cex_ledger')
        AND userid = (SELECT usesysid FROM pg_user WHERE usename = 'ledger')
      ORDER BY total_exec_time DESC
      LIMIT 30;
    " "${statements_file}"
  fi
}

sample_resources() {
  local sample_time
  local cpu user nice system idle iowait irq softirq steal guest guest_nice total
  local mem_total mem_available swap_total swap_free mem_used
  local load_1m load_5m load_15m tasks _
  local container cpu_percent memory_usage memory_used memory_limit
  local memory_percent network_io block_io pids

  sample_time=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

  while IFS=$'\t' read -r container cpu_percent memory_usage memory_percent network_io block_io pids; do
    memory_used=${memory_usage%% / *}
    memory_limit=${memory_usage#* / }
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${sample_time}" "${container}" "${cpu_percent%%%}" "${memory_used}" \
      "${memory_limit}" "${memory_percent%%%}" "${network_io}" "${block_io}" \
      "${pids}" >> "${container_file}"
  done < <(docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}\t{{.PIDs}}')

  read -r cpu user nice system idle iowait irq softirq steal guest guest_nice < /proc/stat
  total=$((user + nice + system + idle + iowait + irq + softirq + steal))
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${sample_time}" "${user}" "${nice}" "${system}" "${idle}" "${iowait}" \
    "${irq}" "${softirq}" "${steal}" "${total}" >> "${host_cpu_file}"

  mem_total=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
  mem_available=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)
  swap_total=$(awk '/^SwapTotal:/ {print $2}' /proc/meminfo)
  swap_free=$(awk '/^SwapFree:/ {print $2}' /proc/meminfo)
  mem_used=$((mem_total - mem_available))
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${sample_time}" "${mem_total}" "${mem_available}" "${mem_used}" \
    "${swap_total}" "${swap_free}" >> "${host_memory_file}"

  read -r load_1m load_5m load_15m tasks _ < /proc/loadavg
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${sample_time}" "${load_1m}" "${load_5m}" "${load_15m}" \
    "${tasks%/*}" "${tasks#*/}" >> "${host_load_file}"
}

printf 'Writing monitoring samples to %s\n' "${output_dir}"
printf 'Press Ctrl+C to stop.\n'

sample_number=0
while (( sample_limit == 0 || sample_number < sample_limit )); do
  append_metrics ledger http://localhost:8081/metrics
  append_metrics order http://localhost:8083/metrics
  append_metrics matching http://localhost:8084/metrics
  sample_postgres
  sample_resources
  sample_number=$((sample_number + 1))
  if (( sample_limit == 0 || sample_number < sample_limit )); then
    sleep "${interval}"
  fi
done

printf 'Captured %d sample(s) in %s\n' "${sample_number}" "${output_dir}"
