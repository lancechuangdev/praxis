#!/usr/bin/env bash
set -euo pipefail

batch_dir=${1:?usage: summarize-order-profile.sh RESULTS_DIRECTORY}
summary_tsv=${batch_dir}/summary.tsv
report=${batch_dir}/baseline-report.md

if ! command -v jq >/dev/null 2>&1; then
  printf 'jq is required to summarize k6 JSON output.\n' >&2
  exit 2
fi

printf 'run\tcompleted\tcompleted_rate\tdropped\tfailure_rate\thttp_avg_ms\thttp_p95_ms\thttp_p99_ms\treserve_avg_ms\treserve_p95_ms\treserve_p99_ms\n' > "${summary_tsv}"

found=0
for summary in "${batch_dir}"/run-*/k6-summary.json; do
  [[ -f ${summary} ]] || continue
  found=1
  run=$(basename "$(dirname "${summary}")")
  jq -r --arg run "${run}" '[
    $run,
    .metrics.http_reqs.count,
    .metrics.http_reqs.rate,
    (.metrics.dropped_iterations.count // 0),
    (.metrics.order_failure_rate.value // 0),
    .metrics.http_req_duration.avg,
    .metrics.http_req_duration["p(95)"],
    .metrics.http_req_duration["p(99)"],
    .metrics.order_reserve_ms.avg,
    .metrics.order_reserve_ms["p(95)"],
    .metrics.order_reserve_ms["p(99)"]
  ] | @tsv' "${summary}" >> "${summary_tsv}"
done

if (( found == 0 )); then
  printf 'No run-*/k6-summary.json files found in %s.\n' "${batch_dir}" >&2
  exit 1
fi

column_stats() {
  local column=$1
  local values count min max median
  values=$(tail -n +2 "${summary_tsv}" | cut -f "${column}" | sort -g)
  count=$(wc -l <<< "${values}")
  min=$(head -n 1 <<< "${values}")
  max=$(tail -n 1 <<< "${values}")
  median=$(awk -v count="${count}" 'NR == int((count + 1) / 2) {a=$1} NR == int((count + 2) / 2) {b=$1} END {if (count % 2) print a; else printf "%.6f", (a+b)/2}' <<< "${values}")
  printf '%s / %s-%s' "${median}" "${min}" "${max}"
}

{
  printf '# Phase 0 baseline report\n\n'
  printf 'Generated: `%s`  \n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'Result directory: `%s`\n\n' "${batch_dir}"
  printf 'Values below are `median / minimum-maximum` across completed runs.\n\n'
  printf '| Metric | Median / range |\n|---|---:|\n'
  printf '| Completed requests | %s |\n' "$(column_stats 2)"
  printf '| Completed requests/s | %s |\n' "$(column_stats 3)"
  printf '| Dropped iterations | %s |\n' "$(column_stats 4)"
  printf '| Failure rate | %s |\n' "$(column_stats 5)"
  printf '| HTTP average (ms) | %s |\n' "$(column_stats 6)"
  printf '| HTTP p95 (ms) | %s |\n' "$(column_stats 7)"
  printf '| HTTP p99 (ms) | %s |\n' "$(column_stats 8)"
  printf '| Reservation average (ms) | %s |\n' "$(column_stats 9)"
  printf '| Reservation p95 (ms) | %s |\n' "$(column_stats 10)"
  printf '| Reservation p99 (ms) | %s |\n\n' "$(column_stats 11)"
  printf 'Per-run metrics are in `summary.tsv`. Environment, pool, PostgreSQL, '
  printf 'statement, WAL, container, and host samples are retained beneath each '
  printf '`run-N` directory. Each run also contains `integrity-status.txt`.\n'
} > "${report}"

printf 'Wrote %s and %s\n' "${summary_tsv}" "${report}"
