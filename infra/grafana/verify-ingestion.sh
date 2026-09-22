#!/usr/bin/env bash
set -euo pipefail

# Read-only AMP checks. AWS credentials must allow aps:QueryMetrics.
: "${AMP_ENDPOINT:?Set AMP_ENDPOINT to managed_metrics_query_endpoint}"
: "${AWS_REGION:?Set AWS_REGION to the AMP workspace Region}"
command -v awscurl >/dev/null || { echo 'awscurl is required' >&2; exit 2; }
command -v jq >/dev/null || { echo 'jq is required' >&2; exit 2; }

query() {
  local encoded response
  encoded=$(jq -rn --arg q "$1" '$q|@uri')
  response=$(awscurl --region "$AWS_REGION" --service aps \
    -X POST "${AMP_ENDPOINT%/}/api/v1/query" \
    --header 'Content-Type: application/x-www-form-urlencoded' \
    --data "query=$encoded")
  jq -e '.status == "success"' <<<"$response" >/dev/null || {
    echo "AMP query failed: $response" >&2
    return 1
  }
  jq -c '.data.result' <<<"$response"
}

for service in order ledger matching; do
  samples=$(query "up{job=\"praxis-eks-hot-path\",service=\"$service\"}")
  jq -e --argjson cutoff "$(($(date +%s) - 180))" \
    'length > 0 and all(.[]; .value[1] == "1" and .value[0] >= $cutoff)' \
    <<<"$samples" >/dev/null || {
    echo "FAIL: $service has no healthy EKS scrape target" >&2
    exit 1
  }
  echo "PASS: $service scrape target is up"
done

for metric in order_requests_total ledger_reserve_requests_total matching_orders_submitted_total; do
  samples=$(query "$metric")
  jq -e --argjson cutoff "$(($(date +%s) - 180))" \
    'length > 0 and any(.[]; .value[0] >= $cutoff)' <<<"$samples" >/dev/null || {
    echo "FAIL: $metric is absent from AMP" >&2
    exit 1
  }
  echo "PASS: $metric is present"
done

echo 'AMP ingestion checks passed. Generate real traffic and inspect dashboard rates separately.'
