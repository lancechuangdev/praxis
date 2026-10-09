#!/usr/bin/env bash
set -euo pipefail

namespace=${PRAXIS_NAMESPACE:-praxis}
observability_namespace=${OBSERVABILITY_NAMESPACE:-observability}

if [[ "${ACK_PHASE1_FAILURE_DRILL:-}" != "delete-one-pod" ]]; then
  printf 'Set ACK_PHASE1_FAILURE_DRILL=delete-one-pod to run this disruptive non-production drill.\n' >&2
  exit 1
fi

for command in kubectl date; do
  command -v "${command}" >/dev/null || {
    printf 'Required command is missing: %s\n' "${command}" >&2
    exit 1
  }
done

context=$(kubectl config current-context)
if [[ "${context}" == *prod* || "${context}" == *production* ]]; then
  printf 'Refusing to run against a context whose name appears to be production: %s\n' "${context}" >&2
  exit 1
fi

drill_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf 'Drill started at %s against context %s\n' "${drill_start}" "${context}"

for workload in order ledger matching; do
  pod=$(kubectl get pods -n "${namespace}" -l "app=${workload}" \
    --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  test -n "${pod}"
  printf 'Deleting %s pod %s\n' "${workload}" "${pod}"
  kubectl delete pod "${pod}" -n "${namespace}" --wait=false
  kubectl rollout status "deployment/${workload}" -n "${namespace}" --timeout=5m
done

collector_pod=$(kubectl get pods -n "${observability_namespace}" -l app=otel-collector \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
test -n "${collector_pod}"
printf 'Deleting collector pod %s\n' "${collector_pod}"
kubectl delete pod "${collector_pod}" -n "${observability_namespace}" --wait=false
kubectl rollout status daemonset/otel-collector -n "${observability_namespace}" --timeout=5m

kubectl wait --for=condition=Ready pod -l app=order -n "${namespace}" --timeout=5m
kubectl wait --for=condition=Ready pod -l app=ledger -n "${namespace}" --timeout=5m
kubectl wait --for=condition=Ready pod -l app=matching -n "${namespace}" --timeout=5m

printf 'Pod failure drill completed at %s. Verify traffic, metrics, traces, logs, and alerts before accepting the result.\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

