#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
namespace=${PRAXIS_NAMESPACE:-praxis}
observability_namespace=${OBSERVABILITY_NAMESPACE:-observability}

for command in terraform kubectl aws jq curl; do
  command -v "${command}" >/dev/null || {
    printf 'Required command is missing: %s\n' "${command}" >&2
    exit 1
  }
done

cluster_name=$(terraform -chdir="${project_dir}/infra/aws" output -raw eks_cluster_name)
order_alb_dns=$(terraform -chdir="${project_dir}/infra/aws" output -raw order_alb_dns_name)
order_url="https://${order_alb_dns}"
aws_region=$(terraform -chdir="${project_dir}/infra/aws" output -json eks_runtime_config | jq -r .aws_region)

printf 'Verifying cluster %s in %s\n' "${cluster_name}" "${aws_region}"
aws eks update-kubeconfig --name "${cluster_name}" --region "${aws_region}" >/dev/null

kubectl auth can-i get pods -n "${namespace}" | grep -qx yes
kubectl get --raw /apis/metrics.k8s.io/v1beta1/nodes >/dev/null

for workload in order ledger matching matching-outbox risk; do
  kubectl rollout status "deployment/${workload}" -n "${namespace}" --timeout=5m
done
kubectl rollout status daemonset/otel-collector -n "${observability_namespace}" --timeout=5m

kubectl wait --for=condition=Available deployment/order deployment/ledger deployment/matching deployment/risk \
  -n "${namespace}" --timeout=5m
kubectl get hpa order ledger matching risk -n "${namespace}"
kubectl get pdb order ledger matching risk -n "${namespace}"

for hpa in order ledger matching risk; do
  target=$(kubectl get hpa "${hpa}" -n "${namespace}" -o jsonpath='{.status.currentMetrics[0].resource.current.averageUtilization}')
  test -n "${target}" || {
    printf 'HPA %s has no CPU utilization; verify metrics-server and pod requests.\n' "${hpa}" >&2
    exit 1
  }
done

response_headers=$(mktemp)
trap 'rm -f "${response_headers}"' EXIT
status=$(curl --silent --show-error --output /dev/null --dump-header "${response_headers}" \
  --write-out '%{http_code}' "${order_url}/healthz")
if [[ "${status}" != 302 ]]; then
  printf 'Expected unauthenticated Order ALB request to redirect with 302, received %s.\n' "${status}" >&2
  exit 1
fi
grep -qi '^location: https://' "${response_headers}" || {
  printf 'Order ALB redirect did not contain an HTTPS identity-provider location.\n' >&2
  exit 1
}

export AWS_REGION="${aws_region}"
export AMP_ENDPOINT
AMP_ENDPOINT=$(terraform -chdir="${project_dir}/infra/aws" output -raw managed_metrics_query_endpoint)
"${project_dir}/infra/grafana/verify-ingestion.sh"

log_group="/aws/containerinsights/${cluster_name}/application"
aws logs describe-log-streams --region "${aws_region}" --log-group-name "${log_group}" \
  --order-by LastEventTime --descending --max-items 1 \
  --query 'logStreams[0].lastEventTimestamp' --output text | grep -vqx None

printf '\nAutomated Phase 1 checks passed. Complete the manual evidence steps:\n'
printf '  1. Submit an authenticated order and retain its status and trace ID.\n'
printf '  2. Confirm that trace in X-Ray and the request in the Grafana dashboard.\n'
printf '  3. Send a Grafana contact-point test and retain delivery evidence.\n'
printf '  4. Run the approved failure drill and record recovery times.\n'
