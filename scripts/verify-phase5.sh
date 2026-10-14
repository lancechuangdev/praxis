#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
namespace=${PRAXIS_NAMESPACE:-praxis}
istio_namespace=${ISTIO_NAMESPACE:-istio-system}

for command in terraform kubectl aws jq istioctl; do
  command -v "${command}" >/dev/null || {
    printf 'Required command is missing: %s\n' "${command}" >&2
    exit 1
  }
done

cluster_name=$(terraform -chdir="${project_dir}/infra/aws" output -raw eks_cluster_name)
aws_region=$(terraform -chdir="${project_dir}/infra/aws" output -json eks_runtime_config | jq -r .aws_region)

printf 'Verifying Phase 5 mesh configuration on cluster %s in %s\n' "${cluster_name}" "${aws_region}"
aws eks update-kubeconfig --name "${cluster_name}" --region "${aws_region}" >/dev/null

for crd in \
  destinationrules.networking.istio.io \
  peerauthentications.security.istio.io \
  authorizationpolicies.security.istio.io \
  telemetries.telemetry.istio.io; do
  kubectl get crd "${crd}" >/dev/null
done

kubectl rollout status deployment/istiod -n "${istio_namespace}" --timeout=5m
for workload in order ledger matching; do
  kubectl rollout status "deployment/${workload}" -n "${namespace}" --timeout=5m

  pods=$(kubectl get pods -n "${namespace}" -l "app=${workload}" -o json)
  jq -e --arg application "${workload}" '
    .items | length > 0 and all(.[].spec.containers;
      (map(.name) | index($application) != null) and
      (map(.name) | index("istio-proxy") != null)
    )
  ' <<<"${pods}" >/dev/null || {
    printf 'FAIL: every %s pod must contain %s and istio-proxy containers.\n' \
      "${workload}" "${workload}" >&2
    exit 1
  }

  jq -e '
    .items | all(.[].status.containerStatuses;
      length >= 2 and all(.[]; .ready == true)
    )
  ' <<<"${pods}" >/dev/null || {
    printf 'FAIL: one or more %s application or proxy containers are not ready.\n' \
      "${workload}" >&2
    exit 1
  }

  admin_port=$(case "${workload}" in
    order) printf 8083 ;;
    ledger) printf 8081 ;;
    matching) printf 8084 ;;
  esac)
  kubectl get peerauthentication "${workload}-strict-mtls" -n "${namespace}" -o json |
    jq -e --arg workload "${workload}" --arg port "${admin_port}" '
      .spec.selector.matchLabels.app == $workload and
      .spec.mtls.mode == "STRICT" and
      .spec.portLevelMtls[$port].mode == "DISABLE"
    ' >/dev/null

  kubectl get telemetry "${workload}-error-access-logs" -n "${namespace}" -o json |
    jq -e --arg workload "${workload}" '
      .spec.selector.matchLabels.app == $workload and
      .spec.accessLogging[0].providers[0].name == "envoy" and
      .spec.accessLogging[0].filter.expression ==
        "!has(response.code) || response.code >= 400"
    ' >/dev/null

  printf 'PASS: %s is enrolled, ready, strict on application traffic, and error-logged.\n' \
    "${workload}"
done

for backend in ledger matching; do
  case "${backend}" in
    ledger) grpc_port=9091 ;;
    matching) grpc_port=9092 ;;
  esac

  available=$(kubectl get deployment "${backend}" -n "${namespace}" \
    -o jsonpath='{.status.availableReplicas}')
  ready_endpoints=$(kubectl get endpointslice -n "${namespace}" \
    -l "kubernetes.io/service-name=${backend}" -o json |
    jq '[.items[].endpoints[] | select(.conditions.ready == true)] | length')
  if [[ -z "${available}" || "${ready_endpoints}" -ne "${available}" ]]; then
    printf 'FAIL: %s has %s available replicas but %s ready service endpoints.\n' \
      "${backend}" "${available:-0}" "${ready_endpoints}" >&2
    exit 1
  fi

  kubectl get authorizationpolicy "${backend}-from-order" -n "${namespace}" -o json |
    jq -e --arg backend "${backend}" --arg port "${grpc_port}" '
      .spec.selector.matchLabels.app == $backend and
      .spec.action == "ALLOW" and
      any(.spec.rules[];
        (.from[0].source.principals[0] | endswith("/ns/praxis/sa/order")) and
        .to[0].operation.ports == [$port]
      )
    ' >/dev/null

  kubectl get destinationrule "${backend}" -n "${namespace}" -o json |
    jq -e --arg host "${backend}.${namespace}.svc.cluster.local" '
      .spec.host == $host and
      .spec.trafficPolicy.loadBalancer.simple == "ROUND_ROBIN" and
      .spec.trafficPolicy.connectionPool.tcp.maxConnections == 64 and
      .spec.trafficPolicy.connectionPool.http.http1MaxPendingRequests == 64 and
      .spec.trafficPolicy.connectionPool.http.http2MaxRequests == 256 and
      .spec.trafficPolicy.connectionPool.http.maxRetries == 0
    ' >/dev/null

  printf 'PASS: %s has %s ready endpoints and the expected identity and traffic policy.\n' \
    "${backend}" "${ready_endpoints}"
done

proxy_status=$(istioctl proxy-status)
printf '%s\n' "${proxy_status}"
if grep -Eq 'STALE|NOT SENT' <<<"${proxy_status}"; then
  printf 'FAIL: one or more Istio proxies have stale or unsent configuration.\n' >&2
  exit 1
fi
for workload in order ledger matching; do
  mapfile -t pod_names < <(kubectl get pods -n "${namespace}" -l "app=${workload}" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  for pod_name in "${pod_names[@]}"; do
    grep -Fq "${pod_name}.${namespace}" <<<"${proxy_status}" || {
      printf 'FAIL: %s is absent from istioctl proxy-status.\n' "${pod_name}" >&2
      exit 1
    }
  done
done
istioctl analyze --namespace "${namespace}"

printf '\nAutomated Phase 5 configuration checks passed. Complete and retain the live evidence steps:\n'
printf '  1. Generate representative authenticated order traffic.\n'
printf '  2. Prove mutual_tls and even per-endpoint request distribution in AMP.\n'
printf '  3. Prove an unauthorized meshed identity is denied on both backend gRPC ports.\n'
printf '  4. Find the denial and an injected transport error in CloudWatch access logs.\n'
printf '  5. Exercise scale-out, rolling drain, zone loss, overload, and a shared-dependency failure.\n'
printf '  6. Set enable_istio_mesh=false, rehearse rollback, and verify application behavior.\n'
