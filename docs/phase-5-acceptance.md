# Phase 5 acceptance procedure

## Status and scope

Repository tooling prepares installation artifacts, supports incremental
workload enrollment, renders health-only authorization probes, and captures
read-only AMP evidence. No live exercise is recorded as passed. Run the
following procedures in a selected non-production EKS environment with a
representative authenticated load driver and synthetic accounts. Keep
financial-integrity checks from the Phase 0 baseline enabled throughout.

Record the Git revision, operator, cluster/context, UTC start/end times, load
profile, approved throughput/latency/error budgets, and rollback revision for
each exercise. Store evidence outside Git; it may contain internal network and
workload identities. Establish sustainable capacity before injecting failures.
Stop an exercise when its approved budget is exceeded and restore the last
accepted configuration. Successful infrastructure rollout alone never passes a
traffic or financial-integrity gate.

## Prepare the pinned platform installation

`infra/istio/version` pins 1.31.1. Review support and Kubernetes compatibility
before each installation; the pin is not an automatic upgrade policy. The
platform owner installs the charts separately from application Terraform.
The values configure at least two Istiod replicas, zone spreading, proxy
resource budgets, startup ordering, a 15-second proxy drain, and access logs
without request headers, paths, or payloads. These initial resource limits
require load validation. The trust domain must match `istio_trust_domain`.

Praxis enforces restricted Pod Security. Istio CNI is therefore required to
avoid a privileged `istio-init` container in application pods. Platform review
must confirm CNI binary/configuration paths on the selected EKS AMI,
compatibility with AWS VPC CNI, daemon coverage of every workload node, and the
privileged CNI namespace's security policy. Do not weaken Praxis Pod Security.

Prepare archives, rendered resources and SHA-256 checksums without changing a
cluster:

```bash
ISTIO_ARTIFACT_DIR=/tmp/praxis-istio-review bash scripts/prepare-istio.sh
```

Review all three rendered manifests and image references, then retain the
artifact directory and checksum file in deployment evidence. Checksums record
the downloaded bytes; they do not replace chart/image provenance verification.
Apply the reviewed local archives in dependency order against the explicitly
selected context, using the exact copied values:

```bash
helm upgrade --install istio-base /tmp/praxis-istio-review/base-1.31.1.tgz \
  --kube-context "$PRAXIS_CONTEXT" -n istio-system --create-namespace \
  --set defaultRevision=default --wait --timeout 10m
helm upgrade --install istio-cni /tmp/praxis-istio-review/cni-1.31.1.tgz \
  --kube-context "$PRAXIS_CONTEXT" -n istio-system \
  -f /tmp/praxis-istio-review/cni-values.yaml --wait --timeout 10m
helm upgrade --install istiod /tmp/praxis-istio-review/istiod-1.31.1.tgz \
  --kube-context "$PRAXIS_CONTEXT" -n istio-system \
  -f /tmp/praxis-istio-review/istiod-values.yaml --wait --timeout 10m
```

Verify webhooks, CRDs, Istiod, CNI daemon readiness, node networking and
`istioctl analyze` before enrollment. Use the matching `istioctl` release.

## Incremental enrollment and rollback

Persist `enable_istio_mesh=true` and `mesh_enrolled_workloads` in the selected
Kubernetes stack's variable file. Plan and apply each step independently:

1. `["order"]` — confirm Order readiness, application telemetry and unchanged
   backend behavior before enabling strict backend mTLS.
2. `["order", "ledger"]` — verify Ledger identity denial and mTLS evidence.
3. `["order", "ledger", "matching"]` — repeat for Matching and gather full
   traffic evidence.

Run `make phase5-verify` after each step. It checks only enrolled workloads and
prints explicit skips for unenrolled backends. Partial enrollment cannot
complete acceptance. Terraform rejects backend enrollment without Order.
Plan rollout changes through Terraform rather than patching its resources.

Rehearse reversing these steps: remove Matching, wait for replacement pods and
validate traffic; remove Ledger and repeat; then disable the mesh and verify
Order has no sidecar. Keep the previous pool-limit values for independent
traffic-policy rollback. Istiod and CNI remain installed until every mesh
consumer in the cluster is removed. If removing the platform, uninstall Istiod,
then CNI, then the base chart; CRD deletion remains a separate cluster-wide
cleanup. Record application traffic and financial integrity before and after
the authority-neutral transport rollback.

## Identity allow and deny exercise

Build `api/cmd/mesh-probe/Dockerfile` from the repository root, publish it to the
approved registry, and obtain its immutable digest. Render probe manifests:

```bash
python3 scripts/render-phase5-identity-probes.py \
  --image "$PRAXIS_PROBE_IMAGE_DIGEST" --run-id phase5-check-001 \
  > /tmp/phase5-identity-probes.json
```

Review and apply that manifest with `kubectl --context "$PRAXIS_CONTEXT"`.
The four Jobs call only `grpc.health.v1.Health/Check`: each backend must accept
the Order service account and explicitly deny a dedicated probe service
account. A timeout, unreachable server, or application denial is a failure,
not proof of mesh authorization. The probe waits for proxy startup and shuts
down its own classic sidecar so the Job can finish. Jobs use no automatic
retries and expire after 90 seconds; retain Job status and probe/proxy logs.
Verify all four Jobs succeed, inspect RBAC denials in CloudWatch, and delete the
reviewed manifest after collecting evidence. Do not delete the existing Order
service account; the manifest creates only its dedicated probe account.
Health authorization verifies the port-level policy; financial RPC correctness
continues to rely on application contract and integrity tests.

## AMP traffic and security evidence

After full enrollment, generate at least ten minutes of representative traffic
with stable ready endpoints. Supply AWS query credentials through the normal
credential chain, not command-line arguments:

```bash
export AMP_ENDPOINT="$(terraform -chdir=infra/aws output -raw managed_metrics_query_endpoint)"
export AWS_REGION="$(terraform -chdir=infra/aws output -json eks_runtime_config | jq -r .aws_region)"
python3 scripts/phase5-evidence.py --context "$PRAXIS_CONTEXT" \
  --environment "$PRAXIS_ENVIRONMENT" --output /tmp/phase5-baseline.json
```

The script uses `kubectl` and `awscurl`. It saves exact queries, query time,
raw AMP results, before/after ready endpoint identities and zones, thresholds,
and a scoped assessment. The default requires three endpoints per backend,
100 observed requests per ready pod over ten minutes, max/min traffic ratio
at most two, healthy scrapes, and only mTLS for observed Order traffic.
Choose thresholds from approved measurements using `--min-requests`,
`--min-endpoints` and `--max-share-ratio`; record those choices. Missing traffic,
missing labels, invalid samples, query warnings, or inventory changes fail.
Dual-stack EndpointSlices are deduplicated by pod. A capture samples inventory
at its boundaries; retain the continuous rollout/watch history separately.

Reassess a saved capture without cluster access:

```bash
python3 scripts/phase5-evidence.py --input /tmp/phase5-baseline.json \
  --output /tmp/phase5-reassessment.json
```

Output files are never overwritten. Offline reassessment evaluates the saved
window, not current deployment health. A passing assessment covers only
observed traffic distribution, mTLS and scrape coverage. It does not establish
identity denial, success rate, traces, logs, rollback, or financial correctness.
Use the Mesh Acceptance dashboard for HTTP/gRPC outcomes and latency. Export
representative application traces before and after enrollment and verify
Order-to-backend parent/child propagation and outbox context remain intact.

## Failure and capacity matrix

Run one controlled disturbance at a time, under the same authenticated load
profile and with continuous application latency/error/integrity measurement.
Prepare the experiment and its restoration commands against the selected
non-production topology before executing it. Preserve time series through the
incident; the stable-window evidence script is for baseline and recovery.

| Exercise | Injection and restoration | Required evidence |
| --- | --- | --- |
| Scale-out | Raise one backend's Terraform-managed HPA minimum by one within its maximum; apply, then restore it. | New pod receives RPCs without restarting Order; endpoint inventory and proxy resource usage agree; gather a fresh stable-window capture. |
| Rolling drain | Roll one backend through a reviewed pod-template change, then restore its prior revision if needed. | Endpoint readiness withdrawal, no new selection after propagation, in-flight RPC completion or bounded cancellation inside the 40-second pod grace period; no duplicate financial effect. |
| Isolated endpoint loss | Delete one selected non-production backend pod and let its Deployment replace it. | Endpoint removal and replacement, bounded failures, continued healthy peer traffic, no synchronized retry wave. Pod replacement does not prove outlier ejection. |
| Node or zone loss | Use the environment's approved node/zone fault experiment and its recovery action. | Eligible capacity and per-zone inventories throughout, admitted throughput/error/latency budgets, recovery without overload. Respect disruption budgets. |
| Shared dependency | Use the environment's bounded database/broker latency or outage experiment; restore dependency connectivity. | Bounded queues/retries and rejection, no backend ejection, recovery without a surge, matching financial invariants. |
| Fleet overload | Ramp the synthetic load above measured sustainable capacity and back down. | Local admission and proxy pool rejections occur before uncontrolled timeouts; successful throughput, p95/p99, pending work and memory recover. Record max Order replicas because pool bounds are per caller. |
| Istiod/CNI/collector disruption | Use the approved platform component experiment, one component at a time. | Existing traffic behavior, new-pod startup and networking, telemetry gaps, alerts, and recovery. Do not accept absent telemetry as zero errors. |
| Mesh rollback | Reverse incremental enrollment and restore prior traffic limits. | Correct traffic and application telemetry throughout, no proxies or workload policies after full removal, financial integrity unchanged. |

For each run, retain proxy CPU/memory (including peaks), CPU throttling/OOM
signals, mesh/app latency deltas, metric series count, log bytes/retention,
query/trace exports, load/retry counts, rollout events and invariant results.
`kubectl top pods --containers` is a point-in-time aid, not peak evidence.
Current allowlisted mesh metrics do not expose active-request or ejection
state; gather proxy cluster stats with the matching `istioctl` and retain
service/zone context. No capacity budget is inferred from the configured
64/64/256 initial pool limits.

## Outlier detection gate

Outlier detection remains disabled. Do not add a consecutive-error-only policy
as a substitute for the roadmap's request-volume, recovery and capacity gates.
Before selecting and implementing a policy, require measured healthy capacity
after removal, a minimum request volume per detector window, per-zone active
and ejected endpoint telemetry, a bounded ejection period and percentage, and
a tested mechanism for gradual reintroduction. With three backend pods, one
removal already takes away one third of the fleet; small percentage settings
must be checked against the actual proxy rounding/minimum-host behavior.

Run isolated failure, correlated dependency failure, overload and recovery
experiments against the candidate policy in a separate rollout. Compare them
with the disabled-policy baseline and reject any policy that worsens useful
throughput or creates flapping. Review implementation against the pinned
Istio/Envoy API: not all volume or reinstatement controls are exposed by a
DestinationRule. Record any required extension in an ADR before introducing it.
The phase remains incomplete until these environment-dependent protections,
implementation and acceptance evidence exist.

## References

- [Istio 1.31.1 release](https://istio.io/latest/news/releases/1.31.x/announcing-1.31.1/)
- [Supported releases](https://istio.io/latest/docs/releases/supported-releases/)
- [Istio CNI](https://istio.io/latest/docs/setup/additional-setup/cni/)
- [Outlier safeguards](outlier-detection-ejection-spirals.md)
