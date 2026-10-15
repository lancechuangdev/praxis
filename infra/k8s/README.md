# Praxis workloads on EKS

This Terraform stack deploys Order, Ledger, the mock Matching Engine, Risk, and
a shared metrics/traces Collector to the EC2-backed EKS cluster created by
`infra/aws`. The AWS stack also owns MSK, PostgreSQL, ECR, IAM, and the ECS
Fargate Outbox Relay. Reporting and Notification are not implemented. Optional
Phase 5 Istio enrollment is disabled by default.

## Apply order

First apply the AWS stack. Set a real `eks_admin_principal_arn` and a pinned
`ledger_migration_image_digest` in `infra/aws/terraform.tfvars`. Create the
distinct Ledger and Outbox runtime password secrets described in
[`../aws/README.md`](../aws/README.md) and set both secret ARNs. Also set
`outbox_migration_image_digest`. The Kubernetes stack reads that stack's local
`terraform.tfstate` directly. If the AWS state is elsewhere, set
`aws_state_path` to its local path.

```bash
terraform -chdir=infra/aws init
terraform -chdir=infra/aws apply
```

The EKS API is private by default, so run the Kubernetes commands from a
machine with network access to the VPC and AWS credentials for the IAM role
named by `eks_admin_principal_arn`. First apply in migration-only mode:

```bash
terraform -chdir=infra/k8s init
terraform -chdir=infra/k8s apply -var='deploy_workloads=false'
```

After the Job completes, run the Outbox ECS migration. It applies the Outbox
schema and creates the restricted Ledger and Outbox runtime roles using the
pre-created secrets. Then set the pinned Order, Ledger, Matching, Risk, and ADOT
collector image digests in `infra/aws/terraform.tfvars` and apply that stack again.
Finally, deploy the workloads with the Kubernetes stack's default setting:

```bash
terraform -chdir=infra/aws apply
terraform -chdir=infra/k8s plan
terraform -chdir=infra/k8s apply
```

The Kubernetes provider authenticates with `aws eks get-token`; no kubeconfig
or `deploy.sh` is required. Terraform creates the restricted `praxis` and
`observability` Namespaces and ServiceAccounts, then runs a one-off Ledger
migration Job and waits for success. With `deploy_workloads=true` (the
default), it also creates the non-secret ConfigMaps, private Services, shared
Collector Deployment, and four application Deployments after the Job. The
Job name changes with its pinned image, so a new migration image runs again.
Ledger's database advisory lock serializes migrations with Outbox's
independent ECS migration task.

Ledger reads its runtime password directly from Secrets Manager through its
Pod Identity role. The migration Job has a separate role limited to the RDS
admin secret. Neither password is stored in Kubernetes Secrets or Terraform
state. A rotated runtime password is fetched when the Ledger Pod restarts.

The Kubernetes stack deploys an ADOT Collector DaemonSet in the restricted
`observability` namespace. Each Prometheus receiver discovers the application
pods on its own node and reads `/metrics` on their named `http` ports, then
remote-writes to AMP with SigV4. Node filtering prevents duplicate samples
without retaining a single collector-pod failure domain. The collectors accept
OTLP traces over the private `otel-collector` Service and send them to X-Ray.
Their Pod Identity role has AMP and X-Ray write permissions; their Kubernetes
Role can watch only pods in `praxis`. No per-application sidecar is needed.
Check `up{job="praxis-eks-hot-path"}` in AMP and generate a sampled request to
verify a trace in X-Ray.

The manifest exposes Order on NodePort 30083 for the public ALB in `infra/aws`.
The ALB terminates TLS and requires OIDC authentication before forwarding. Its
callback URL is `https://<order-alb-dns>/oauth2/idpresponse`. Protect the OIDC
client secret and the Terraform state that contains it.

Order, Ledger, Matching, and Risk use CPU HPAs backed by the pinned EKS metrics-server
add-on and disruption budgets for voluntary maintenance. CloudWatch application
logs and Container Insights use the pinned CloudWatch Observability add-on and
Pod Identity. Scaling limits are initial safety bounds, not production capacity
claims; validate them under load. The mock Matching Engine still lacks fenced
market ownership and complete order-book recovery, so it is not production
matching even when replicas are healthy.

Order runs six replicas with hard zone and hostname spread constraints, placing
two replicas per Availability Zone on separate nodes. Ledger runs three replicas
with one replica per zone. Their Services use `trafficDistribution: PreferSameZone`, so
Order-to-Ledger connections select a Ledger Pod in the caller's zone when a
healthy endpoint exists there and fall back to another zone otherwise. The Order
NodePort uses `externalTrafficPolicy: Local`; together with cross-zone routing
disabled on the ALB target group, an ALB node sends ingress only to an Order Pod
on a node in the same zone. Nodes without a local ready Order Pod fail the
target-group health check and receive no ingress. This requires at least six EKS
nodes and increases the baseline EC2 cost; if a zone lacks two eligible nodes,
the affected Order replica remains Pending instead of colocating.

Matching runs three replicas spread across the Availability Zones. Its Kafka
traffic and the RDS cluster endpoints are role-aware rather than
zone-aware; Ledger-to-PostgreSQL traffic can also cross zones when the selected
database writer or reader is elsewhere. These settings reduce avoidable
cross-zone hops but do not guarantee an entirely single-AZ order path.

Risk runs three replicas spread across the three Availability Zones behind the
private `risk.praxis.svc.cluster.local:9095` Service. Order uses it in shadow or
authoritative mode according to `ORDER_RISK_REMOTE_MODE`; the checked-in EKS
deployment remains in shadow mode. Its immutable startup policy is deliberately
aligned with the embedded `embedded-v1` policy.

## Optional Istio enrollment

Install a version-pinned Istio sidecar control plane before enabling this
stack's mesh resources. Control-plane installation and upgrades deliberately
remain a separate operator-owned lifecycle; the Kubernetes provider cannot
safely bootstrap custom resources before their CRDs exist.
Follow the [Istio installation runbook](../../docs/istio-installation.md) for
the install order, verification, acceptance gates, and rollback procedure.

Set `enable_istio_mesh=true` only in a non-production workspace first. Use
`mesh_enrolled_workloads` to enroll `["order"]`, then `["order", "ledger"]`,
then all three workloads. A backend cannot be enrolled without Order. Reverse
those steps during rollback and wait for each rollout before continuing.
The default set contains all three services for compatibility with existing
environments. The [Phase 5 acceptance procedure](../../docs/phase-5-acceptance.md)
covers the pinned CNI/control-plane installation, identity probes, AMP evidence,
failure/capacity matrix and rollback. CNI is required because Praxis keeps
restricted Pod Security enabled. The flag:

- injects sidecars into Order, Ledger, and Matching, but not Risk;
- requires mTLS on their internal application ports while preserving the HTTP
  health, metrics, and public Order ingress ports;
- permits only Order's SPIFFE service identity to call the Ledger and Matching
  gRPC ports;
- selects backend endpoints round-robin with bounded connections and pending
  HTTP/2 requests;
- adds a metric- and label-allowlisted Istio proxy scrape to the node-local
  collectors; and
- logs only responses with status 400 or higher and connections that fail
  before producing a response, using Istio's built-in Envoy stdout provider.

The `mesh_backend_connection_limits` map sets independent Ledger and Matching
`max_connections`, `max_pending_requests`, and `max_active_requests` values.
Both backend entries are required; each value must be a positive integer.
Defaults remain 64 connections, 64 pending requests, and 256 active requests.
These are per-caller-proxy bounds, not an authoritative fleet concurrency quota.
Choose values using backend saturation measurements and the maximum number of
Order replicas; repeat overload, shared-dependency, and rolling-drain exercises
after tuning. Roll back by restoring the previous map and applying Terraform.
Istio documents these fields in its
[DestinationRule reference](https://istio.io/latest/docs/reference/config/networking/destination-rule/#ConnectionPoolSettings).

After applying, `make phase5-verify` reads the Kubernetes stack's applied
`mesh_backend_connection_limits` output and checks live policy against it.
Reapply older state to populate this output before running the updated preflight.
Run `python3 -m unittest discover -s scripts/tests` for offline preflight tests.

No mesh retry policy or outlier detection is enabled. Before acceptance, prove
identity denial, `mutual_tls` telemetry, even endpoint distribution, scaling,
drain, overload, correlated dependency failure, and unchanged application
metrics/traces. Verify that error and denial logs arrive in CloudWatch without
authorization headers or financial payloads; the default Envoy format records
request paths and network addresses, so retention and access must follow the
environment's log-data policy. Record the Istio version, proxy resource
overhead, and results with the Phase 1 evidence. To roll back, set
`enable_istio_mesh=false`, apply, and verify all three Deployments have completed
their sidecar-removal rollout; do not uninstall the control plane until no
enrolled pods or mesh policies remain.

## Verify and operate

Terraform waits for the migration Job and Deployment rollouts. From a machine
that can reach the private EKS API, verify further with `kubectl`:

```bash
kubectl -n praxis get jobs,pods,services
kubectl -n praxis port-forward service/order 8083:8083
```

Outbox Relay and its migration task remain on ECS Fargate. Run
`./infra/aws/run-migrations.sh outbox` separately after the Ledger migration
has completed; the task also bootstraps both runtime roles.

After deployment, run `../../scripts/verify-phase1.sh` and complete the evidence
and failure exercises in
[`../../docs/phase-1-production-baseline.md`](../../docs/phase-1-production-baseline.md).
When the optional mesh is enabled, also run `../../scripts/verify-phase5.sh`
and complete the live exercises in the
[Istio installation runbook](../../docs/istio-installation.md).
Live trace, metric, log, authentication, alert-delivery, scaling, restore, and
failure verification remain operator gates. Network policies and durable
Matching also remain separate work. No live AWS deployment or failure drill is
implied by this repository configuration.
