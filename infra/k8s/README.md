# Praxis workloads on EKS

This Terraform stack deploys Order, Ledger, the mock Matching Engine, and a
shared metrics/traces Collector to the EC2-backed EKS cluster created by
`infra/aws`. The AWS stack also owns MSK, PostgreSQL, ECR, IAM, and the ECS
Fargate Outbox Relay. Reporting and Notification are not implemented.

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
pre-created secrets. Then set the pinned Order, Ledger, Matching, and ADOT
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
Collector Deployment, and three application Deployments after the Job. The
Job name changes with its pinned image, so a new migration image runs again.
Ledger's database advisory lock serializes migrations with Outbox's
independent ECS migration task.

Ledger reads its runtime password directly from Secrets Manager through its
Pod Identity role. The migration Job has a separate role limited to the RDS
admin secret. Neither password is stored in Kubernetes Secrets or Terraform
state. A rotated runtime password is fetched when the Ledger Pod restarts.

The Kubernetes stack deploys one shared ADOT Collector in the restricted
`observability` namespace. Its Prometheus receiver discovers the three
application pods and reads `/metrics` on each pod's named `http` port, then
remote-writes to AMP with SigV4. Its OTLP receiver accepts traces from the
three Go services over the private `otel-collector` Service and sends them to
X-Ray. The collector's Pod Identity role has AMP and X-Ray write permissions;
its Kubernetes Role can watch only pods in `praxis`. No per-application sidecar
is needed. Check `up{job="praxis-eks-hot-path"}` in AMP and generate a sampled
request to verify a trace in X-Ray. Container-log shipping is still separate.

The manifest exposes Order on NodePort 30083 for the public ALB in
`infra/aws`, routes its gRPC calls through Kubernetes Services, uses restricted
non-root Pods, and keeps Matching at one replica with `Recreate` updates. The
mock Matching Engine has no durable order book or fenced ownership; do not
scale it above one. This stack adds no edge authentication.

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

Matching remains one replica, so Order-to-Matching and Matching-to-Kafka traffic
cannot be guaranteed AZ-local. RDS cluster endpoints are role-aware rather than
zone-aware; Ledger-to-PostgreSQL traffic can also cross zones when the selected
database writer or reader is elsewhere. These settings reduce avoidable
cross-zone hops but do not guarantee an entirely single-AZ order path.

## Verify and operate

Terraform waits for the migration Job and Deployment rollouts. From a machine
that can reach the private EKS API, verify further with `kubectl`:

```bash
kubectl -n praxis get jobs,pods,services
kubectl -n praxis port-forward service/order 8083:8083
```

Outbox Relay and its migration task remain on ECS Fargate. Run
`./infra/aws/run-migrations.sh outbox` separately after the Ledger migration
has completed; the task also bootstraps both runtime roles. Kubernetes
application log shipping, live trace/metric export verification, network policies,
authenticated ingress, restore drills, and durable Matching remain separate
work. No live AWS deployment or failure drill has been performed by this
repository change.
