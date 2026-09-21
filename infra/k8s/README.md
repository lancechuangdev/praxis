# Praxis workloads on EKS

This directory deploys **Order, Ledger, and the mock Matching Engine** onto
EC2-backed EKS nodes. The AWS Terraform stack continues to own MSK, PostgreSQL,
ECR, and the ECS Fargate Outbox Relay. Reporting and Notification are not
implemented and have no Kubernetes workloads here.

## Provision the cluster

In `infra/aws/terraform.tfvars`, set `eks_enabled = true`, a supported
`eks_version`, and `eks_admin_principal_arn` to a dedicated IAM operator role.
EKS is billable. The default node group has three `m6i.xlarge` EC2 instances;
review instance type, node count, subnet IP capacity, and cost before applying.
The Kubernetes API is private by default. Run `kubectl` from within the VPC or
set narrow `eks_public_access_cidrs` for operator access. Do not use
`0.0.0.0/0`. The operator must use the role named by
`eks_admin_principal_arn`.

```bash
terraform -chdir=infra/aws plan
terraform -chdir=infra/aws apply
```

Terraform provisions the EKS control plane, EC2 managed node group, standard
networking add-ons, Pod Identity agent, scoped Ledger/Matching MSK IAM roles,
and security-group paths to the existing MSK and RDS. It does **not** deploy
Kubernetes objects or run database migrations. Keep using the existing
serialized one-off Ledger migration task before deploying a new Ledger image.

## Deploy workloads

Set digest-pinned `order_image_digest`, `ledger_image_digest`, and
`matching_image_digest` in Terraform and apply them first. Set the ECS task
counts for both Matching services to zero and confirm they have stopped. The
mock engine has no durable order book, partition lease, or fencing; running it
on ECS and Kubernetes simultaneously is unsafe. Pause Order admission during
this stop/start handoff. A new Matching pod starts with empty in-memory state.

Ledger uses the **same** RDS database and Kafka consumer group on both
platforms. By default, `deploy.sh` refuses to start Kubernetes Ledger while
an ECS Ledger task is active. If a deliberate, monitored overlap is needed,
run it with `ALLOW_LEDGER_PARALLEL_CONSUMER=true`; the Kubernetes pod will
immediately process real commands. Ledger's restricted runtime password is
read from Secrets Manager by the deploy operator and streamed into a
Kubernetes Secret, never committed to this repository. Rotate it by rerunning
the deployment; the operator's AWS identity needs permission to read that
specific secret. The Pods use separate EKS Pod Identity roles for MSK IAM;
Order has no AWS role.

From a machine that can reach the EKS API and has `aws`, `terraform`, `jq`,
`envsubst`, and `kubectl` installed:

```bash
./infra/k8s/deploy.sh
kubectl -n praxis get pods,services
kubectl -n praxis port-forward service/order 8083:8083
```

The script checks ECS Matching ownership, verifies pinned images, configures
non-secret settings, refreshes the Ledger Secret, applies the manifests, and
waits for all three rollouts. The manifest keeps Order private (`ClusterIP`),
routes its gRPC calls through Kubernetes Services, uses restricted non-root
Pods, and keeps Matching at one replica with `Recreate` updates. Do not scale
Matching above one until it has durable state and fenced partition ownership.

## Handoff and rollback

This is an **opt-in deployment path**, not an automatic traffic cutover.
The existing ECS Order ALB still returns a fixed 503, and these manifests do
not add a public ingress or authentication. Verify Order through port-forward
or another controlled private route before connecting an authenticated edge.
Once a separate ingress/cutover is tested, set the ECS Order desired counts to
zero so those services stay available for rollback without serving traffic.
Terraform then owns those counts; an apply can reconcile any ECS autoscaling
drift back to the configured value.

Keep the Outbox Relay on ECS Fargate throughout. It reads Ledger's existing
outbox in RDS and publishes to MSK; no Kubernetes relay should be started.
Rollback must reverse traffic routing first, then restore the prior ECS task
counts. For Matching, stop the Kubernetes pod before restarting the ECS mock
to avoid two sequence owners. For Ledger, expect Kafka consumer-group
rebalancing and verify journal invariants and lag during the handoff.

Kubernetes application log shipping, managed trace/metric export, workload
network policies, ingress authentication, restore drills, and a durable
Matching implementation remain separate work. Do not call this a completed
production migration until those controls and live failure/rollback tests pass.
