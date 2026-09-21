# Praxis workloads on EKS

This directory deploys **Order, Ledger, and the mock Matching Engine** onto
EC2-backed EKS nodes. The AWS Terraform stack continues to own MSK, PostgreSQL,
ECR, and the ECS Fargate Outbox Relay. Reporting and Notification are not
implemented and have no Kubernetes workloads here.

## Provision the cluster

EKS is always provisioned; Order, Ledger, and Matching have no ECS service
definitions. In `infra/aws/terraform.tfvars`, set `eks_admin_principal_arn` to a
dedicated IAM operator role and review the pinned `eks_version`.
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
and separate Secrets Manager access for the Ledger runtime and migration Job,
and security-group paths to the existing MSK and RDS. It does **not** deploy
Kubernetes objects or run database migrations. Run the serialized one-off
Ledger migration Job before deploying a new Ledger image:

```bash
./infra/aws/run-migrations.sh ledger
```

This requires AWS CLI, Terraform, jq, envsubst, and kubectl with access to the
private EKS API. The Job's Pod Identity role reads the RDS admin secret
directly from Secrets Manager; the operator does not need to retrieve the
password. Outbox migrations still use a one-off ECS Fargate task.

## Deploy workloads

Set digest-pinned `order_image_digest`, `ledger_image_digest`, and
`matching_image_digest` in Terraform and apply them first. The mock Matching
Engine has no durable order book, partition lease, or fencing; keep it at one
replica. Its pod starts with empty in-memory state.

Ledger's Pod Identity role reads the restricted runtime password directly from
Secrets Manager at startup. The migration Job uses a separate role with access
to the RDS admin secret. Neither password is copied into a Kubernetes Secret,
Terraform state, or the operator's shell. Restart Ledger after rotating its
runtime secret so it obtains the new password. Matching has its own MSK IAM
role, while Order has no AWS role.

From a machine that can reach the EKS API and has `aws`, `terraform`, `jq`,
`envsubst`, and `kubectl` installed:

```bash
./infra/k8s/deploy.sh
kubectl -n praxis get pods,services
kubectl -n praxis port-forward service/order 8083:8083
```

The script verifies pinned images, configures non-secret settings, applies the
manifests, and
waits for all three rollouts. The manifest keeps Order private (`ClusterIP`),
routes its gRPC calls through Kubernetes Services, uses restricted non-root
Pods, and keeps Matching at one replica with `Recreate` updates. Do not scale
Matching above one until it has durable state and fenced partition ownership.

## Handoff and rollback

EKS is the **default compute platform** for these three services, but Terraform
does not apply Kubernetes manifests: run `deploy.sh` after provisioning the
cluster and images. This is not an automatic traffic cutover.
These manifests do not add a public ingress or authentication. Verify Order
through port-forward or another controlled private route before connecting an
authenticated edge. There is no ECS Order rollback service in this stack;
rollback requires a prior Kubernetes image and a tested restore procedure.

Keep the Outbox Relay on ECS Fargate throughout. It reads Ledger's existing
outbox in RDS and publishes to MSK; no Kubernetes relay should be started.
Rollback must reverse traffic routing first, then restore the prior Kubernetes
image or manifest. For Ledger, verify journal invariants and consumer lag
during a rollout.

Kubernetes application log shipping, managed trace/metric export, workload
network policies, ingress authentication, restore drills, and a durable
Matching implementation remain separate work. Do not call this a completed
production migration until those controls and live failure/rollback tests pass.
