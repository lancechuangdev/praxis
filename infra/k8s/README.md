# Praxis workloads on EKS

This Terraform stack deploys Order, Ledger, and the mock Matching Engine to
the EC2-backed EKS cluster created by `infra/aws`. The AWS stack also owns
MSK, PostgreSQL, ECR, IAM, and the ECS Fargate Outbox Relay. Reporting and
Notification are not implemented.

## Apply order

First apply the AWS stack. Set a real `eks_admin_principal_arn` and a pinned
`ledger_migration_image_digest` in `infra/aws/terraform.tfvars`. If deploying
Outbox, also set `outbox_migration_image_digest`. The Kubernetes
stack reads that stack's local `terraform.tfstate` directly. If the AWS state
is elsewhere, set `aws_state_path` to its local path.

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

After the Job completes, run the Outbox ECS migration if deploying Outbox,
then create the restricted PostgreSQL runtime roles and Ledger runtime secret
as described in [`../aws/README.md`](../aws/README.md).
Set `ledger_runtime_secret_arn` and the pinned Order, Ledger, and Matching
image digests in `infra/aws/terraform.tfvars`, then apply that stack again.
Finally, deploy the workloads with the Kubernetes stack's default setting:

```bash
terraform -chdir=infra/aws apply
terraform -chdir=infra/k8s plan
terraform -chdir=infra/k8s apply
```

The Kubernetes provider authenticates with `aws eks get-token`; no kubeconfig
or `deploy.sh` is required. Terraform creates the restricted `praxis`
Namespace and ServiceAccounts, then runs a one-off Ledger migration Job and
waits for success. With `deploy_workloads=true` (the default), it also creates
the non-secret ConfigMap, private Services, and all three Deployments after
the Job. The Job name changes with its pinned image, so a new migration image
runs again. Ledger's database advisory lock serializes migrations with
Outbox's independent ECS migration task.

Ledger reads its runtime password directly from Secrets Manager through its
Pod Identity role. The migration Job has a separate role limited to the RDS
admin secret. Neither password is stored in Kubernetes Secrets or Terraform
state. A rotated runtime password is fetched when the Ledger Pod restarts.

The manifest keeps Order private (`ClusterIP`), routes its gRPC calls through
Kubernetes Services, uses restricted non-root Pods, and keeps Matching at one
replica with `Recreate` updates. The mock Matching Engine has no durable
order book or fenced ownership; do not scale it above one. This Terraform
stack adds no public ingress or edge authentication.

## Verify and operate

Terraform waits for the migration Job and Deployment rollouts. From a machine
that can reach the private EKS API, verify further with `kubectl`:

```bash
kubectl -n praxis get jobs,pods,services
kubectl -n praxis port-forward service/order 8083:8083
```

Outbox Relay and its migration task remain on ECS Fargate. Run
`./infra/aws/run-migrations.sh outbox` separately after the Ledger migration
has completed and before bootstrapping the Outbox runtime role. Kubernetes application log shipping,
managed trace/metric export, network policies, authenticated ingress, restore
drills, and durable Matching remain separate work. No live AWS deployment or
failure drill has been performed by this repository change.
