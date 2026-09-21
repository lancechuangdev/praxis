# Praxis AWS infrastructure

This Terraform stack creates a three-AZ VPC, provisioned MSK cluster and
topics, PostgreSQL Multi-AZ Ledger database, ECR repositories, and an EKS
cluster with EC2 managed nodes for Order, Ledger, and Matching. It also creates
an ECS Fargate cluster for Outbox Relay and its one-off database migration.
Reporting and Notification are not implemented. No Order ALB or public ingress
is provisioned; the Kubernetes Order Service is private.

## Provision

Terraform needs AWS permissions for EKS, EC2/VPC, MSK, RDS, IAM, KMS, ECR,
Secrets Manager, CloudWatch, and ECS. Copy `terraform.tfvars.example` to
`terraform.tfvars`, set a dedicated `eks_admin_principal_arn`, and review the
selected Region, EKS version, instance sizes, storage, and cost. The EKS API
is private unless narrow `eks_public_access_cidrs` are configured.

```bash
terraform init
terraform plan
terraform apply
```

The default network has three Availability Zones and one NAT gateway per AZ.
`nat_gateway_per_az = false` reduces cost but makes outbound private-subnet
traffic depend on one AZ. Kafka topic creation is disabled in applications;
manage topics through the Terraform `topics` variable. Use the
`bootstrap_brokers_sasl_iam` output for IAM/TLS Kafka clients.

Terraform creates immutable, scan-on-push ECR repositories for all four
services. Publish images with unique tags, resolve their digests, and set
`order_image_digest`, `ledger_image_digest`, `matching_image_digest`, and
`outbox_image_digest` as appropriate. The three hot-path digests are consumed
by the `infra/k8s` Terraform stack, not ECS task definitions.

## Database migrations and credentials

Ledger migration runs as a one-off EKS Job on EC2 nodes. Set
`ledger_migration_image_digest` and apply this AWS stack, then run
`terraform -chdir=infra/k8s apply -var='deploy_workloads=false'` from the
repository root. The Kubernetes Terraform stack waits for the Job to complete
before it can deploy Ledger. The Job reads the RDS admin password directly
from Secrets Manager through its dedicated Pod Identity role. The operator
needs access to the private EKS API but not to the admin password. The
password never enters Terraform state or a Kubernetes Secret.

Before applying the Outbox migration task definition, create two distinct
Secrets Manager secrets with JSON shape `{"password":"..."}` and set
`ledger_runtime_secret_arn` and `outbox_runtime_secret_arn` in Terraform.
Keep passwords out of Terraform state and shell history. Outbox migration
remains a one-off ECS Fargate task. Set `outbox_migration_image_digest`, apply
Terraform, and run `./run-migrations.sh outbox` after the Ledger Job completes.
Use the exact migration image digest that will be deployed as the service.
ECS injects the two runtime passwords from Secrets Manager into the migration
task. After applying the Outbox schema, it creates or updates the restricted
`ledger_runtime` and `outbox_runtime` PostgreSQL roles and grants their
privileges; no manual `psql` step is needed. Re-running the task reapplies
the passwords and grants. Both schema migration paths and role bootstrap
serialize through the same PostgreSQL advisory lock.

Ledger reads its runtime password directly from Secrets Manager using Pod
Identity; the Outbox password is injected into its ECS task from Secrets
Manager. Use the RDS writer endpoint for both services and reserve the reader
endpoint for replica-lag-tolerant queries.

## Kubernetes hot path

EKS is always provisioned. This AWS Terraform stack creates the private
control plane, EC2 node group, core add-ons, Pod Identity roles for Ledger and
Matching, and security-group paths to MSK and RDS. The separate Kubernetes
Terraform stack applies the workload resources. Follow
[`../k8s/README.md`](../k8s/README.md) to deploy Order, Ledger, and
Matching. Order has no AWS role; Ledger can consume its command topic and
Matching can publish its event topic through separate scoped Pod Identity
roles. The mock Matching Engine must remain single-replica until it has
durable state and fenced ownership.

## ECS Outbox Relay

Set `outbox_image_digest` after its migration and runtime credential are
ready. Terraform starts one private Fargate task with no public endpoint. It
reads Ledger's outbox from the writer database, maps stored `ledger-events`
to `ledger.events.v1`, and publishes to MSK with IAM/TLS. Its ECS task role is
scoped to that event topic. It is not deployed to Kubernetes.

The ECS cluster also supplies the migration tasks. Its default capacity
provider is Fargate; Fargate Spot is registered but not used by the relay.
Only the relay receives ECS tracing and task-local metric scraping when the
optional ADOT collector is configured. Terraform always creates an AMP
workspace. When present, the relay collector signs remote writes to it. An AMP
managed scraper discovers the `praxis` namespace's Order, Ledger, and Matching
pods and scrapes their named `http` ports (`8083`, `8081`, and `8084`) every
30 seconds. The scraper uses private subnets, EKS API access entries, and a
dedicated security group limited to the API and those metrics ports. No
collector sidecars are needed in the EKS application pods. Terraform also
defines AMP rules for the hot-path metrics. No Alertmanager receiver or
notification action is configured.

After applying both Terraform stacks and deploying the pods, check the
`managed_metrics_eks_scraper_id` output and the scraper's status in AMP. Query
`up{job="praxis-eks-hot-path"}`: each running Order, Ledger, and Matching pod
should appear with value `1`. Then query, for example,
`rate(order_requests_total{service="order"}[5m])`. Missing `up` series mean the
pod was not discovered; `up == 0` means it was found but scraping failed.
The EKS metrics path is independent of the still-unfinished EKS traces and
container-log pipelines.

CloudWatch alarms cover Outbox task availability, collector export failures,
and Ledger Kafka consumer lag. Application and collector log retention is
controlled by `ecs_log_retention_days`. The EKS control-plane log group uses
the same retention setting. No live deployment or rollback drill is implied
by this Terraform configuration.
