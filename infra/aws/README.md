# CEX AWS infrastructure

This Terraform stack provisions a production-oriented, provisioned Amazon MSK
cluster, its Kafka topics, and a PostgreSQL Multi-AZ DB cluster for the ledger.

This stack is self-contained. It does not read or reuse an external VPC,
subnet, route, or security-group resources.

It creates:

- A dedicated CEX VPC spanning three Availability Zones
- Public and private subnets, an internet gateway, NAT gateways, and routes
- Dedicated CEX application-client and MSK security groups
- A three-broker MSK cluster in the new CEX private subnets
- A PostgreSQL Multi-AZ DB cluster with one writer and two readable standbys
- Immutable, scan-on-push ECR repositories for each current service
- An ECS cluster with Fargate and opt-in Fargate Spot capacity
- A shared task execution role and per-service CloudWatch log groups
- A distinct IAM task role for each current application service
- An optional HTTPS-only Order ALB foundation, disabled by default
- A private Cloud Map namespace with Ledger and Matching gRPC service records
- An opt-in, private, single-replica Matching Engine ECS service
- An opt-in, private, single-replica Ledger ECS service
- An opt-in, private, single-replica Outbox Relay ECS service
- One-off Ledger and Outbox schema migration task definitions
- An opt-in, private Order ECS service with CPU target tracking (1–3 tasks)
- RDS-managed database credentials stored in Secrets Manager
- IAM authentication and TLS-only client connections
- Separate KMS encryption keys for MSK and PostgreSQL
- CloudWatch broker logs and Prometheus broker exporters
- A broker security group allowing port `9098` from approved client security groups
- `matching.events.v1`, `ledger.commands.v1`, `ledger.events.v1`, and `ledger.events.dlq.v1`

## Deploy

Terraform and its AWS credentials need permission to manage MSK, topic API,
RDS, Secrets Manager, EC2 networking and security groups, KMS, and CloudWatch
Logs resources.

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

The default network uses three Availability Zones and one NAT gateway per AZ.
Set `nat_gateway_per_az = false` for a cheaper non-production environment; that
reduces cost but makes outbound private-subnet traffic depend on one AZ. The
broker count must be a multiple of the Availability Zone count. Topic
replication factor `3` requires at least three brokers.

## Network structure

```mermaid
flowchart TB
    Internet((Internet))
    IGW[Internet gateway]

    subgraph VPC[Independent CEX VPC — 10.80.0.0/16]
        ClientSG[CEX client security group]
        MSKSG[MSK security group\nIAM/TLS :9098]
        PGSG[PostgreSQL security group\nTCP :5432]

        subgraph AZA[Availability Zone A]
            PubA[Public subnet A]
            NATA[NAT gateway A]
            PrivA[Private subnet A]
            AppA[Ledger / relay tasks]
            MSKA[MSK broker 1]
            PGWriter[(PostgreSQL writer)]
            PubA --> NATA
            PrivA --> NATA
            AppA --- PrivA
            MSKA --- PrivA
            PGWriter --- PrivA
        end

        subgraph AZB[Availability Zone B]
            PubB[Public subnet B]
            NATB[NAT gateway B]
            PrivB[Private subnet B]
            AppB[Ledger / relay tasks]
            MSKB[MSK broker 2]
            PGReaderB[(PostgreSQL reader / standby)]
            PubB --> NATB
            PrivB --> NATB
            AppB --- PrivB
            MSKB --- PrivB
            PGReaderB --- PrivB
        end

        subgraph AZC[Availability Zone C]
            PubC[Public subnet C]
            NATC[NAT gateway C]
            PrivC[Private subnet C]
            AppC[Ledger / relay tasks]
            MSKC[MSK broker 3]
            PGReaderC[(PostgreSQL reader / standby)]
            PubC --> NATC
            PrivC --> NATC
            AppC --- PrivC
            MSKC --- PrivC
            PGReaderC --- PrivC
        end

        ClientSG -->|Kafka :9098| MSKSG
        ClientSG -->|PostgreSQL :5432| PGSG
        MSKSG --- MSKA
        MSKSG --- MSKB
        MSKSG --- MSKC
        PGSG --- PGWriter
        PGSG --- PGReaderB
        PGSG --- PGReaderC
        PGWriter -. semisynchronous replication .-> PGReaderB
        PGWriter -. semisynchronous replication .-> PGReaderC
    end

    Internet --- IGW
    IGW --- PubA
    IGW --- PubB
    IGW --- PubC
```

Normal Kafka and PostgreSQL traffic remains inside the VPC. NAT gateways are
only for outbound connections initiated by workloads in private subnets.

## PostgreSQL connections

RDS exposes logical cluster endpoints rather than requiring applications to
track individual database instances:

```text
ledger_writer_endpoint ──► current writer
ledger_reader_endpoint ──► reader B or reader C, selected per connection
```

Use the writer endpoint for ledger posting, balance reservation, migrations,
and the outbox relay. Use the reader endpoint only for replica-lag-tolerant
reporting and historical queries. Credentials are generated by RDS; retrieve
them using the `ledger_master_secret_arn` output rather than putting a password
in `terraform.tfvars` or Terraform state.

Automatic Kafka topic creation is disabled. Add or change topics through the
`topics` variable so partition counts, replication, retention, and durability
settings remain version controlled.

The applications should use the `bootstrap_brokers_sasl_iam` output. ECS
services should run in the `private_subnet_ids` output and attach the
`cex_client_security_group_id` output. Their IAM roles must separately receive
the required `kafka-cluster:Connect`, read, write, and consumer-group
permissions.

## Container repositories

Terraform creates separate repositories for Order, Ledger, Matching, and the
Outbox Relay. Repository names include the application and environment, image
tags are immutable, and every push is scanned. Lifecycle policies delete
untagged images after seven days and retain the newest 50 tagged images by
default; both limits are configurable.

After applying the stack, retrieve image destinations with:

```bash
terraform output -json ecr_repository_urls
```

Build pipelines should publish a unique tag such as the Git commit SHA. ECS
task definitions should deploy the resolved image digest rather than a mutable
tag so a rollback always selects the same artifact.

## ECS cluster foundation

The ECS cluster enables enhanced Container Insights and registers both
`FARGATE` and `FARGATE_SPOT`. Its default strategy uses only `FARGATE`, so a
service cannot land on Spot accidentally. A later service definition may opt
an interruption-safe consumer into `FARGATE_SPOT` explicitly.

The shared task execution role has AWS's managed execution policy for ECR image
pulls and CloudWatch log delivery. Ledger uses a dedicated execution role so
its database secret permission is not shared. Execution roles are deliberately
separate from application task roles: Ledger, Matching, Order, and relays
receive narrowly scoped roles for their own Kafka, database-authentication,
secret, and other runtime
permissions. Terraform also creates a retained CloudWatch application log group
for every current service.

## Private gRPC discovery

Cloud Map provides private `A` records for Ledger and Matching inside the CEX
VPC. Retrieve the intended Order Service targets with:

```bash
terraform output -json grpc_service_addresses
```

The targets use the existing Ledger port `9091` and Matching port `9092`.
The records have a 10-second TTL and ECS-managed custom health status. Creating
the namespace and services alone does **not** register task IPs: the later ECS
service definitions must attach the corresponding
`grpc_service_discovery_arns` as service registries. Their task security groups
must also allow Order-to-Ledger and Order-to-Matching gRPC traffic.

## First ECS service: mock Matching Engine

Matching is the first ECS workload because it does not need a database secret.
It is **not** a production matching engine: partition sequence and request
deduplication live only in memory. The ECS service is absent by default. After
building and pushing an image to the Matching ECR repository, set
`matching_image_digest` to its `sha256:...` digest to create the task definition
and one private Fargate task. The task runs without a public IP, exports JSON
logs to its CloudWatch log group, authenticates to MSK with IAM/TLS, and
registers its private IP in Cloud Map.

The task's health command calls its local `/readyz` endpoint without requiring
a shell or `curl` in the non-root distroless image. ECS uses a stop-before-start
deployment (`minimum_healthy_percent = 0`, `maximum_percent = 100`) so a
replacement cannot overlap this mock's in-memory partition ownership; expect
brief unavailability during deployments. Do not scale it above one task until
partition leasing, fencing, and recovery are implemented. Order tasks must
later attach `order_grpc_client_security_group_id` to call its private gRPC
port `9092`; the Matching task group permits that source only. The existing
CEX client group supplies the task's outbound MSK access.

## Ledger ECS service

Ledger is also disabled by default. Build and push the Ledger image, run its
one-off migration task as described below, then set `ledger_image_digest` to
the same `sha256:...` digest to create one private Fargate task. It registers
its gRPC port `9091` in Cloud Map, consumes
`ledger.commands.v1` through MSK IAM/TLS, and uses `/readyz` for container
health checks. Only tasks with `order_grpc_client_security_group_id` can call
its gRPC port; there is no public Ledger listener.

The task reads a dedicated Ledger runtime secret's `password` JSON key through
its own execution role; that role cannot read the RDS admin secret. Its
database host, name, and username come from Terraform;
the password itself is not put in Terraform state. ECS injects the password at
task startup, so a secret rotation requires a new deployment. Provision the
restricted role as described below before enabling the service. Its
stop-before-start deployment can briefly interrupt Ledger requests.

## Outbox Relay ECS service

Run the Outbox migration task after Ledger's, then set `outbox_image_digest`
to its pushed image digest to create one private Fargate relay task. The task
has no public endpoint or inbound security-group
rule. It connects to the Ledger writer database and MSK with IAM/TLS, maps
stored `ledger-events` to `ledger.events.v1`, and reports health through its
local `/readyz` endpoint. Its hostname supplies a distinct lease owner ID.

The relay has its own execution role for its runtime password secret and its
own task role restricted to the Ledger event topic. It cannot read the RDS
admin secret or change the schema. The service starts at one task and uses
Fargate rather than Spot. A secret rotation requires a new deployment to
refresh the injected password.

## One-off database migration tasks

Set `ledger_migration_image_digest` or `outbox_migration_image_digest` to
register a Fargate migration task definition without starting its service.
Use the exact image digest that will later be assigned to the corresponding
service. The migration definition uses that image's `migrate` command.
The task receives only database connection settings and the RDS-managed
password; it does not receive the application's MSK task role, open a service
port, or run continuously. Retrieve its ARN with
`ledger_migration_task_definition_arn` or
`outbox_migration_task_definition_arn`. Run it as a standalone ECS task in a
private subnet with `cex_client_security_group_id` attached and public IP
assignment disabled. Wait for the task to stop and verify its container exit
code is zero before proceeding; `tasks-stopped` alone does not mean success.

Terraform **registers but does not execute** these tasks. Apply with migration
digest(s) first, then run `./run-migrations.sh all` from this directory (requires
AWS CLI, Terraform, and jq). The script uses Terraform outputs for the cluster,
private subnets, and security group; it runs Ledger before Outbox and checks
each task's container exit code. You can also pass `ledger` or `outbox` to run
one migration. Both migration runners serialize concurrent executions using
the same PostgreSQL advisory lock because they share an outbox table. After
the migrations succeed, connect to
the writer database from inside the VPC as the RDS admin and run
`psql -f bootstrap-runtime-roles.sql`; it prompts for separate Ledger and
Outbox runtime passwords and grants DML access without schema ownership.
Create two separate Secrets Manager secrets containing JSON
`{"password":"<matching password>"}`. Keep their ARNs in
`ledger_runtime_secret_arn` and `outbox_runtime_secret_arn`. The fixed
database usernames are `ledger_runtime` and `outbox_runtime`. Use the
AWS-managed Secrets Manager encryption key unless you
also grant the corresponding ECS execution role `kms:Decrypt` on a custom key.
Keep these passwords out of Terraform state and shell history. Only then set
the matching
`ledger_image_digest` and `outbox_image_digest` and apply again. On upgrades,
keep the old service digest until the new migration task succeeds. ECS services
set `LEDGER_MIGRATE_ON_STARTUP=false` and `OUTBOX_MIGRATE_ON_STARTUP=false`;
they verify embedded migration checksums and fail startup if a required
migration was skipped. Local Compose still runs migrations on startup.
Database-backed migration tests require `LEDGER_TEST_DATABASE_URL` and
`OUTBOX_TEST_DATABASE_URL`; they are skipped when those variables are unset.

## Private Order ECS service

Set `order_image_digest` to a pushed image digest only after Ledger and
Matching are configured. Terraform then creates one private Fargate Order task
that calls both gRPC services through Cloud Map and uses its local `/readyz`
endpoint for health checks. The task has no public IP. Its security group
allows outbound gRPC only to Ledger and Matching, plus HTTPS to AWS APIs
through NAT for image pulls and logs.

When `order_alb_enabled` is true, ECS registers Order tasks with the ALB target
group so ALB health checks can run. A separate HTTP listener associates the
target group but has **no security-group ingress rule** on its port 8080; do
not open that port. The public HTTPS listener still returns 503. Edge
authentication and HTTPS forwarding must be implemented before admitting
public orders. The current Order service still embeds mock Risk logic and the
Matching Engine remains an in-memory mock; this is not a production rollout.

Order uses ECS CPU target tracking at 60% with a 1–3 task range. Scaling out
waits 60 seconds and scaling in waits 120 seconds. Terraform ignores changes
to the service's `desired_count` after creation so autoscaling can own it.
This does not expose Order publicly; the HTTPS listener still returns 503.
Ledger, Matching, and Outbox are not autoscaled by this policy.

## Production ECS tracing

Local Compose uses an unauthenticated Collector and local Tempo; do not expose
those endpoints on a production network. ECS tracing is disabled until
`trace_collector_image` is set to a reviewed, digest-pinned ADOT image
(`public.ecr.aws/aws-observability/aws-otel-collector@sha256:...`). Use a
release at least v0.34.0 so the X-Ray exporter accepts W3C trace IDs. Set
`ecs_alarm_sns_topic_arn` to an existing topic with confirmed subscribers
before enabling it. Terraform then deploys an essential ADOT sidecar in each
enabled Order, Ledger, Matching, and Outbox task. It also raises the Fargate
task size to 1 vCPU and 2 GiB to give the collector headroom.

Application OTLP/gRPC export goes only to `127.0.0.1:4317` inside the same ECS
task; the receiver binds to loopback and has no port mapping or inbound
security-group rule. The collector uses the task role to authenticate to AWS
X-Ray, with only trace-write IAM actions. Tracing uses parent-based sampling
with `trace_sample_ratio` (default 0.1 for root spans) and tags the resource
with the deployment environment and service namespace. The collector adds ECS
task metadata. X-Ray is the managed trace backend and retains traces for 30
days; this retention cannot be changed. Application and collector CloudWatch
log groups use `ecs_log_retention_days` (default 30 days). The local Tempo
dashboard is not the AWS trace viewer; use the X-Ray console for ECS traces.

The existing no-running-tasks alarm covers an essential sidecar that exits.
A separate alarm counts collector `Exporting failed` log messages and sends
ALARM transitions to the configured SNS topic. Verify that a test trace
appears in X-Ray and that the alarm topic reaches an operator after deploying;
Terraform cannot verify either external delivery path. This setup handles
traces and ECS infrastructure metrics. Enable managed application metric
ingestion separately as described below.

## Managed application metrics

Set `managed_metrics_enabled=true` alongside a pinned `trace_collector_image`
to create an Amazon Managed Service for Prometheus (AMP) workspace. The ADOT
sidecar in each enabled ECS service then scrapes only its own task-local
`/metrics` endpoint every 15 seconds and signs remote-write requests to AMP
with SigV4. Its task role has `aps:RemoteWrite` only on this workspace; no
metric endpoint is exposed to the VPC. `managed_metrics_retention_days` defaults
to 30 days. The workspace ID and query endpoint are Terraform outputs.

Metrics carry service and environment labels plus ECS task metadata, so
multiple running tasks do not write indistinguishable series. The existing
collector export-failure alarm also covers failed AMP writes. After deployment,
query `up{service="order"}` and the relevant `*_total` counters through an
AWS-authenticated Prometheus-compatible client. AMP query access is separate
from the collector's write-only permission. Dashboards, RED latency histograms,
and application alert thresholds are still to be implemented.

## ECS availability alarms

Each enabled ECS service gets a CloudWatch alarm when its Container Insights
`RunningTaskCount` stays below one for three one-minute periods. Missing metric
data also counts as breaching, so stopped services do not silently disappear
from the signal. These alarms are created only for services with an image
digest. Set `ecs_alarm_sns_topic_arn` to an existing SNS topic ARN to send
ALARM transitions there. The topic must allow CloudWatch to publish and have
confirmed subscribers; `null` leaves the alarms visible in CloudWatch but
does not page anyone. Terraform does not create or subscribe a topic here.

## Application task roles

Terraform creates a separate task role for Order, Ledger, Matching, and Outbox
Relay. Use the `service_task_role_arns` output as the corresponding ECS task
definition's `task_role_arn`; do not substitute the shared execution role. Each
trust policy permits ECS tasks from this account and Region to assume the role.
AWS does not support narrowing the trust policy to one ECS cluster ARN.

The Ledger, Matching, and Outbox Relay roles receive separate MSK policies.
Ledger can read only `ledger.commands.v1` as the configured consumer group;
Matching can write only `matching.events.v1`; the relay can write only
`ledger.events.v1`. Order receives no MSK grant. These roles still need other
runtime permissions, including database access, before ECS deployment.

## Kafka client authentication

Ledger, Matching, and Outbox Relay default to `KAFKA_AUTH_MODE=plaintext` for
local Compose. For the IAM-authenticated MSK bootstrap brokers, set
`KAFKA_AUTH_MODE=msk_iam`, `AWS_REGION` to the cluster Region, and each
service's `*_KAFKA_BROKERS` variable to `bootstrap_brokers_sasl_iam`. The IAM
mode uses TLS certificate verification and obtains a fresh SASL/OAUTHBEARER
token from the ECS task role for each new broker connection. It fails startup
configuration validation if `AWS_REGION` is missing. Do not use the plaintext
mode against the IAM-only MSK cluster.

For the current Ledger schema, set
`OUTBOX_KAFKA_TOPIC_MAP=ledger-events=ledger.events.v1` on its dedicated relay.
This changes the Kafka destination during publication without rewriting
committed outbox rows, preserving local Compose behavior and retry semantics.
Set `LEDGER_COMMANDS_TOPIC=ledger.commands.v1` on Ledger; its local default is
still `ledger-commands`. Set `LEDGER_CONSUMER_GROUP` to the Terraform
`ledger_consumer_group` value (default `cex-ledger-service`). The relay must
only read the owning Ledger database; do not reuse its credential or topic map
for another service's outbox.

## Order ALB foundation

The public Order ALB is off by default. To create it, set
`order_alb_enabled = true` and provide an ACM certificate ARN in the same AWS
Region through `order_alb_certificate_arn`. There is no public HTTP listener.
The HTTPS listener deliberately returns `503` rather than forwarding requests:
the current Order API does not yet authenticate clients at the edge.

Terraform also creates an `ip` target group on port `8083` with `/readyz`
health checks, plus security groups that permit only ALB-to-Order HTTP traffic.
The future Order ECS service must attach `order_target_group_arn` and
`order_task_security_group_id`. Before changing the listener to forward traffic,
implement edge authentication, configure the Order service, and verify the
health checks and load-test behavior. The task group has no outbound rules of
its own; attach only the additional narrowly scoped groups needed for its
private dependencies.
