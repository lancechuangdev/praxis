# Private projection deployment module

This independent, opt-in module supplies Phase 6 configuration for existing
workflow and journal streams. It has not been applied or verified in AWS/Atlas.
It does not change the existing `infra/aws` or `infra/k8s` defaults.

Inputs reference an existing Atlas project, VPC/private subnets, EKS cluster
with Pod Identity agent, MSK cluster, Ledger publisher role and an operator
role. Use an isolated Atlas project or review its existing network/access
policies; the module creates no public IP allowlist. Atlas and the endpoint must
use the same AWS region. The existing broker security group must permit IAM/TLS
from the selected application node/pod security groups. EKS must enforce the
provided NetworkPolicy. Enable `enable_projection_metrics` in `infra/k8s` to
scrape the labelled projection pods. The supplied node selector matches the
existing collector node coverage; budget projection resources or change both
placements together.

The module creates a dedicated three-node M10 replica set with backup/PIT and
termination protection, an Atlas endpoint service and AWS interface endpoint,
30-day delete-retention lifecycle topics (24 partitions, replication three,
minimum in-sync replicas two), source publication grants, and per-target
consumer identities. Treat these partition counts as frozen for these v1
streams; use versioned topics for a reviewed topology change. Do not reduce
retention below the reviewed recovery window.

A target gets exact-topic/group MSK permissions and a collection-scoped Atlas
user without delete/provision permissions. Group namespaces include an
environment/database hash; use the emitted namespace, not the input prefix,
for manual clients. Source publishers get exact write-topic grants; consumers
cannot write Kafka. If no existing Order role is supplied, the module creates
one for the `order` service account. Pod role trust binds account, cluster,
namespace and service account.

Runtime Atlas passwords are generated and stored as private-SRV URI secrets in
Secrets Manager. Pod Identity grants each target access only to its own secret.
The module creates a dedicated Secrets Manager interface endpoint and passes its
URL to the SDK through `PROJECTION_SECRETS_MANAGER_ENDPOINT`. Private DNS stays
disabled, so existing workloads retain their current VPC DNS behavior.
MongoDB uses SCRAM credentials from that secret; Kafka and secret retrieval use
AWS identity. This avoids relying on the MongoDB driver's different AWS
credential-chain support for Pod Identity. A separate operator user/secret has
readWrite/dbAdmin only for the projection database and selected cluster.
Provision collections, reconcile and select routes with the operator identity;
never give that secret to a consumer. Private SRV discovery must match the
created endpoint; missing private connection strings fail rather than falling
back to public addresses.

Use Atlas provider credentials through its supported environment/configuration
path. Database passwords and URI secret versions are sensitive Terraform state:
configure an encrypted, access-controlled, locked backend before applying. No
secret values are output or committed. Review/rotate generated users through
the secret-management workflow and roll dependent workloads; do not rotate a
live password without coordinating its users.

```bash
terraform -chdir=infra/projections init
terraform -chdir=infra/projections plan -var-file=terraform.tfvars
```

Create the real untracked tfvars file from the example and replace every
placeholder. `workload_manifests` outputs YAML for service accounts, non-root
replicated workloads, private ClusterIP services, disruption budgets and scoped
network policies. Export/review/apply each manifest in the selected environment
using the existing Kubernetes deployment workflow. Images must be digest-pinned.
Dependency CIDRs must cover private Atlas/MSK/API endpoints; public ranges are
rejected. The Pod Identity agent's link-local endpoint and cluster DNS are
explicitly allowed. Avoid unintended mesh injection; these workloads are not
part of the earlier Order/Ledger/Matching mesh enrollment.

Reads default off. When enabling them, supply issuer/audience/key ID and create
`projection-jwt-public-key` with `public.pem`, configure private TLS ingress,
then follow the reconciled route procedure. `prepare`/operator jobs must use
separate credentials and never start Kafka consumers. Source capture and
publication flags are separately configured in the Order/Ledger deployments;
applying this module does not activate them.

Offline checks are `terraform validate` and the plan-only `terraform test`,
which uses mocked AWS/Atlas/random providers and contacts no cloud APIs. Run
`make phase6-infra-verify` from the repository root. Schema validation and mock
plans do not establish live endpoint readiness, IAM, database privilege
acceptance, DNS/TLS, secret rotation, cost or cutover readiness. No cluster,
consumer, read API or gateway cutover has been deployed by this implementation.

The Atlas resource configuration follows the pinned provider's
[cluster](https://registry.terraform.io/providers/mongodb/mongodbatlas/2.8.0/docs/resources/advanced_cluster)
and [private endpoint](https://registry.terraform.io/providers/mongodb/mongodbatlas/2.8.0/docs/resources/privatelink_endpoint_service)
contracts. Pod Identity trust tags follow the
[AWS role guidance](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-role.html).
See the [Phase 6 runbook](../../docs/phase6-operations.md) for enrollment,
recovery, source exports, replay, reconciliation and rollback.
