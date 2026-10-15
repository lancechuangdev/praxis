# Production Grafana for Praxis

This independent Terraform stack imports the existing RED dashboard, configures
an Amazon Managed Service for Prometheus (AMP) data source, and creates
Grafana-managed alert rules delivered to an email contact point. The
[`infra/aws`](../aws/README.md) stack creates the Amazon Managed Grafana
workspace, its AMP query role, Identity Center admin assignment, and a
`praxis-terraform` service account. This stack configures that workspace via
its Grafana API. Nothing is imported or verified until an operator applies it.

The AWS stack pins Grafana 12.4, uses IAM Identity Center for sign-in, and
grants its role `aps:QueryMetrics`, `aps:GetMetricMetadata`, `aps:GetSeries`,
and `aps:GetLabels` on the AMP workspace. The Amazon Prometheus data source
plugin and Grafana's email delivery must be available in the workspace.
After AWS apply, sign in as an assigned admin and create a short-lived token
for the `praxis-terraform` service account. Keep the token outside Terraform
state; it has workspace-admin privileges. Rotate or revoke it after use.

The stack also imports `Praxis / Praxis Mesh Acceptance` from
[`praxis-mesh-acceptance.json`](../../observability/grafana/dashboards/praxis-mesh-acceptance.json).
This dashboard uses the existing AMP data source and the collector's curated
metric labels. Select the target environment before capturing evidence. It
shows Order RPC rate and traffic share by backend pod, transport security,
HTTP and gRPC outcomes, mesh p95 duration, proxy scrape health, and independent
application rate and latency panels. Destination-side reports avoid counting
the same RPC at both caller and backend. See
[Istio's standard metric definitions](https://istio.io/latest/docs/reference/config/metrics/).

The dashboard is expected to have no mesh data until the optional mesh is
enrolled and telemetry arrives. It creates no paging alerts and changes no
traffic policy. Empty panels are missing evidence, never a successful check.
For Phase 5 acceptance:

1. Run `make phase5-verify` and retain the ready backend pod inventory.
2. Generate representative authenticated order traffic for at least two
   five-minute rate windows; select the corresponding dashboard interval.
3. Compare every ready Ledger and Matching pod with the RPC-rate and share
   panels. Pods without a metric series are absent from the chart, not known
   to have received traffic. Repeat after scale-out and during rolling drain;
   retain timestamped endpoint inventories and rollout events with each view.
4. Verify that observed Order RPCs report `mutual_tls`. Run the separate
   unauthorized-identity exercise and inspect denial logs; this panel filters
   to Order traffic and cannot prove that other identities are denied.
5. Compare application rates and p95 duration with the pre-enrollment baseline.
   Application operations and mesh RPCs cover different work and need not have
   equal counts or latency. Inspect gRPC status as well as HTTP status because
   gRPC failures can arrive with HTTP 200.

Queries use five-minute rates, so drain and scale-out appear with a lag. Proxy
scrape health does not establish backend readiness. The dashboard does not
measure zone-level capacity, retry budgets, active requests, ejections, or
proxy resource overhead; those remain separate acceptance evidence. It can
also be loaded by the local Grafana file provisioner, but the default local
Compose topology does not export Kubernetes mesh metrics. Roll back this
dashboard independently by restoring its prior JSON and applying this stack;
workload and collector configuration remain separate.

First provision the AWS and Kubernetes stacks and deploy real workloads. Then
run the read-only ingestion check from the repository root:

```bash
export AMP_ENDPOINT="$(terraform -chdir=infra/aws output -raw managed_metrics_query_endpoint)"
export AWS_REGION="$(terraform -chdir=infra/aws output -json eks_runtime_config | jq -r .aws_region)"
bash infra/grafana/verify-ingestion.sh
```

The check requires `awscurl`, `jq`, and AWS credentials with `aps:QueryMetrics`.
It asserts healthy scrape targets and the presence of the three hot-path
counters; it cannot prove request traffic, trace delivery, Outbox ingestion,
or email delivery. Generate traffic and inspect the dashboard rates separately.
If Outbox metrics are desired, enable its optional ECS collector before using
the Outbox alert. The Grafana rule's NoData state is not a paging alert.

After ingestion passes, apply this stack from a machine that can reach the
Grafana API. Use a monitored mailbox; do not put the Grafana token in a tfvars
file or a shell command recorded in history:

```bash
export GRAFANA_URL="$(terraform -chdir=infra/aws output -raw grafana_workspace_url)"
export GRAFANA_AUTH='your-short-lived-service-account-token'
terraform -chdir=infra/grafana init
terraform -chdir=infra/grafana plan \
  -var="amp_endpoint=$AMP_ENDPOINT" -var="aws_region=$AWS_REGION" \
  -var='alert_email_addresses=["alerts@example.com"]'
terraform -chdir=infra/grafana apply \
  -var="amp_endpoint=$AMP_ENDPOINT" -var="aws_region=$AWS_REGION" \
  -var='alert_email_addresses=["alerts@example.com"]'
```

Replace the example token and email. Protect this stack's Terraform
state: it contains workspace configuration and recipient addresses. The
service-account token is read from the environment and is not stored in state.

Open the `Praxis / CEX Service RED` dashboard, check that Order, Ledger, and
Matching panels render, and inspect the five Grafana alert rules. Use Grafana's
contact-point test to send a test email and confirm receipt. The existing AMP
rules are not notification sources; Grafana evaluates equivalent thresholds
and sends the email. Rule-level routing leaves any pre-existing workspace-wide
notification policy untouched. Do not declare the migration checklist's live
verification complete until these checks actually pass.
