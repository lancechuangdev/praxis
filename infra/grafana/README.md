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
