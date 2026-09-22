# Production Grafana for Praxis

This independent Terraform stack imports the existing RED dashboard, configures
an Amazon Managed Service for Prometheus (AMP) data source, and creates
Grafana-managed alert rules delivered to an email contact point. It does not
create a Grafana workspace or deploy AWS resources. Nothing is imported or
verified until an operator applies it against a reachable workspace.

Use an existing Amazon Managed Grafana v12 workspace with the Amazon Prometheus
data source plugin, or a compatible self-managed Grafana with that plugin and
SigV4 authentication. The workspace's AWS role needs `aps:QueryMetrics`,
`aps:GetMetricMetadata`, `aps:GetSeries`, and `aps:GetLabels` on the AMP
workspace ARN. Configure the workspace's outbound email capability before
relying on alerts. The workspace must have Grafana 10.4+ simplified alert
routing support for rule-level contact points. The Grafana service-account
token must have permission to manage data sources, folders, dashboards,
contact points, and alert rules.

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
export GRAFANA_URL='https://your-grafana-workspace.example'
export GRAFANA_AUTH='your-service-account-token'
terraform -chdir=infra/grafana init
terraform -chdir=infra/grafana plan \
  -var="amp_endpoint=$AMP_ENDPOINT" -var="aws_region=$AWS_REGION" \
  -var='alert_email_addresses=["alerts@example.com"]'
terraform -chdir=infra/grafana apply \
  -var="amp_endpoint=$AMP_ENDPOINT" -var="aws_region=$AWS_REGION" \
  -var='alert_email_addresses=["alerts@example.com"]'
```

Replace the example URL, token, and email. Protect this stack's Terraform
state: it contains workspace configuration and recipient addresses. The
service-account token is read from the environment and is not stored in state.

Open the `Praxis / CEX Service RED` dashboard, check that Order, Ledger, and
Matching panels render, and inspect the five Grafana alert rules. Use Grafana's
contact-point test to send a test email and confirm receipt. The existing AMP
rules are not notification sources; Grafana evaluates equivalent thresholds
and sends the email. Rule-level routing leaves any pre-existing workspace-wide
notification policy untouched. Do not declare the migration checklist's live
verification complete until these checks actually pass.
