data "aws_iam_policy_document" "grafana_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["grafana.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "grafana_workspace" {
  name               = "${local.resource_name}-grafana-workspace"
  description        = "Amazon Managed Grafana access to the Praxis AMP workspace"
  assume_role_policy = data.aws_iam_policy_document.grafana_assume.json
}

data "aws_iam_policy_document" "grafana_amp_query" {
  statement {
    actions = [
      "aps:QueryMetrics",
      "aps:GetMetricMetadata",
      "aps:GetSeries",
      "aps:GetLabels",
    ]
    resources = [aws_prometheus_workspace.application.arn]
  }
}

resource "aws_iam_role_policy" "grafana_amp_query" {
  name   = "amp-query"
  role   = aws_iam_role.grafana_workspace.id
  policy = data.aws_iam_policy_document.grafana_amp_query.json
}

resource "aws_grafana_workspace" "praxis" {
  name                     = "${local.resource_name}-grafana"
  description              = "Praxis application metrics and alerts"
  grafana_version          = "12.4"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "CUSTOMER_MANAGED"
  role_arn                 = aws_iam_role.grafana_workspace.arn

  depends_on = [aws_iam_role_policy.grafana_amp_query]
}

resource "aws_grafana_role_association" "admins" {
  role         = "ADMIN"
  workspace_id = aws_grafana_workspace.praxis.id
  user_ids     = var.grafana_admin_user_ids
}

# A token is created later through Grafana/AWS, never through Terraform state.
resource "aws_grafana_workspace_service_account" "terraform" {
  name         = "praxis-terraform"
  grafana_role = "ADMIN"
  workspace_id = aws_grafana_workspace.praxis.id
}
