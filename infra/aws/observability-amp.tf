resource "aws_prometheus_workspace" "application" {
  alias = "${local.resource_name}-application"
}

resource "aws_prometheus_workspace_configuration" "application" {
  workspace_id             = aws_prometheus_workspace.application.id
  retention_period_in_days = var.managed_metrics_retention_days
}

data "aws_iam_policy_document" "managed_metrics_write" {
  statement {
    sid       = "WriteApplicationMetrics"
    actions   = ["aps:RemoteWrite"]
    resources = [aws_prometheus_workspace.application.arn]
  }
}

resource "aws_iam_role_policy" "managed_metrics_write" {
  for_each = local.trace_enabled_services

  name   = "managed-prometheus-remote-write"
  role   = aws_iam_role.service_task[local.trace_service_task_role_keys[each.key]].id
  policy = data.aws_iam_policy_document.managed_metrics_write.json
}
