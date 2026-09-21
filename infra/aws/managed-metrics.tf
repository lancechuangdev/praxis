resource "aws_prometheus_workspace" "application" {
  count = var.managed_metrics_enabled ? 1 : 0

  alias = "${local.resource_name}-application"

  lifecycle {
    precondition {
      condition     = var.trace_collector_image != null
      error_message = "Set trace_collector_image before enabling managed metrics; ECS sidecars scrape and export the metrics."
    }
  }
}

resource "aws_prometheus_workspace_configuration" "application" {
  count = var.managed_metrics_enabled ? 1 : 0

  workspace_id             = aws_prometheus_workspace.application[0].id
  retention_period_in_days = var.managed_metrics_retention_days
}

data "aws_iam_policy_document" "managed_metrics_write" {
  count = var.managed_metrics_enabled ? 1 : 0

  statement {
    sid       = "WriteApplicationMetrics"
    actions   = ["aps:RemoteWrite"]
    resources = [aws_prometheus_workspace.application[0].arn]
  }
}

resource "aws_iam_role_policy" "managed_metrics_write" {
  for_each = var.managed_metrics_enabled ? local.trace_enabled_services : {}

  name   = "managed-prometheus-remote-write"
  role   = aws_iam_role.service_task[local.trace_service_task_role_keys[each.key]].id
  policy = data.aws_iam_policy_document.managed_metrics_write[0].json
}
