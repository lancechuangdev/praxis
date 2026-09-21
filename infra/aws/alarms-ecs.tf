locals {
  ecs_service_alarm_names = {
    order    = { enabled = var.order_image_digest != null, name = "${local.resource_name}-order-service" }
    ledger   = { enabled = var.ledger_image_digest != null, name = "${local.resource_name}-ledger-service" }
    matching = { enabled = var.matching_image_digest != null, name = "${local.resource_name}-matching-engine" }
    outbox   = { enabled = var.outbox_image_digest != null, name = "${local.resource_name}-outbox-relay" }
  }
}

resource "aws_cloudwatch_metric_alarm" "ecs_service_unavailable" {
  for_each = { for service, config in local.ecs_service_alarm_names : service => config.name if config.enabled }

  alarm_name          = "${local.resource_name}-${each.key}-no-running-tasks"
  alarm_description   = "${each.value} has had no running ECS tasks for three minutes."
  namespace           = "ECS/ContainerInsights"
  metric_name         = "RunningTaskCount"
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 3
  datapoints_to_alarm = 3
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.this.name
    ServiceName = each.value
  }
}
