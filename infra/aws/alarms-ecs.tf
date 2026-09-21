locals {
  ecs_service_alarm_names = {
    order    = { enabled = var.order_image_digest != null && var.order_fargate_desired_count > 0, name = "${local.resource_name}-order-service" }
    ledger   = { enabled = var.ledger_image_digest != null && var.ledger_fargate_desired_count > 0, name = "${local.resource_name}-ledger-service" }
    matching = { enabled = var.matching_image_digest != null && var.matching_fargate_desired_count > 0, name = "${local.resource_name}-matching-engine" }
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

resource "aws_cloudwatch_metric_alarm" "ledger_consumer_lag" {
  count = var.ledger_image_digest == null ? 0 : 1

  alarm_name          = "${local.resource_name}-ledger-consumer-offset-lag"
  alarm_description   = "Ledger command consumption is more than ${var.ledger_consumer_max_offset_lag} records behind for three of five minutes."
  namespace           = "AWS/Kafka"
  metric_name         = "MaxOffsetLag"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  datapoints_to_alarm = 3
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.ledger_consumer_max_offset_lag
  treat_missing_data  = "missing"

  dimensions = {
    "Cluster Name"   = aws_msk_cluster.this.cluster_name
    "Consumer Group" = var.ledger_consumer_group
    Topic            = aws_msk_topic.this["ledger_commands"].name
  }
}

resource "aws_cloudwatch_metric_alarm" "order_ec2_unavailable" {
  count = var.order_ec2_enabled && var.order_ec2_desired_count > 0 ? 1 : 0

  alarm_name          = "${local.resource_name}-order-ec2-no-running-tasks"
  alarm_description   = "Parallel Order EC2 service has had no running tasks for three minutes."
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
    ServiceName = aws_ecs_service.order_ec2[0].name
  }
}

resource "aws_cloudwatch_metric_alarm" "ledger_ec2_unavailable" {
  count = var.ledger_ec2_enabled && var.ledger_ec2_desired_count > 0 ? 1 : 0

  alarm_name          = "${local.resource_name}-ledger-ec2-no-running-tasks"
  alarm_description   = "Parallel Ledger EC2 service has had no running tasks for three minutes."
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
    ServiceName = aws_ecs_service.ledger_ec2[0].name
  }
}

resource "aws_cloudwatch_metric_alarm" "matching_ec2_unavailable" {
  count = var.matching_ec2_enabled && var.matching_ec2_desired_count > 0 ? 1 : 0

  alarm_name          = "${local.resource_name}-matching-ec2-no-running-tasks"
  alarm_description   = "Parallel Matching EC2 service has had no running tasks for three minutes."
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
    ServiceName = aws_ecs_service.matching_ec2[0].name
  }
}
