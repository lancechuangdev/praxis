locals {
  trace_service_task_role_keys = {
    order    = "order_service"
    ledger   = "ledger_service"
    matching = "matching_engine"
    outbox   = "outbox_relay"
  }

  trace_enabled_services = var.trace_collector_image == null ? {} : {
    for key, config in local.ecs_service_alarm_names : key => config if config.enabled
  }

  trace_collector_config = <<-YAML
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 127.0.0.1:4317
    processors:
      memory_limiter:
        check_interval: 5s
        limit_mib: 128
        spike_limit_mib: 32
      resourcedetection:
        detectors: [env, ecs]
        timeout: 5s
        override: false
      batch:
        timeout: 5s
        send_batch_size: 128
    exporters:
      awsxray:
        region: ${var.aws_region}
    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [memory_limiter, resourcedetection, batch]
          exporters: [awsxray]
  YAML

  trace_application_environment = var.trace_collector_image == null ? [] : [
    { name = "OTEL_TRACES_ENABLED", value = "true" },
    { name = "OTEL_EXPORTER_OTLP_ENDPOINT", value = "http://127.0.0.1:4317" },
    { name = "OTEL_EXPORTER_OTLP_INSECURE", value = "true" },
    { name = "OTEL_TRACE_SAMPLE_RATIO", value = tostring(var.trace_sample_ratio) },
    { name = "OTEL_RESOURCE_ATTRIBUTES", value = "deployment.environment=${var.environment},service.namespace=${var.name}" }
  ]

  trace_collector_containers = {
    for key, config in local.ecs_service_alarm_names : key => [
      {
        name        = "adot-collector"
        image       = var.trace_collector_image
        essential   = true
        stopTimeout = 30
        memory      = 256
        environment = [
          { name = "AWS_REGION", value = var.aws_region },
          { name = "AOT_CONFIG_CONTENT", value = local.trace_collector_config }
        ]
        logConfiguration = {
          logDriver = "awslogs"
          options = {
            awslogs-group         = aws_cloudwatch_log_group.trace_collector[key].name
            awslogs-region        = var.aws_region
            awslogs-stream-prefix = "adot-collector"
          }
        }
      }
    ] if var.trace_collector_image != null && config.enabled
  }
}

resource "aws_cloudwatch_log_group" "trace_collector" {
  for_each          = local.trace_enabled_services
  name              = "/ecs/${local.resource_name}/${each.key}-adot-collector"
  retention_in_days = var.ecs_log_retention_days
}

data "aws_iam_policy_document" "xray_export" {
  statement {
    sid       = "ExportTracesToXRay"
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "xray_export" {
  for_each = local.trace_enabled_services

  name   = "xray-trace-export"
  role   = aws_iam_role.service_task[local.trace_service_task_role_keys[each.key]].id
  policy = data.aws_iam_policy_document.xray_export.json
}

resource "aws_cloudwatch_log_metric_filter" "trace_export_failure" {
  for_each = local.trace_enabled_services

  name           = "${local.resource_name}-${each.key}-trace-export-failure"
  log_group_name = aws_cloudwatch_log_group.trace_collector[each.key].name
  pattern        = "\"Exporting failed\""

  metric_transformation {
    name      = "${local.resource_name}-${each.key}-TraceExportFailures"
    namespace = "Praxis/Telemetry"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "trace_export_failure" {
  for_each = local.trace_enabled_services

  alarm_name          = "${local.resource_name}-${each.key}-trace-export-failure"
  alarm_description   = "The ${each.key} ADOT collector could not export traces to X-Ray."
  namespace           = "Praxis/Telemetry"
  metric_name         = aws_cloudwatch_log_metric_filter.trace_export_failure[each.key].metric_transformation[0].name
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = var.ecs_alarm_sns_topic_arn == null ? [] : [var.ecs_alarm_sns_topic_arn]
}
