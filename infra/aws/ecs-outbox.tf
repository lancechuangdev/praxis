resource "aws_iam_role" "outbox_task_execution" {
  count = var.outbox_image_digest == null ? 0 : 1

  name               = "${local.resource_name}-outbox-execution"
  description        = "Outbox ECS image, logs, and database secret retrieval"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
}

resource "aws_iam_role_policy_attachment" "outbox_task_execution" {
  count = var.outbox_image_digest == null ? 0 : 1

  role       = aws_iam_role.outbox_task_execution[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "outbox_secret_execution" {
  count = var.outbox_image_digest == null ? 0 : 1

  name   = "outbox-database-secret"
  role   = aws_iam_role.outbox_task_execution[0].id
  policy = data.aws_iam_policy_document.outbox_runtime_secret_execution[0].json
}

data "aws_iam_policy_document" "outbox_runtime_secret_execution" {
  count = var.outbox_image_digest == null ? 0 : 1
  statement {
    sid       = "ReadOutboxRuntimeSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.outbox_runtime_secret_arn]
  }
}

resource "aws_ecs_task_definition" "outbox" {
  count = var.outbox_image_digest == null ? 0 : 1

  lifecycle {
    precondition {
      condition     = var.outbox_runtime_secret_arn != ""
      error_message = "Set outbox_runtime_secret_arn and provision the restricted PostgreSQL role before deploying Outbox."
    }
    precondition {
      condition     = var.trace_collector_image == null || var.ecs_alarm_sns_topic_arn != null
      error_message = "Set ecs_alarm_sns_topic_arn before enabling the ECS trace collector."
    }
  }

  family                   = "${local.resource_name}-outbox-relay"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.trace_collector_image == null ? "512" : "1024"
  memory                   = var.trace_collector_image == null ? "1024" : "2048"
  execution_role_arn       = aws_iam_role.outbox_task_execution[0].arn
  task_role_arn            = aws_iam_role.service_task["outbox_relay"].arn

  container_definitions = jsonencode(concat([{
    name                   = "outbox-relay"
    image                  = "${aws_ecr_repository.service["outbox_relay"].repository_url}@${var.outbox_image_digest}"
    essential              = true
    readonlyRootFilesystem = true
    stopTimeout            = 30
    portMappings           = [{ containerPort = 8082, hostPort = 8082, protocol = "tcp" }]
    healthCheck = {
      command     = ["CMD", "/outbox-relay", "healthcheck"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
    environment = concat([
      { name = "AWS_REGION", value = var.aws_region },
      { name = "KAFKA_AUTH_MODE", value = "msk_iam" },
      { name = "OUTBOX_HTTP_ADDRESS", value = ":8082" },
      { name = "OUTBOX_DB_HOST", value = aws_rds_cluster.ledger.endpoint },
      { name = "OUTBOX_DB_USER", value = "outbox_runtime" },
      { name = "OUTBOX_DB_NAME", value = var.postgres_database_name },
      { name = "OUTBOX_MIGRATE_ON_STARTUP", value = "false" },
      { name = "OUTBOX_KAFKA_BROKERS", value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam },
      { name = "OUTBOX_KAFKA_TOPIC_MAP", value = "ledger-events=${aws_msk_topic.this["ledger_events"].name}" }
    ], local.trace_application_environment)
    secrets = [{
      name      = "OUTBOX_DB_PASSWORD"
      valueFrom = "${var.outbox_runtime_secret_arn}:password::"
    }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.ecs_service["outbox_relay"].name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "outbox-relay"
      }
    }
  }], lookup(local.trace_collector_containers, "outbox", [])))
}

resource "aws_ecs_service" "outbox" {
  count = var.outbox_image_digest == null ? 0 : 1

  name             = "${local.resource_name}-outbox-relay"
  cluster          = aws_ecs_cluster.this.id
  task_definition  = aws_ecs_task_definition.outbox[0].arn
  desired_count    = 1
  platform_version = "1.4.0"

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.cex_clients.id]
    assign_public_ip = false
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this, aws_iam_role_policy.msk_client["outbox_relay"], aws_iam_role_policy.outbox_secret_execution, aws_iam_role_policy_attachment.outbox_task_execution, aws_iam_role_policy.xray_export]
}
