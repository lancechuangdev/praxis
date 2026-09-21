resource "aws_security_group" "ledger_task" {
  name_prefix = "${local.resource_name}-ledger-task-"
  description = "Private Ledger gRPC task ingress"
  vpc_id      = aws_vpc.cex.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "ledger_from_order" {
  security_group_id            = aws_security_group.ledger_task.id
  referenced_security_group_id = aws_security_group.order_grpc_clients.id
  description                  = "Ledger gRPC from Order tasks"
  ip_protocol                  = "tcp"
  from_port                    = 9091
  to_port                      = 9091
}

resource "aws_iam_role" "ledger_task_execution" {
  count = var.ledger_image_digest == null && var.ledger_migration_image_digest == null ? 0 : 1

  name               = "${local.resource_name}-ledger-execution"
  description        = "Ledger ECS image, logs, and database secret retrieval"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
}

resource "aws_iam_role_policy_attachment" "ledger_task_execution" {
  count = var.ledger_image_digest == null && var.ledger_migration_image_digest == null ? 0 : 1

  role       = aws_iam_role.ledger_task_execution[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Ledger and Outbox use the same RDS database secret, but separate execution roles.
data "aws_iam_policy_document" "database_secret_execution" {
  statement {
    sid       = "ReadLedgerDatabaseSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_rds_cluster.ledger.master_user_secret[0].secret_arn]
  }

  statement {
    sid       = "DecryptLedgerDatabaseSecret"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.postgres.arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["secretsmanager.${var.aws_region}.${data.aws_partition.current.dns_suffix}"]
    }
  }
}

resource "aws_iam_role_policy" "ledger_secret_execution" {
  count = var.ledger_image_digest == null && var.ledger_migration_image_digest == null ? 0 : 1

  name   = "ledger-database-secret"
  role   = aws_iam_role.ledger_task_execution[0].id
  policy = data.aws_iam_policy_document.database_secret_execution.json
}

resource "aws_ecs_task_definition" "ledger" {
  count = var.ledger_image_digest == null ? 0 : 1

  family                   = "${local.resource_name}-ledger-service"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ledger_task_execution[0].arn
  task_role_arn            = aws_iam_role.service_task["ledger_service"].arn

  container_definitions = jsonencode([{
    name                   = "ledger-service"
    image                  = "${aws_ecr_repository.service["ledger_service"].repository_url}@${var.ledger_image_digest}"
    essential              = true
    readonlyRootFilesystem = true
    stopTimeout            = 30
    portMappings = [
      { containerPort = 9091, hostPort = 9091, protocol = "tcp" },
      { containerPort = 8081, hostPort = 8081, protocol = "tcp" }
    ]
    healthCheck = {
      command     = ["CMD", "/ledger-service", "healthcheck"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
    environment = [
      { name = "AWS_REGION", value = var.aws_region },
      { name = "KAFKA_AUTH_MODE", value = "msk_iam" },
      { name = "LEDGER_HTTP_ADDRESS", value = ":8081" },
      { name = "LEDGER_GRPC_ADDRESS", value = ":9091" },
      { name = "LEDGER_DB_HOST", value = aws_rds_cluster.ledger.endpoint },
      { name = "LEDGER_DB_USER", value = var.postgres_master_username },
      { name = "LEDGER_DB_NAME", value = var.postgres_database_name },
      { name = "LEDGER_MIGRATE_ON_STARTUP", value = "false" },
      { name = "LEDGER_KAFKA_BROKERS", value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam },
      { name = "LEDGER_COMMANDS_TOPIC", value = aws_msk_topic.this["ledger_commands"].name },
      { name = "LEDGER_CONSUMER_GROUP", value = var.ledger_consumer_group }
    ]
    secrets = [{
      name      = "LEDGER_DB_PASSWORD"
      valueFrom = "${aws_rds_cluster.ledger.master_user_secret[0].secret_arn}:password::"
    }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.ecs_service["ledger_service"].name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "ledger-service"
      }
    }
  }])
}

resource "aws_ecs_service" "ledger" {
  count = var.ledger_image_digest == null ? 0 : 1

  name             = "${local.resource_name}-ledger-service"
  cluster          = aws_ecs_cluster.this.id
  task_definition  = aws_ecs_task_definition.ledger[0].arn
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
    security_groups  = [aws_security_group.ledger_task.id, aws_security_group.cex_clients.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.grpc["ledger_service"].arn
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this, aws_iam_role_policy.msk_client["ledger_service"], aws_iam_role_policy.ledger_secret_execution, aws_iam_role_policy_attachment.ledger_task_execution]
}
