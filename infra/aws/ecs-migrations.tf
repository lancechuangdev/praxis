resource "aws_ecs_task_definition" "outbox_migration" {
  count = var.outbox_migration_image_digest == null ? 0 : 1

  lifecycle {
    precondition {
      condition     = var.ledger_runtime_secret_arn != "" && var.outbox_runtime_secret_arn != "" && var.ledger_runtime_secret_arn != var.outbox_runtime_secret_arn
      error_message = "Set distinct ledger_runtime_secret_arn and outbox_runtime_secret_arn before creating the Outbox migration task."
    }
  }

  family                   = "${local.resource_name}-outbox-migration"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.outbox_migration_execution[0].arn

  container_definitions = jsonencode([{
    name                   = "outbox-migration"
    image                  = "${aws_ecr_repository.service["outbox_relay"].repository_url}@${var.outbox_migration_image_digest}"
    command                = ["migrate"]
    essential              = true
    readonlyRootFilesystem = true
    stopTimeout            = 30
    environment = [
      { name = "OUTBOX_DB_HOST", value = aws_rds_cluster.ledger.endpoint },
      { name = "OUTBOX_DB_USER", value = var.postgres_master_username },
      { name = "OUTBOX_DB_NAME", value = var.postgres_database_name },
      { name = "OUTBOX_BOOTSTRAP_RUNTIME_ROLES", value = "true" }
    ]
    secrets = [
      {
        name      = "OUTBOX_DB_PASSWORD"
        valueFrom = "${aws_rds_cluster.ledger.master_user_secret[0].secret_arn}:password::"
      },
      {
        name      = "LEDGER_RUNTIME_PASSWORD"
        valueFrom = "${var.ledger_runtime_secret_arn}:password::"
      },
      {
        name      = "OUTBOX_RUNTIME_PASSWORD"
        valueFrom = "${var.outbox_runtime_secret_arn}:password::"
      }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.ecs_service["outbox_relay"].name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "outbox-migration"
      }
    }
  }])

  depends_on = [aws_iam_role_policy.outbox_migration_secret, aws_iam_role_policy.outbox_migration_runtime_secrets, aws_iam_role_policy_attachment.outbox_migration_execution]
}
