resource "aws_security_group" "order_grpc_clients" {
  name_prefix = "${local.resource_name}-order-grpc-"
  description = "Identifies Order tasks allowed to call private gRPC services"
  vpc_id      = aws_vpc.cex.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "matching_task" {
  name_prefix = "${local.resource_name}-matching-task-"
  description = "Private Matching gRPC task ingress"
  vpc_id      = aws_vpc.cex.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "matching_from_order" {
  security_group_id            = aws_security_group.matching_task.id
  referenced_security_group_id = aws_security_group.order_grpc_clients.id
  description                  = "Matching gRPC from Order tasks"
  ip_protocol                  = "tcp"
  from_port                    = 9092
  to_port                      = 9092
}

resource "aws_ecs_task_definition" "matching" {
  count = var.matching_image_digest == null ? 0 : 1

  lifecycle {
    precondition {
      condition     = var.matching_fargate_desired_count + var.matching_ec2_desired_count <= 1
      error_message = "This mock Matching Engine cannot run Fargate and EC2 tasks simultaneously: set the Fargate count to zero first."
    }

    precondition {
      condition     = var.matching_ec2_desired_count == 0 || var.matching_ec2_enabled
      error_message = "matching_ec2_desired_count requires matching_ec2_enabled."
    }
  }

  family                   = "${local.resource_name}-matching-engine"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.trace_collector_image == null ? "512" : "1024"
  memory                   = var.trace_collector_image == null ? "1024" : "2048"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.service_task["matching_engine"].arn

  container_definitions = jsonencode(concat([{
    name                   = "matching-engine"
    image                  = "${aws_ecr_repository.service["matching_engine"].repository_url}@${var.matching_image_digest}"
    essential              = true
    readonlyRootFilesystem = true
    stopTimeout            = 30
    portMappings = [
      { containerPort = 9092, hostPort = 9092, protocol = "tcp" },
      { containerPort = 8084, hostPort = 8084, protocol = "tcp" }
    ]
    healthCheck = {
      command     = ["CMD", "/matching-engine", "healthcheck"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 30
    }
    environment = concat([
      { name = "AWS_REGION", value = var.aws_region },
      { name = "KAFKA_AUTH_MODE", value = "msk_iam" },
      { name = "MATCHING_GRPC_ADDRESS", value = ":9092" },
      { name = "MATCHING_HTTP_ADDRESS", value = ":8084" },
      { name = "MATCHING_KAFKA_ENABLED", value = "true" },
      { name = "MATCHING_KAFKA_BROKERS", value = aws_msk_cluster.this.bootstrap_brokers_sasl_iam },
      { name = "MATCHING_KAFKA_TOPIC", value = aws_msk_topic.this["matching_events"].name }
    ], local.trace_application_environment)
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.ecs_service["matching_engine"].name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "matching-engine"
      }
    }
  }], lookup(local.trace_collector_containers, "matching", [])))

}

resource "aws_ecs_service" "matching" {
  count = var.matching_image_digest == null ? 0 : 1

  name            = "${local.resource_name}-matching-engine"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.matching[0].arn
  desired_count   = var.matching_fargate_desired_count

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
    security_groups  = [aws_security_group.matching_task.id, aws_security_group.cex_clients.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.grpc["matching_engine"].arn
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this, aws_iam_role_policy.msk_client["matching_engine"], aws_iam_role_policy.xray_export, aws_iam_role_policy.managed_metrics_write]
}
