resource "aws_vpc_security_group_egress_rule" "order_to_ledger" {
  security_group_id            = aws_security_group.order_grpc_clients.id
  referenced_security_group_id = aws_security_group.ledger_task.id
  description                  = "Order gRPC requests to Ledger"
  ip_protocol                  = "tcp"
  from_port                    = 9091
  to_port                      = 9091
}

resource "aws_vpc_security_group_egress_rule" "order_to_matching" {
  security_group_id            = aws_security_group.order_grpc_clients.id
  referenced_security_group_id = aws_security_group.matching_task.id
  description                  = "Order gRPC requests to Matching"
  ip_protocol                  = "tcp"
  from_port                    = 9092
  to_port                      = 9092
}

resource "aws_vpc_security_group_egress_rule" "order_to_aws_apis" {
  security_group_id = aws_security_group.order_grpc_clients.id
  description       = "HTTPS for ECR image pulls and CloudWatch logs through NAT"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_ecs_task_definition" "order" {
  count = var.order_image_digest == null ? 0 : 1

  family                   = "${local.resource_name}-order-service"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.service_task["order_service"].arn

  container_definitions = jsonencode([{
    name                   = "order-service"
    image                  = "${aws_ecr_repository.service["order_service"].repository_url}@${var.order_image_digest}"
    essential              = true
    readonlyRootFilesystem = true
    stopTimeout            = 30
    portMappings           = [{ containerPort = 8083, hostPort = 8083, protocol = "tcp" }]
    healthCheck = {
      command     = ["CMD", "/order-service", "healthcheck"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 60
    }
    environment = [
      { name = "ORDER_HTTP_ADDRESS", value = ":8083" },
      { name = "ORDER_LEDGER_MODE", value = "grpc" },
      { name = "ORDER_LEDGER_GRPC_ADDRESS", value = "${local.grpc_services["ledger_service"].name}.${aws_service_discovery_private_dns_namespace.services.name}:${local.grpc_services["ledger_service"].port}" },
      { name = "ORDER_MATCHING_MODE", value = "grpc" },
      { name = "ORDER_MATCHING_GRPC_ADDRESS", value = "${local.grpc_services["matching_engine"].name}.${aws_service_discovery_private_dns_namespace.services.name}:${local.grpc_services["matching_engine"].port}" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.ecs_service["order_service"].name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "order-service"
      }
    }
  }])

  lifecycle {
    precondition {
      condition     = var.ledger_image_digest != null && var.matching_image_digest != null
      error_message = "Order requires ledger_image_digest and matching_image_digest so its gRPC readiness dependencies can run."
    }
  }
}

resource "aws_ecs_service" "order" {
  count = var.order_image_digest == null ? 0 : 1

  name            = "${local.resource_name}-order-service"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.order[0].arn
  desired_count   = 1

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
    security_groups  = concat([aws_security_group.order_grpc_clients.id], var.order_alb_enabled ? [aws_security_group.order_task[0].id] : [])
    assign_public_ip = false
  }

  dynamic "load_balancer" {
    for_each = var.order_alb_enabled ? [1] : []

    content {
      target_group_arn = aws_lb_target_group.order[0].arn
      container_name   = "order-service"
      container_port   = 8083
    }
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this, aws_ecs_service.ledger, aws_ecs_service.matching, aws_lb_listener.order_target_registration]
}
