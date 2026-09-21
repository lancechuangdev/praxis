data "aws_ssm_parameter" "ledger_ecs_optimized_ami" {
  count = var.ledger_ec2_enabled ? 1 : 0
  name  = "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
}

resource "aws_iam_role" "ledger_ec2_instance" {
  count              = var.ledger_ec2_enabled ? 1 : 0
  name               = "${local.resource_name}-ledger-ecs-instance"
  assume_role_policy = data.aws_iam_policy_document.ecs_instance_assume.json
}

resource "aws_iam_role_policy_attachment" "ledger_ec2_instance" {
  count      = var.ledger_ec2_enabled ? 1 : 0
  role       = aws_iam_role.ledger_ec2_instance[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "ledger_ec2" {
  count = var.ledger_ec2_enabled ? 1 : 0
  name  = "${local.resource_name}-ledger-ecs-instance"
  role  = aws_iam_role.ledger_ec2_instance[0].name
}

resource "aws_security_group" "ledger_ec2_instance" {
  count       = var.ledger_ec2_enabled ? 1 : 0
  name_prefix = "${local.resource_name}-ledger-host-"
  description = "Ledger ECS hosts; no inbound connections"
  vpc_id      = aws_vpc.cex.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_launch_template" "ledger_ec2" {
  count         = var.ledger_ec2_enabled ? 1 : 0
  name_prefix   = "${local.resource_name}-ledger-"
  image_id      = data.aws_ssm_parameter.ledger_ecs_optimized_ami[0].value
  instance_type = var.ledger_ec2_instance_type

  iam_instance_profile {
    arn = aws_iam_instance_profile.ledger_ec2[0].arn
  }

  network_interfaces {
    associate_public_ip_address = false
    security_groups             = [aws_security_group.ledger_ec2_instance[0].id]
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  user_data = base64encode("#!/bin/bash\necho 'ECS_CLUSTER=${aws_ecs_cluster.this.name}' >> /etc/ecs/ecs.config\n")

  tag_specifications {
    resource_type = "instance"
    tags          = { Name = "${local.resource_name}-ledger-ecs" }
  }
}

resource "aws_autoscaling_group" "ledger_ec2" {
  count                 = var.ledger_ec2_enabled ? 1 : 0
  name                  = "${local.resource_name}-ledger-ecs"
  min_size              = 0
  max_size              = var.ledger_ec2_max_instances
  desired_capacity      = 0
  vpc_zone_identifier   = aws_subnet.private[*].id
  protect_from_scale_in = true
  health_check_type     = "EC2"

  launch_template {
    id      = aws_launch_template.ledger_ec2[0].id
    version = aws_launch_template.ledger_ec2[0].latest_version
  }

  tag {
    key                 = "AmazonECSManaged"
    value               = ""
    propagate_at_launch = true
  }

  lifecycle {
    ignore_changes = [desired_capacity]

    precondition {
      condition     = var.ledger_image_digest != null
      error_message = "ledger_ec2_enabled requires ledger_image_digest and the existing Ledger runtime credentials."
    }

    precondition {
      condition     = var.ledger_ec2_desired_count <= var.ledger_ec2_max_instances
      error_message = "ledger_ec2_desired_count must not exceed ledger_ec2_max_instances."
    }
  }

  depends_on = [aws_iam_role_policy_attachment.ledger_ec2_instance]
}

resource "aws_ecs_capacity_provider" "ledger" {
  count = var.ledger_ec2_enabled ? 1 : 0
  name  = "${local.resource_name}-ledger-ec2"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.ledger_ec2[0].arn
    managed_termination_protection = "ENABLED"
    managed_draining               = "ENABLED"

    managed_scaling {
      status          = "ENABLED"
      target_capacity = 90
    }
  }
}

# Separate service/discovery name keeps Order on Fargate Ledger until a
# deliberate client endpoint change. Both services use the same database.
resource "aws_ecs_task_definition" "ledger_ec2" {
  count                    = var.ledger_ec2_enabled ? 1 : 0
  family                   = "${local.resource_name}-ledger-service-ec2"
  requires_compatibilities = ["EC2"]
  network_mode             = "awsvpc"
  cpu                      = aws_ecs_task_definition.ledger[0].cpu
  memory                   = aws_ecs_task_definition.ledger[0].memory
  execution_role_arn       = aws_iam_role.ledger_task_execution[0].arn
  task_role_arn            = aws_iam_role.service_task["ledger_service"].arn
  container_definitions    = aws_ecs_task_definition.ledger[0].container_definitions
}

resource "aws_ecs_service" "ledger_ec2" {
  count           = var.ledger_ec2_enabled ? 1 : 0
  name            = "${local.resource_name}-ledger-service-ec2"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.ledger_ec2[0].arn
  desired_count   = var.ledger_ec2_desired_count

  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.ledger[0].name
    weight            = 1
  }

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ledger_task.id, aws_security_group.cex_clients.id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.ledger_ec2[0].arn
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this, aws_iam_role_policy.msk_client["ledger_service"], aws_iam_role_policy.ledger_secret_execution, aws_iam_role_policy_attachment.ledger_task_execution, aws_iam_role_policy.xray_export, aws_iam_role_policy.managed_metrics_write]
}
