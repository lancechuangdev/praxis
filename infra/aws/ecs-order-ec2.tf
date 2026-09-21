data "aws_ssm_parameter" "ecs_optimized_ami" {
  count = var.order_ec2_enabled ? 1 : 0
  name  = "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
}

data "aws_iam_policy_document" "ecs_instance_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "order_ec2_instance" {
  count              = var.order_ec2_enabled ? 1 : 0
  name               = "${local.resource_name}-order-ecs-instance"
  assume_role_policy = data.aws_iam_policy_document.ecs_instance_assume.json
}

resource "aws_iam_role_policy_attachment" "order_ec2_instance" {
  count      = var.order_ec2_enabled ? 1 : 0
  role       = aws_iam_role.order_ec2_instance[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "order_ec2" {
  count = var.order_ec2_enabled ? 1 : 0
  name  = "${local.resource_name}-order-ecs-instance"
  role  = aws_iam_role.order_ec2_instance[0].name
}

resource "aws_security_group" "order_ec2_instance" {
  count       = var.order_ec2_enabled ? 1 : 0
  name_prefix = "${local.resource_name}-order-host-"
  description = "Order ECS hosts; no inbound connections"
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

resource "aws_launch_template" "order_ec2" {
  count         = var.order_ec2_enabled ? 1 : 0
  name_prefix   = "${local.resource_name}-order-"
  image_id      = data.aws_ssm_parameter.ecs_optimized_ami[0].value
  instance_type = var.order_ec2_instance_type

  iam_instance_profile {
    arn = aws_iam_instance_profile.order_ec2[0].arn
  }

  network_interfaces {
    associate_public_ip_address = false
    security_groups             = [aws_security_group.order_ec2_instance[0].id]
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  user_data = base64encode("#!/bin/bash\necho 'ECS_CLUSTER=${aws_ecs_cluster.this.name}' >> /etc/ecs/ecs.config\n")

  tag_specifications {
    resource_type = "instance"
    tags          = { Name = "${local.resource_name}-order-ecs" }
  }
}

resource "aws_autoscaling_group" "order_ec2" {
  count                 = var.order_ec2_enabled ? 1 : 0
  name                  = "${local.resource_name}-order-ecs"
  min_size              = 0
  max_size              = var.order_ec2_max_instances
  desired_capacity      = 0
  vpc_zone_identifier   = aws_subnet.private[*].id
  protect_from_scale_in = true
  health_check_type     = "EC2"

  launch_template {
    id      = aws_launch_template.order_ec2[0].id
    version = aws_launch_template.order_ec2[0].latest_version
  }

  tag {
    key                 = "AmazonECSManaged"
    value               = ""
    propagate_at_launch = true
  }

  lifecycle {
    ignore_changes = [desired_capacity]
    precondition {
      condition     = var.order_image_digest != null
      error_message = "order_ec2_enabled requires order_image_digest."
    }
  }

  depends_on = [aws_iam_role_policy_attachment.order_ec2_instance]
}

resource "aws_ecs_capacity_provider" "order" {
  count = var.order_ec2_enabled ? 1 : 0
  name  = "${local.resource_name}-order-ec2"

  auto_scaling_group_provider {
    auto_scaling_group_arn         = aws_autoscaling_group.order_ec2[0].arn
    managed_termination_protection = "ENABLED"
    managed_draining               = "ENABLED"

    managed_scaling {
      status          = "ENABLED"
      target_capacity = 90
    }
  }
}

# An ECS service cannot change from a Fargate to an EC2 capacity provider.
# Run a separate service and retain the original Fargate service for rollback.
resource "aws_ecs_task_definition" "order_ec2" {
  count                    = var.order_ec2_enabled ? 1 : 0
  family                   = "${local.resource_name}-order-service-ec2"
  requires_compatibilities = ["EC2"]
  network_mode             = "awsvpc"
  cpu                      = aws_ecs_task_definition.order[0].cpu
  memory                   = aws_ecs_task_definition.order[0].memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.service_task["order_service"].arn
  container_definitions    = aws_ecs_task_definition.order[0].container_definitions
}

resource "aws_ecs_service" "order_ec2" {
  count           = var.order_ec2_enabled ? 1 : 0
  name            = "${local.resource_name}-order-service-ec2"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.order_ec2[0].arn
  desired_count   = var.order_ec2_desired_count

  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  capacity_provider_strategy {
    capacity_provider = aws_ecs_capacity_provider.order[0].name
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
      target_group_arn = aws_lb_target_group.order_ec2[0].arn
      container_name   = "order-service"
      container_port   = 8083
    }
  }

  depends_on = [aws_ecs_cluster_capacity_providers.this, aws_lb_listener.order_ec2_target_registration, aws_iam_role_policy_attachment.ecs_task_execution]
}
