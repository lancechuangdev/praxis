resource "aws_security_group" "order_alb" {
  count = var.order_alb_enabled ? 1 : 0

  name_prefix = "${local.resource_name}-order-alb-"
  description = "Public HTTPS entry point for Order Service"
  vpc_id      = aws_vpc.cex.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "order_task" {
  count = var.order_alb_enabled ? 1 : 0

  name_prefix = "${local.resource_name}-order-task-"
  description = "Private Order Service tasks; inbound HTTP only from the Order ALB"
  vpc_id      = aws_vpc.cex.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "order_alb_https" {
  count = var.order_alb_enabled ? 1 : 0

  security_group_id = aws_security_group.order_alb[0].id
  description       = "Public HTTPS access"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "order_alb_to_task" {
  count = var.order_alb_enabled ? 1 : 0

  security_group_id            = aws_security_group.order_alb[0].id
  referenced_security_group_id = aws_security_group.order_task[0].id
  description                  = "Order HTTP and target health checks"
  ip_protocol                  = "tcp"
  from_port                    = 8083
  to_port                      = 8083
}

resource "aws_vpc_security_group_ingress_rule" "order_task_from_alb" {
  count = var.order_alb_enabled ? 1 : 0

  security_group_id            = aws_security_group.order_task[0].id
  referenced_security_group_id = aws_security_group.order_alb[0].id
  description                  = "Order HTTP only from the public ALB"
  ip_protocol                  = "tcp"
  from_port                    = 8083
  to_port                      = 8083
}

resource "aws_lb" "order" {
  count = var.order_alb_enabled ? 1 : 0

  name                       = "${substr(local.resource_name, 0, 22)}-order"
  internal                   = false
  load_balancer_type         = "application"
  security_groups            = [aws_security_group.order_alb[0].id]
  subnets                    = aws_subnet.public[*].id
  drop_invalid_header_fields = true
  enable_deletion_protection = var.order_alb_deletion_protection
}

resource "aws_lb_target_group" "order" {
  count = var.order_alb_enabled ? 1 : 0

  name        = "${substr(local.resource_name, 0, 19)}-order-http"
  port        = 8083
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.cex.id

  health_check {
    enabled             = true
    path                = "/readyz"
    protocol            = "HTTP"
    port                = "traffic-port"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
}

resource "aws_lb_target_group" "order_ec2" {
  count       = var.order_ec2_enabled && var.order_alb_enabled ? 1 : 0
  name        = "${substr(local.resource_name, 0, 17)}-order-ec2"
  port        = 8083
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.cex.id

  health_check {
    enabled             = true
    path                = "/readyz"
    protocol            = "HTTP"
    matcher             = "200"
    interval            = 15
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
}

resource "aws_lb_listener" "order_https" {
  count = var.order_alb_enabled ? 1 : 0

  load_balancer_arn = aws_lb.order[0].arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = var.order_alb_certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  default_action {
    type = "fixed-response"

    fixed_response {
      content_type = "text/plain"
      message_body = "Order admission is not deployed"
      status_code  = "503"
    }
  }

  lifecycle {
    precondition {
      condition     = try(trimspace(var.order_alb_certificate_arn), "") != ""
      error_message = "order_alb_certificate_arn is required when order_alb_enabled is true."
    }
  }
}

# Associates the target group with this ALB so ECS can register Order tasks.
# The ALB security group intentionally has no ingress rule for port 8080.
# Public HTTPS stays on the fixed 503 response until authentication is ready.
resource "aws_lb_listener" "order_target_registration" {
  count = var.order_alb_enabled && var.order_image_digest != null ? 1 : 0

  load_balancer_arn = aws_lb.order[0].arn
  port              = 8080
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.order[0].arn
  }
}

# No public ingress on 8082. The HTTPS listener remains a fixed 503 until
# authentication is implemented and a separate, approved cutover is made.
resource "aws_lb_listener" "order_ec2_target_registration" {
  count = var.order_ec2_enabled && var.order_alb_enabled ? 1 : 0

  load_balancer_arn = aws_lb.order[0].arn
  port              = 8082
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.order_ec2[0].arn
  }
}
