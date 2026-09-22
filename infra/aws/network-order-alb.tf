# The Kubernetes Order Service uses the fixed NodePort 30083. The managed
# node group's Auto Scaling group registers its instances as ALB targets.
resource "aws_security_group" "order_alb" {
  name_prefix = "${local.resource_name}-order-alb-"
  description = "Public HTTPS ingress for Order"
  vpc_id      = aws_vpc.cex.id

  ingress {
    description = "HTTP redirect to HTTPS"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS to Order"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Order NodePort on private EKS nodes"
    from_port   = 30083
    to_port     = 30083
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
}

resource "aws_vpc_security_group_ingress_rule" "order_from_alb" {
  security_group_id            = local.eks_cluster_security_group_id
  referenced_security_group_id = aws_security_group.order_alb.id
  description                  = "Order NodePort from public ALB"
  ip_protocol                  = "tcp"
  from_port                    = 30083
  to_port                      = 30083
}

resource "aws_lb" "order" {
  name               = "${substr(local.resource_name, 0, 26)}-order"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.order_alb.id]
  subnets            = aws_subnet.public[*].id
}

resource "aws_lb_target_group" "order" {
  name        = "${substr(local.resource_name, 0, 26)}-order"
  port        = 30083
  protocol    = "HTTP"
  target_type = "instance"
  vpc_id      = aws_vpc.cex.id

  health_check {
    enabled             = true
    path                = "/readyz"
    port                = "traffic-port"
    protocol            = "HTTP"
    healthy_threshold   = 2
    unhealthy_threshold = 2
    matcher             = "200"
  }
}

resource "aws_autoscaling_attachment" "order" {
  autoscaling_group_name = aws_eks_node_group.hot_path.resources[0].autoscaling_groups[0].name
  lb_target_group_arn    = aws_lb_target_group.order.arn
}

resource "aws_lb_listener" "order_https" {
  load_balancer_arn = aws_lb.order.arn
  port              = 443
  protocol          = "HTTPS"
  certificate_arn   = var.order_alb_certificate_arn
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.order.arn
  }
}

resource "aws_lb_listener" "order_http" {
  load_balancer_arn = aws_lb.order.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}
