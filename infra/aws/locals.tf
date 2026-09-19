locals {
  resource_name = "${var.name}-${var.environment}"
}

data "aws_partition" "current" {}
