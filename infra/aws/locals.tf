locals {
  resource_name                 = "${var.name}-${var.environment}"
  eks_cluster_security_group_id = one(aws_eks_cluster.this.vpc_config).cluster_security_group_id
}

data "aws_partition" "current" {}
