locals {
  order_sensitive_partition_counts_by_topic = {
    "matching.events.v1" = 96
    "ledger.commands.v1" = 24
    "ledger.events.v1"   = 24
  }
  resource_name                 = "${var.name}-${var.environment}"
  eks_cluster_security_group_id = one(aws_eks_cluster.this.vpc_config).cluster_security_group_id
}

data "aws_partition" "current" {}
