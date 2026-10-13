resource "aws_msk_topic" "this" {
  for_each = var.topics

  name               = each.value.name
  cluster_arn        = aws_msk_cluster.this.arn
  partition_count    = each.value.partitions
  replication_factor = each.value.replication_factor
  configs            = jsonencode(each.value.configs)

  lifecycle {
    precondition {
      condition     = each.value.replication_factor <= var.broker_count
      error_message = "Topic replication_factor cannot exceed broker_count."
    }
    precondition {
      condition     = var.allow_order_sensitive_partition_change || each.value.partitions == lookup(local.order_sensitive_partition_counts_by_topic, each.value.name, each.value.partitions)
      error_message = "Order-sensitive topic partition counts are frozen. Use a versioned-topic migration; set allow_order_sensitive_partition_change only for an approved, rehearsed migration."
    }
  }
}
