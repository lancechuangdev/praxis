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
  }
}
