locals {
  msk_topic_arn_prefix = replace(aws_msk_cluster.this.arn, ":cluster/", ":topic/")
  msk_group_arn_prefix = replace(aws_msk_cluster.this.arn, ":cluster/", ":group/")

  msk_client_access = {
    ledger_service = {
      read_topics  = [aws_msk_topic.this["ledger_commands"].name]
      write_topics = []
      groups       = [var.ledger_consumer_group]
    }
    matching_engine = {
      read_topics  = []
      write_topics = [aws_msk_topic.this["matching_events"].name]
      groups       = []
    }
    outbox_relay = {
      read_topics  = []
      write_topics = [aws_msk_topic.this["ledger_events"].name]
      groups       = []
    }
  }
}

data "aws_iam_policy_document" "msk_client" {
  for_each = local.msk_client_access

  statement {
    sid       = "Connect"
    actions   = ["kafka-cluster:Connect"]
    resources = [aws_msk_cluster.this.arn]
  }

  dynamic "statement" {
    for_each = length(each.value.read_topics) > 0 ? [1] : []
    content {
      sid       = "ReadTopics"
      actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
      resources = [for topic in each.value.read_topics : "${local.msk_topic_arn_prefix}/${topic}"]
    }
  }

  dynamic "statement" {
    for_each = length(each.value.write_topics) > 0 ? [1] : []
    content {
      sid       = "WriteTopics"
      actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:WriteData"]
      resources = [for topic in each.value.write_topics : "${local.msk_topic_arn_prefix}/${topic}"]
    }
  }

  dynamic "statement" {
    for_each = length(each.value.groups) > 0 ? [1] : []
    content {
      sid       = "ConsumeGroups"
      actions   = ["kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"]
      resources = [for group in each.value.groups : "${local.msk_group_arn_prefix}/${group}"]
    }
  }
}

resource "aws_iam_role_policy" "msk_client" {
  for_each = { outbox_relay = local.msk_client_access.outbox_relay }

  name   = "msk-client"
  role   = aws_iam_role.service_task[each.key].id
  policy = data.aws_iam_policy_document.msk_client[each.key].json
}
