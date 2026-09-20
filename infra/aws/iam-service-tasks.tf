data "aws_caller_identity" "current" {}

data "aws_iam_policy_document" "service_task_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        "arn:${data.aws_partition.current.partition}:ecs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      ]
    }
  }
}

resource "aws_iam_role" "service_task" {
  for_each = var.ecr_repositories

  name               = "${local.resource_name}-${each.value}-task"
  description        = "Application task identity for ${each.value}; permissions are granted per service"
  assume_role_policy = data.aws_iam_policy_document.service_task_assume_role.json
}
