locals {
  eks_msk_pod_services = {
    ledger   = { service_account = "ledger", msk_policy = "ledger_service" }
    matching = { service_account = "matching", msk_policy = "matching_engine" }
  }
}

data "aws_iam_policy_document" "eks_pod_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_pod" {
  for_each = var.eks_enabled ? local.eks_msk_pod_services : {}

  name               = "${local.resource_name}-${each.key}-eks-pod"
  description        = "MSK IAM identity for the ${each.key} Kubernetes service account"
  assume_role_policy = data.aws_iam_policy_document.eks_pod_assume.json
}

resource "aws_iam_role_policy" "eks_pod_msk" {
  for_each = var.eks_enabled ? local.eks_msk_pod_services : {}

  name   = "msk-client"
  role   = aws_iam_role.eks_pod[each.key].id
  policy = data.aws_iam_policy_document.msk_client[each.value.msk_policy].json
}

resource "aws_eks_pod_identity_association" "msk_clients" {
  for_each = var.eks_enabled ? local.eks_msk_pod_services : {}

  cluster_name    = aws_eks_cluster.this[0].name
  namespace       = "praxis"
  service_account = each.value.service_account
  role_arn        = aws_iam_role.eks_pod[each.key].arn

  depends_on = [aws_eks_addon.pod_identity_agent, aws_iam_role_policy.eks_pod_msk]
}
