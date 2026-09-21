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
  for_each = local.eks_msk_pod_services

  name               = "${local.resource_name}-${each.key}-eks-pod"
  description        = "MSK IAM identity for the ${each.key} Kubernetes service account"
  assume_role_policy = data.aws_iam_policy_document.eks_pod_assume.json
}

resource "aws_iam_role_policy" "eks_pod_msk" {
  for_each = local.eks_msk_pod_services

  name   = "msk-client"
  role   = aws_iam_role.eks_pod[each.key].id
  policy = data.aws_iam_policy_document.msk_client[each.value.msk_policy].json
}

data "aws_iam_policy_document" "eks_ledger_runtime_secret" {
  count = var.ledger_runtime_secret_arn == "" ? 0 : 1

  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.ledger_runtime_secret_arn]
  }
}

resource "aws_iam_role_policy" "eks_ledger_runtime_secret" {
  count  = var.ledger_runtime_secret_arn == "" ? 0 : 1
  name   = "ledger-runtime-secret"
  role   = aws_iam_role.eks_pod["ledger"].id
  policy = data.aws_iam_policy_document.eks_ledger_runtime_secret[0].json
}

resource "aws_iam_role" "eks_ledger_migration" {
  name               = "${local.resource_name}-ledger-migration-eks-pod"
  description        = "RDS admin secret identity for the one-off Ledger Kubernetes migration Job"
  assume_role_policy = data.aws_iam_policy_document.eks_pod_assume.json
}

resource "aws_iam_role_policy" "eks_ledger_migration_secret" {
  name   = "rds-admin-secret"
  role   = aws_iam_role.eks_ledger_migration.id
  policy = data.aws_iam_policy_document.database_migration_secret.json
}

resource "aws_eks_pod_identity_association" "ledger_migration" {
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "praxis"
  service_account = "ledger-migration"
  role_arn        = aws_iam_role.eks_ledger_migration.arn

  depends_on = [aws_eks_addon.pod_identity_agent, aws_iam_role_policy.eks_ledger_migration_secret]
}

resource "aws_eks_pod_identity_association" "msk_clients" {
  for_each = local.eks_msk_pod_services

  cluster_name    = aws_eks_cluster.this.name
  namespace       = "praxis"
  service_account = each.value.service_account
  role_arn        = aws_iam_role.eks_pod[each.key].arn

  depends_on = [aws_eks_addon.pod_identity_agent, aws_iam_role_policy.eks_pod_msk]
}
