data "aws_iam_policy_document" "database_migration_secret" {
  statement {
    sid       = "ReadRDSAdminSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_rds_cluster.ledger.master_user_secret[0].secret_arn]
  }

  statement {
    sid       = "DecryptRDSAdminSecret"
    actions   = ["kms:Decrypt"]
    resources = [aws_kms_key.postgres.arn]

    condition {
      test     = "StringEquals"
      variable = "kms:ViaService"
      values   = ["secretsmanager.${var.aws_region}.${data.aws_partition.current.dns_suffix}"]
    }
  }
}

resource "aws_iam_role" "outbox_migration_execution" {
  count              = var.outbox_migration_image_digest == null ? 0 : 1
  name               = "${local.resource_name}-outbox-migration-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
}

resource "aws_iam_role_policy_attachment" "outbox_migration_execution" {
  count      = var.outbox_migration_image_digest == null ? 0 : 1
  role       = aws_iam_role.outbox_migration_execution[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "outbox_migration_secret" {
  count  = var.outbox_migration_image_digest == null ? 0 : 1
  name   = "rds-admin-secret"
  role   = aws_iam_role.outbox_migration_execution[0].id
  policy = data.aws_iam_policy_document.database_migration_secret.json
}

data "aws_iam_policy_document" "outbox_migration_runtime_secrets" {
  count = var.outbox_migration_image_digest == null || var.ledger_runtime_secret_arn == "" || var.outbox_runtime_secret_arn == "" ? 0 : 1

  statement {
    sid       = "ReadRuntimePasswordsForBootstrap"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.ledger_runtime_secret_arn, var.outbox_runtime_secret_arn]
  }
}

resource "aws_iam_role_policy" "outbox_migration_runtime_secrets" {
  count  = var.outbox_migration_image_digest == null || var.ledger_runtime_secret_arn == "" || var.outbox_runtime_secret_arn == "" ? 0 : 1
  name   = "runtime-secrets-for-bootstrap"
  role   = aws_iam_role.outbox_migration_execution[0].id
  policy = data.aws_iam_policy_document.outbox_migration_runtime_secrets[0].json
}
