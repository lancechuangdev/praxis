resource "aws_db_subnet_group" "ledger" {
  name       = "${local.resource_name}-ledger"
  subnet_ids = aws_subnet.private[*].id

  tags = {
    Name = "${local.resource_name}-ledger"
  }
}

resource "aws_security_group" "postgres" {
  name_prefix = "${local.resource_name}-postgres-"
  description = "PostgreSQL access from CEX application services"
  vpc_id      = aws_vpc.cex.id

  egress {
    description = "Allow PostgreSQL cluster outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "postgres_from_cex_clients" {
  security_group_id            = aws_security_group.postgres.id
  referenced_security_group_id = aws_security_group.cex_clients.id
  description                  = "PostgreSQL from ledger, outbox relay, and other approved CEX services"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_kms_key" "postgres" {
  description             = "Encryption key for ${local.resource_name} ledger PostgreSQL cluster"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "postgres" {
  name          = "alias/${local.resource_name}-postgres"
  target_key_id = aws_kms_key.postgres.key_id
}

resource "aws_rds_cluster" "ledger" {
  cluster_identifier = "${local.resource_name}-ledger"

  engine          = "postgres"
  engine_version  = var.postgres_engine_version
  port            = 5432
  database_name   = var.postgres_database_name
  master_username = var.postgres_master_username

  manage_master_user_password   = true
  master_user_secret_kms_key_id = aws_kms_key.postgres.arn

  availability_zones        = local.availability_zones
  db_subnet_group_name      = aws_db_subnet_group.ledger.name
  vpc_security_group_ids    = [aws_security_group.postgres.id]
  db_cluster_instance_class = var.postgres_instance_class
  storage_type              = "io2"
  allocated_storage         = var.postgres_allocated_storage_gib
  iops                      = var.postgres_iops
  storage_encrypted         = true
  kms_key_id                = aws_kms_key.postgres.arn

  backup_retention_period   = var.postgres_backup_retention_days
  copy_tags_to_snapshot     = true
  deletion_protection       = var.postgres_deletion_protection
  skip_final_snapshot       = var.postgres_skip_final_snapshot
  final_snapshot_identifier = var.postgres_skip_final_snapshot ? null : "${local.resource_name}-ledger-final"

  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]
  apply_immediately               = false
}


resource "aws_rds_cluster" "matching" {
  cluster_identifier              = "${local.resource_name}-matching"
  engine                          = "postgres"
  engine_version                  = var.postgres_engine_version
  port                            = 5432
  database_name                   = var.matching_postgres_database_name
  master_username                 = var.matching_postgres_master_username
  manage_master_user_password     = true
  master_user_secret_kms_key_id   = aws_kms_key.postgres.arn
  availability_zones              = local.availability_zones
  db_subnet_group_name            = aws_db_subnet_group.ledger.name
  vpc_security_group_ids          = [aws_security_group.postgres.id]
  db_cluster_instance_class       = var.postgres_instance_class
  storage_type                    = "io2"
  allocated_storage               = var.postgres_allocated_storage_gib
  iops                            = var.postgres_iops
  storage_encrypted               = true
  kms_key_id                      = aws_kms_key.postgres.arn
  backup_retention_period         = var.postgres_backup_retention_days
  copy_tags_to_snapshot           = true
  deletion_protection             = var.postgres_deletion_protection
  skip_final_snapshot             = var.postgres_skip_final_snapshot
  final_snapshot_identifier       = var.postgres_skip_final_snapshot ? null : "${local.resource_name}-matching-final"
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]
  apply_immediately               = false
}
