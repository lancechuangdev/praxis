output "msk_cluster_arn" {
  description = "ARN of the provisioned MSK cluster."
  value       = aws_msk_cluster.this.arn
}

output "msk_cluster_name" {
  description = "Name of the provisioned MSK cluster."
  value       = aws_msk_cluster.this.cluster_name
}

output "bootstrap_brokers_sasl_iam" {
  description = "IAM-authenticated TLS bootstrap brokers for the CEX services."
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "msk_security_group_id" {
  description = "Security group attached to the MSK brokers."
  value       = aws_security_group.msk.id
}

output "vpc_id" {
  description = "ID of the independent CEX VPC."
  value       = aws_vpc.cex.id
}

output "public_subnet_ids" {
  description = "Public subnet IDs for CEX internet-facing resources."
  value       = aws_subnet.public[*].id
}

output "order_alb_dns_name" {
  description = "Public Order ALB DNS name."
  value       = aws_lb.order.dns_name
}

output "private_subnet_ids" {
  description = "Private subnet IDs for MSK, ECS services, databases, and internal resources."
  value       = aws_subnet.private[*].id
}

output "cex_client_security_group_id" {
  description = "Security group to attach to CEX workloads that need IAM/TLS access to MSK."
  value       = aws_security_group.cex_clients.id
}

output "matching_writer_endpoint" {
  description = "PostgreSQL writer endpoint for the matching engine and its outbox relay."
  value       = aws_rds_cluster.matching.endpoint
}

output "matching_reader_endpoint" {
  description = "Read-only endpoint for replica-safe Matching queries."
  value       = aws_rds_cluster.matching.reader_endpoint
}

output "matching_master_secret_arn" {
  description = "RDS-managed administrative credentials for the Matching cluster."
  value       = aws_rds_cluster.matching.master_user_secret[0].secret_arn
}

output "ledger_writer_endpoint" {
  description = "PostgreSQL writer endpoint for the ledger service, outbox relay, and migrations."
  value       = aws_rds_cluster.ledger.endpoint
}

output "ledger_reader_endpoint" {
  description = "Load-balanced read-only PostgreSQL endpoint for reporting and other replica-safe queries."
  value       = aws_rds_cluster.ledger.reader_endpoint
}

output "ledger_database_name" {
  description = "Initial PostgreSQL database name."
  value       = aws_rds_cluster.ledger.database_name
}

output "ledger_master_secret_arn" {
  description = "Secrets Manager ARN containing the RDS-managed administrative credentials."
  value       = aws_rds_cluster.ledger.master_user_secret[0].secret_arn
}

output "postgres_security_group_id" {
  description = "Security group attached to the PostgreSQL cluster."
  value       = aws_security_group.postgres.id
}

output "topic_names" {
  description = "Kafka topic names keyed by their logical Terraform names."
  value       = { for key, topic in aws_msk_topic.this : key => topic.name }
}

output "ecr_repository_urls" {
  description = "ECR repository URLs keyed by service name. Use immutable image digests in ECS task definitions."
  value       = { for key, repository in aws_ecr_repository.service : key => repository.repository_url }
}

output "ecr_repository_arns" {
  description = "ECR repository ARNs keyed by service name for CI publisher and ECS execution-role policies."
  value       = { for key, repository in aws_ecr_repository.service : key => repository.arn }
}

output "ecs_cluster_name" {
  description = "Name of the ECS cluster used by the CEX services."
  value       = aws_ecs_cluster.this.name
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster used by the CEX services."
  value       = aws_ecs_cluster.this.arn
}

output "ecs_task_execution_role_arn" {
  description = "Shared ECS execution-role ARN for pulling images and delivering container logs. This is not an application task role."
  value       = aws_iam_role.ecs_task_execution.arn
}

output "ecs_service_log_group_names" {
  description = "CloudWatch application log-group names keyed by service name."
  value       = { for key, log_group in aws_cloudwatch_log_group.ecs_service : key => log_group.name }
}

output "trace_collector_log_group_names" {
  description = "ADOT collector log-group names for enabled ECS tracing, keyed by service."
  value       = { for key, log_group in aws_cloudwatch_log_group.trace_collector : key => log_group.name }
}

output "trace_export_alarm_names" {
  description = "Trace export failure alarm names for enabled ECS tracing, keyed by service."
  value       = { for key, alarm in aws_cloudwatch_metric_alarm.trace_export_failure : key => alarm.alarm_name }
}

output "ledger_consumer_lag_alarm_name" {
  description = "Ledger MSK consumer-lag alarm name when the Ledger service is enabled."
  value       = var.ledger_image_digest == null ? null : aws_cloudwatch_metric_alarm.ledger_consumer_lag[0].alarm_name
}

output "managed_metrics_workspace_id" {
  description = "Managed Prometheus workspace ID for application metrics."
  value       = aws_prometheus_workspace.application.id
}

output "managed_metrics_query_endpoint" {
  description = "Prometheus-compatible workspace endpoint; requests require AWS SigV4 authentication."
  value       = aws_prometheus_workspace.application.prometheus_endpoint
}

output "grafana_workspace_id" {
  description = "Amazon Managed Grafana workspace ID."
  value       = aws_grafana_workspace.praxis.id
}

output "grafana_workspace_url" {
  description = "URL of the Amazon Managed Grafana workspace."
  value       = "https://${aws_grafana_workspace.praxis.endpoint}"
}

output "grafana_terraform_service_account_id" {
  description = "Managed Grafana service account ID; create its short-lived API token outside Terraform."
  value       = aws_grafana_workspace_service_account.terraform.service_account_id
}

output "service_task_role_arns" {
  description = "Outbox Relay ECS task-role ARN, distinct from the shared task execution role."
  value       = { for key, role in aws_iam_role.service_task : key => role.arn }
}

output "outbox_ecs_service_arn" {
  description = "Outbox Relay ECS service ARN when outbox_image_digest is set."
  value       = var.outbox_image_digest == null ? null : aws_ecs_service.outbox[0].id
}

output "ledger_migration_config" {
  description = "Non-secret settings for the one-off EKS Ledger migration Job. Terraform does not run the Job."
  value = {
    image          = var.ledger_migration_image_digest == null ? null : "${aws_ecr_repository.service["ledger_service"].repository_url}@${var.ledger_migration_image_digest}"
    db_writer_host = aws_rds_cluster.ledger.endpoint
    db_user        = var.postgres_master_username
    db_name        = var.postgres_database_name
    secret_arn     = aws_rds_cluster.ledger.master_user_secret[0].secret_arn
    aws_region     = var.aws_region
  }
}

output "outbox_migration_task_definition_arn" {
  description = "One-off Outbox migration task definition ARN when outbox_migration_image_digest is set. Terraform does not run the task."
  value       = var.outbox_migration_image_digest == null ? null : aws_ecs_task_definition.outbox_migration[0].arn
}

output "eks_cluster_name" {
  description = "EKS cluster for Order, Ledger, and Matching."
  value       = aws_eks_cluster.this.name
}

output "eks_cluster_endpoint" {
  description = "EKS API endpoint; private by default."
  value       = aws_eks_cluster.this.endpoint
}

output "eks_cluster_ca_data" {
  description = "Base64-encoded EKS cluster certificate authority data for the Kubernetes Terraform provider."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "eks_cluster_security_group_id" {
  description = "EKS cluster security group used by managed nodes for VPC access to MSK and RDS."
  value       = local.eks_cluster_security_group_id
}

output "eks_pod_role_arns" {
  description = "Pod Identity roles for Ledger, Matching, the EKS collector, and the one-off Ledger migration Job. Order has no AWS role."
  value       = merge({ for key, role in aws_iam_role.eks_pod : key => role.arn }, { ledger_migration = aws_iam_role.eks_ledger_migration.arn, collector = aws_iam_role.eks_collector.arn })
}

output "eks_image_refs" {
  description = "Digest-pinned ECR images for Kubernetes workloads. All three digests must be set before deployment."
  value = {
    order    = var.order_image_digest == null ? null : "${aws_ecr_repository.service["order_service"].repository_url}@${var.order_image_digest}"
    ledger   = var.ledger_image_digest == null ? null : "${aws_ecr_repository.service["ledger_service"].repository_url}@${var.ledger_image_digest}"
    matching = var.matching_image_digest == null ? null : "${aws_ecr_repository.service["matching_engine"].repository_url}@${var.matching_image_digest}"
  }
}

output "eks_runtime_config" {
  description = "Non-secret application settings for the Kubernetes Terraform stack."
  value = {
    aws_region                = var.aws_region
    environment               = var.environment
    eks_cluster_name          = aws_eks_cluster.this.name
    eks_collector_image       = var.eks_collector_image
    trace_sample_ratio        = var.trace_sample_ratio
    amp_remote_write_endpoint = "${trimsuffix(aws_prometheus_workspace.application.prometheus_endpoint, "/")}/api/v1/remote_write"
    ledger_db_writer_host     = aws_rds_cluster.ledger.endpoint
    ledger_db_reader_host     = aws_rds_cluster.ledger.reader_endpoint
    ledger_db_name            = var.postgres_database_name
    ledger_consumer_group     = var.ledger_consumer_group
    ledger_commands_topic     = aws_msk_topic.this["ledger_commands"].name
    matching_db_writer_host   = aws_rds_cluster.matching.endpoint
    matching_db_name          = var.matching_postgres_database_name
    matching_db_secret_arn    = aws_rds_cluster.matching.master_user_secret[0].secret_arn
    matching_db_user          = var.matching_postgres_master_username
    matching_events_topic     = aws_msk_topic.this["matching_events"].name
    bootstrap_brokers_iam     = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
    ledger_runtime_secret_arn = var.ledger_runtime_secret_arn
  }
}
