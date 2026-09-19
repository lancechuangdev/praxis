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

output "private_subnet_ids" {
  description = "Private subnet IDs for MSK, ECS services, databases, and internal resources."
  value       = aws_subnet.private[*].id
}

output "cex_client_security_group_id" {
  description = "Security group to attach to CEX workloads that need IAM/TLS access to MSK."
  value       = aws_security_group.cex_clients.id
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
