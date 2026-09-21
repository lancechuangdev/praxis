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

output "trace_collector_log_group_names" {
  description = "ADOT collector log-group names for enabled ECS tracing, keyed by service."
  value       = { for key, log_group in aws_cloudwatch_log_group.trace_collector : key => log_group.name }
}

output "trace_export_alarm_names" {
  description = "Trace export failure alarm names for enabled ECS tracing, keyed by service."
  value       = { for key, alarm in aws_cloudwatch_metric_alarm.trace_export_failure : key => alarm.alarm_name }
}

output "service_discovery_namespace" {
  description = "Private DNS namespace for ECS services in the CEX VPC."
  value       = aws_service_discovery_private_dns_namespace.services.name
}

output "grpc_service_discovery_arns" {
  description = "Cloud Map service ARNs to attach to the corresponding ECS service registries."
  value       = { for key, service in aws_service_discovery_service.grpc : key => service.arn }
}

output "grpc_service_addresses" {
  description = "Order Service gRPC targets after ECS services register their tasks with Cloud Map."
  value = {
    for key, service in local.grpc_services :
    key => "${service.name}.${aws_service_discovery_private_dns_namespace.services.name}:${service.port}"
  }
}

output "service_task_role_arns" {
  description = "Application ECS task-role ARNs keyed by service name; distinct from the shared task execution role."
  value       = { for key, role in aws_iam_role.service_task : key => role.arn }
}

output "order_alb_dns_name" {
  description = "Public Order ALB DNS name, if enabled. The listener returns 503 until the authenticated Order ECS rollout."
  value       = var.order_alb_enabled ? aws_lb.order[0].dns_name : null
}

output "order_target_group_arn" {
  description = "Order IP target group ARN to attach to the future ECS service."
  value       = var.order_alb_enabled ? aws_lb_target_group.order[0].arn : null
}

output "order_task_security_group_id" {
  description = "Private Order task security group allowing inbound HTTP only from the ALB."
  value       = var.order_alb_enabled ? aws_security_group.order_task[0].id : null
}

output "order_grpc_client_security_group_id" {
  description = "Security group to attach to future Order tasks so they can reach private gRPC services."
  value       = aws_security_group.order_grpc_clients.id
}

output "matching_task_security_group_id" {
  description = "Private Matching task ingress security group."
  value       = aws_security_group.matching_task.id
}

output "matching_ecs_service_arn" {
  description = "Matching ECS service ARN when matching_image_digest is set."
  value       = var.matching_image_digest == null ? null : aws_ecs_service.matching[0].id
}

output "ledger_task_security_group_id" {
  description = "Private Ledger task ingress security group."
  value       = aws_security_group.ledger_task.id
}

output "ledger_ecs_service_arn" {
  description = "Ledger ECS service ARN when ledger_image_digest is set."
  value       = var.ledger_image_digest == null ? null : aws_ecs_service.ledger[0].id
}

output "outbox_ecs_service_arn" {
  description = "Outbox Relay ECS service ARN when outbox_image_digest is set."
  value       = var.outbox_image_digest == null ? null : aws_ecs_service.outbox[0].id
}

output "ledger_migration_task_definition_arn" {
  description = "One-off Ledger migration task definition ARN when ledger_migration_image_digest is set. Terraform does not run the task."
  value       = var.ledger_migration_image_digest == null ? null : aws_ecs_task_definition.ledger_migration[0].arn
}

output "outbox_migration_task_definition_arn" {
  description = "One-off Outbox migration task definition ARN when outbox_migration_image_digest is set. Terraform does not run the task."
  value       = var.outbox_migration_image_digest == null ? null : aws_ecs_task_definition.outbox_migration[0].arn
}

output "order_ecs_service_arn" {
  description = "Private Order ECS service ARN when order_image_digest is set. The public ALB listener still returns 503."
  value       = var.order_image_digest == null ? null : aws_ecs_service.order[0].id
}
