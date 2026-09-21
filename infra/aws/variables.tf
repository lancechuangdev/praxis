variable "name" {
  description = "Name prefix used for CEX infrastructure."
  type        = string
  default     = "praxis-cex"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "development"
}

variable "aws_region" {
  description = "AWS region in which to create the MSK resources."
  type        = string
  default     = "us-west-2"
}

variable "vpc_cidr" {
  description = "CIDR for the independent CEX VPC."
  type        = string
  default     = "10.80.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr))
    error_message = "vpc_cidr must be a valid IPv4 CIDR."
  }
}

variable "availability_zone_count" {
  description = "Number of Availability Zones used by the CEX network."
  type        = number
  default     = 3

  validation {
    condition     = var.availability_zone_count == 3
    error_message = "availability_zone_count must be three for the PostgreSQL Multi-AZ DB cluster."
  }
}

variable "nat_gateway_per_az" {
  description = "Create one NAT gateway per Availability Zone for high availability. When false, all private subnets use one NAT gateway."
  type        = bool
  default     = true
}

variable "kafka_version" {
  description = "Amazon MSK Kafka version available in the selected region."
  type        = string
  default     = "3.7.x"
}

variable "broker_instance_type" {
  description = "MSK provisioned broker instance type."
  type        = string
  default     = "kafka.m7g.large"
}

variable "broker_count" {
  description = "Number of brokers. It must be a multiple of the number of supplied subnets."
  type        = number
  default     = 3

  validation {
    condition     = var.broker_count >= 3
    error_message = "broker_count must be at least three for the default replication factor."
  }
}

variable "broker_volume_size_gib" {
  description = "EBS storage allocated to each broker."
  type        = number
  default     = 1000
}

variable "log_retention_days" {
  description = "CloudWatch retention for MSK broker logs."
  type        = number
  default     = 30
}

variable "postgres_engine_version" {
  description = "Optional exact RDS PostgreSQL engine version. Null lets AWS select its current default; pin this in production."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.postgres_engine_version == null || trimspace(var.postgres_engine_version) != ""
    error_message = "postgres_engine_version must be null or a non-empty version string."
  }
}

variable "postgres_instance_class" {
  description = "Instance class used by the writer and both readable standby instances."
  type        = string
  default     = "db.r6gd.xlarge"
}

variable "postgres_database_name" {
  description = "Initial PostgreSQL database used by the ledger service."
  type        = string
  default     = "cex_ledger"
}

variable "postgres_master_username" {
  description = "Administrative username. RDS manages its password in Secrets Manager."
  type        = string
  default     = "ledger_admin"
}

variable "ledger_runtime_secret_arn" {
  description = "Secrets Manager ARN of the Ledger runtime password. Required when ledger_image_digest is set; create the matching restricted PostgreSQL role first."
  type        = string
  default     = ""
}

variable "outbox_runtime_secret_arn" {
  description = "Secrets Manager ARN of the Outbox runtime password. Required when outbox_image_digest is set; create the matching restricted PostgreSQL role first."
  type        = string
  default     = ""
}

variable "postgres_allocated_storage_gib" {
  description = "Provisioned io2 storage allocated to each DB instance."
  type        = number
  default     = 500
}

variable "postgres_iops" {
  description = "Provisioned IOPS allocated to each DB instance."
  type        = number
  default     = 10000
}

variable "postgres_backup_retention_days" {
  description = "Number of days to retain automated PostgreSQL backups."
  type        = number
  default     = 14
}

variable "postgres_deletion_protection" {
  description = "Protect the ledger database from deletion through the RDS API."
  type        = bool
  default     = true
}

variable "postgres_skip_final_snapshot" {
  description = "Skip the final snapshot when destroying the cluster. Keep false for production."
  type        = bool
  default     = false
}

variable "ecr_repositories" {
  description = "ECR repository suffixes keyed by service name. Repository names are prefixed with the application and environment."
  type        = map(string)
  default = {
    ledger_service  = "ledger-service"
    matching_engine = "matching-engine"
    order_service   = "order-service"
    outbox_relay    = "outbox-relay"
  }

  validation {
    condition = length(var.ecr_repositories) > 0 && alltrue([
      for name in values(var.ecr_repositories) :
      can(regex("^[a-z0-9]+(?:[._/-][a-z0-9]+)*$", name))
      ]) && length(distinct(values(var.ecr_repositories))) == length(var.ecr_repositories) && alltrue([
      for key in ["ledger_service", "matching_engine", "order_service", "outbox_relay"] :
      contains(keys(var.ecr_repositories), key)
    ])
    error_message = "ecr_repositories must contain all four current service keys with unique, valid lowercase suffixes."
  }
}

variable "ecr_tagged_image_retention_count" {
  description = "Number of tagged images retained in each service repository."
  type        = number
  default     = 50

  validation {
    condition     = var.ecr_tagged_image_retention_count >= 2
    error_message = "ecr_tagged_image_retention_count must be at least two to preserve rollback capacity."
  }
}

variable "ecr_untagged_retention_days" {
  description = "Days to retain untagged images in each service repository."
  type        = number
  default     = 7

  validation {
    condition     = var.ecr_untagged_retention_days >= 1
    error_message = "ecr_untagged_retention_days must be at least one."
  }
}

variable "ecs_container_insights_mode" {
  description = "Container Insights mode for the ECS cluster. Enhanced mode adds task- and container-level telemetry."
  type        = string
  default     = "enhanced"

  validation {
    condition     = contains(["enabled", "enhanced"], var.ecs_container_insights_mode)
    error_message = "ecs_container_insights_mode must be enabled or enhanced."
  }
}

variable "ecs_log_retention_days" {
  description = "CloudWatch retention for ECS service application logs."
  type        = number
  default     = 30

  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545,
      731, 1096, 1827, 2192, 2557, 2922, 3288, 3653
    ], var.ecs_log_retention_days)
    error_message = "ecs_log_retention_days must be a retention period supported by CloudWatch Logs."
  }
}

variable "ledger_consumer_group" {
  description = "Ledger Kafka consumer group authorized by the Ledger task role; set LEDGER_CONSUMER_GROUP to the same value in ECS."
  type        = string
  default     = "cex-ledger-service"

  validation {
    condition     = can(regex("^[A-Za-z0-9._-]+$", var.ledger_consumer_group))
    error_message = "ledger_consumer_group must be a valid non-empty Kafka group name."
  }
}

variable "ledger_image_digest" {
  description = "Opt-in Ledger image digest (sha256:...) already pushed to its ECR repository. Null creates no Ledger task definition or ECS service."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.ledger_image_digest == null || can(regex("^sha256:[0-9a-f]{64}$", var.ledger_image_digest))
    error_message = "ledger_image_digest must be null or a lowercase sha256 digest with 64 hexadecimal characters."
  }
}

variable "ledger_migration_image_digest" {
  description = "Ledger image digest for the one-off migration task, independently deployable before the Ledger service image."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.ledger_migration_image_digest == null || can(regex("^sha256:[0-9a-f]{64}$", var.ledger_migration_image_digest))
    error_message = "ledger_migration_image_digest must be null or a lowercase sha256 digest with 64 hexadecimal characters."
  }
}

variable "outbox_image_digest" {
  description = "Opt-in Outbox Relay image digest (sha256:...) already pushed to its ECR repository. Null creates no Outbox task definition or ECS service."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.outbox_image_digest == null || can(regex("^sha256:[0-9a-f]{64}$", var.outbox_image_digest))
    error_message = "outbox_image_digest must be null or a lowercase sha256 digest with 64 hexadecimal characters."
  }
}

variable "outbox_migration_image_digest" {
  description = "Outbox image digest for the one-off migration task, independently deployable before the Outbox service image."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.outbox_migration_image_digest == null || can(regex("^sha256:[0-9a-f]{64}$", var.outbox_migration_image_digest))
    error_message = "outbox_migration_image_digest must be null or a lowercase sha256 digest with 64 hexadecimal characters."
  }
}

variable "order_image_digest" {
  description = "Opt-in Order image digest (sha256:...) already pushed to its ECR repository. Null creates no Order task definition or ECS service."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.order_image_digest == null || can(regex("^sha256:[0-9a-f]{64}$", var.order_image_digest))
    error_message = "order_image_digest must be null or a lowercase sha256 digest with 64 hexadecimal characters."
  }
}

variable "ecs_alarm_sns_topic_arn" {
  description = "Optional existing SNS topic ARN for ECS unavailable-service alarm notifications. Null leaves alarms without actions."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.ecs_alarm_sns_topic_arn == null || can(regex("^arn:[^:]+:sns:[^:]+:[0-9]{12}:[^:]+$", var.ecs_alarm_sns_topic_arn))
    error_message = "ecs_alarm_sns_topic_arn must be null or a valid SNS topic ARN."
  }
}

variable "trace_collector_image" {
  description = "Digest-pinned AWS Distro for OpenTelemetry Collector image. Null disables ECS tracing; set an SNS alarm topic before enabling."
  type        = string
  default     = null

  validation {
    condition     = var.trace_collector_image == null || can(regex("^.+@sha256:[0-9a-f]{64}$", var.trace_collector_image))
    error_message = "trace_collector_image must be null or an image URI pinned by sha256 digest."
  }
}

variable "trace_sample_ratio" {
  description = "Parent-based root sampling ratio for ECS service traces sent to X-Ray."
  type        = number
  default     = 0.1

  validation {
    condition     = var.trace_sample_ratio >= 0 && var.trace_sample_ratio <= 1
    error_message = "trace_sample_ratio must be between zero and one."
  }
}

variable "matching_image_digest" {
  description = "Opt-in Matching image digest (sha256:...) already pushed to its ECR repository. Null creates no Matching task definition or ECS service."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.matching_image_digest == null || can(regex("^sha256:[0-9a-f]{64}$", var.matching_image_digest))
    error_message = "matching_image_digest must be null or a lowercase sha256 digest with 64 hexadecimal characters."
  }
}

variable "order_alb_enabled" {
  description = "Create the public Order ALB foundation. The HTTPS listener returns 503 until edge authentication and the ECS rollout are implemented."
  type        = bool
  default     = false
}

variable "order_alb_certificate_arn" {
  description = "ACM certificate ARN for the Order HTTPS listener; required when order_alb_enabled is true."
  type        = string
  default     = null
  nullable    = true
}

variable "order_alb_deletion_protection" {
  description = "Protect the Order ALB from accidental deletion; enable for production."
  type        = bool
  default     = false
}

variable "topics" {
  description = "Kafka topics managed through the Amazon MSK topic API."
  type = map(object({
    name               = string
    partitions         = number
    replication_factor = number
    configs            = map(string)
  }))

  default = {
    matching_events = {
      name               = "matching.events.v1"
      partitions         = 96
      replication_factor = 3
      configs = {
        "cleanup.policy"      = "delete"
        "retention.ms"        = "604800000"
        "min.insync.replicas" = "2"
      }
    }
    ledger_commands = {
      name               = "ledger.commands.v1"
      partitions         = 24
      replication_factor = 3
      configs = {
        "cleanup.policy"      = "delete"
        "retention.ms"        = "604800000"
        "min.insync.replicas" = "2"
      }
    }
    ledger_events = {
      name               = "ledger.events.v1"
      partitions         = 24
      replication_factor = 3
      configs = {
        "cleanup.policy"      = "delete"
        "retention.ms"        = "604800000"
        "min.insync.replicas" = "2"
      }
    }
    ledger_events_dlq = {
      name               = "ledger.events.dlq.v1"
      partitions         = 6
      replication_factor = 3
      configs = {
        "cleanup.policy"      = "delete"
        "retention.ms"        = "2592000000"
        "min.insync.replicas" = "2"
      }
    }
  }

  validation {
    condition = alltrue([
      for topic in values(var.topics) :
      trimspace(topic.name) != "" && topic.partitions > 0 && topic.replication_factor > 0
    ])
    error_message = "Every topic must have positive partition and replication counts."
  }
}

variable "tags" {
  description = "Additional tags applied to supported AWS resources."
  type        = map(string)
  default     = {}
}
