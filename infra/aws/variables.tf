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

variable "eks_enabled" {
  description = "Provision an opt-in EKS cluster and EC2 managed node group for Order, Ledger, and Matching. ECS Outbox Relay remains on Fargate."
  type        = bool
  default     = false
}

variable "eks_version" {
  description = "Pinned EKS Kubernetes minor version; verify availability and standard-support dates in the selected Region before enabling."
  type        = string
  default     = "1.35"

  validation {
    condition     = can(regex("^1\\.[0-9]+$", var.eks_version))
    error_message = "eks_version must be a Kubernetes minor version such as 1.35."
  }
}

variable "eks_admin_principal_arn" {
  description = "IAM role ARN granted EKS cluster-admin access. Required when eks_enabled is true; use a dedicated operator role."
  type        = string
  default     = null
  nullable    = true
}

variable "eks_public_access_cidrs" {
  description = "Optional operator CIDRs allowed to reach the EKS API publicly. Empty disables public API access; private access remains enabled."
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.eks_public_access_cidrs : can(cidrnetmask(cidr)) && cidr != "0.0.0.0/0"])
    error_message = "eks_public_access_cidrs must contain valid, narrower-than-global IPv4 CIDRs."
  }
}

variable "eks_node_instance_types" {
  description = "EC2 instance types for the EKS managed node group; keep one architecture per group."
  type        = list(string)
  default     = ["m6i.xlarge"]

  validation {
    condition     = length(var.eks_node_instance_types) >= 1 && length(var.eks_node_instance_types) <= 3
    error_message = "eks_node_instance_types must contain one to three compatible instance types."
  }
}

variable "eks_node_min_size" {
  description = "Minimum EKS EC2 node count across three Availability Zones."
  type        = number
  default     = 3

  validation {
    condition     = var.eks_node_min_size >= 1 && var.eks_node_min_size <= 20 && floor(var.eks_node_min_size) == var.eks_node_min_size
    error_message = "eks_node_min_size must be an integer from one to 20."
  }
}

variable "eks_node_max_size" {
  description = "Maximum EKS EC2 node count; no Kubernetes autoscaler is installed by this stack."
  type        = number
  default     = 6

  validation {
    condition     = var.eks_node_max_size >= 1 && var.eks_node_max_size <= 100 && floor(var.eks_node_max_size) == var.eks_node_max_size
    error_message = "eks_node_max_size must be an integer from one to 100."
  }
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

variable "ledger_consumer_max_offset_lag" {
  description = "Maximum tolerated Ledger consumer offset lag before its CloudWatch alarm enters ALARM."
  type        = number
  default     = 1000

  validation {
    condition     = var.ledger_consumer_max_offset_lag >= 1
    error_message = "ledger_consumer_max_offset_lag must be at least one."
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

variable "ledger_ec2_enabled" {
  description = "Provision a parallel Ledger ECS EC2 service without changing the Fargate service or Order's Ledger endpoint. Requires ledger_image_digest."
  type        = bool
  default     = false
}

variable "ledger_fargate_desired_count" {
  description = "Ledger Fargate task count. Leave at one until the EC2 Ledger service and Order endpoint have been verified; zero retains the service for rollback."
  type        = number
  default     = 1

  validation {
    condition     = var.ledger_fargate_desired_count >= 0 && var.ledger_fargate_desired_count <= 100 && floor(var.ledger_fargate_desired_count) == var.ledger_fargate_desired_count
    error_message = "ledger_fargate_desired_count must be an integer from zero to 100."
  }
}

variable "ledger_ec2_desired_count" {
  description = "Number of parallel Ledger EC2 tasks. Defaults to zero because running tasks join the live Ledger Kafka consumer group and share its database."
  type        = number
  default     = 0

  validation {
    condition     = var.ledger_ec2_desired_count >= 0 && var.ledger_ec2_desired_count <= 100 && floor(var.ledger_ec2_desired_count) == var.ledger_ec2_desired_count
    error_message = "ledger_ec2_desired_count must be an integer from zero to 100."
  }
}

variable "ledger_ec2_instance_type" {
  description = "EC2 instance type for the opt-in Ledger capacity provider."
  type        = string
  default     = "m6i.large"
}

variable "ledger_ec2_max_instances" {
  description = "Maximum EC2 instances for the Ledger capacity provider."
  type        = number
  default     = 6

  validation {
    condition     = var.ledger_ec2_max_instances >= 2 && var.ledger_ec2_max_instances <= 100 && floor(var.ledger_ec2_max_instances) == var.ledger_ec2_max_instances
    error_message = "ledger_ec2_max_instances must be an integer from two to 100."
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

variable "order_fargate_desired_count" {
  description = "Minimum and desired Order Fargate task count. Set zero only after traffic has moved away; the ECS service remains for rollback."
  type        = number
  default     = 1

  validation {
    condition     = contains([0, 1], var.order_fargate_desired_count)
    error_message = "order_fargate_desired_count must be zero or one."
  }
}

variable "order_ledger_target" {
  description = "Ledger endpoint used by Order services: the original Fargate Cloud Map name or the parallel EC2 Cloud Map name."
  type        = string
  default     = "fargate"

  validation {
    condition     = contains(["fargate", "ec2"], var.order_ledger_target)
    error_message = "order_ledger_target must be fargate or ec2."
  }
}

variable "order_matching_target" {
  description = "Matching Cloud Map endpoint used by Order: fargate or ec2. Change only after the EC2 Matching task is healthy."
  type        = string
  default     = "fargate"

  validation {
    condition     = contains(["fargate", "ec2"], var.order_matching_target)
    error_message = "order_matching_target must be fargate or ec2."
  }
}

variable "order_ec2_enabled" {
  description = "Create a parallel Order ECS service on EC2; leave the Fargate service intact. Requires order_image_digest. Does not expose Order publicly."
  type        = bool
  default     = false
}

variable "order_ec2_desired_count" {
  description = "Minimum and desired Order ECS EC2 task count when enabled. Set zero after Kubernetes cutover."
  type        = number
  default     = 1

  validation {
    condition     = contains([0, 1], var.order_ec2_desired_count)
    error_message = "order_ec2_desired_count must be zero or one."
  }
}

variable "order_ec2_instance_type" {
  description = "Instance type for the opt-in Order ECS EC2 capacity provider. Check task and awsvpc ENI capacity before deployment."
  type        = string
  default     = "m6i.large"
}

variable "order_ec2_max_instances" {
  description = "Upper bound on EC2 instances managed by the Order capacity provider."
  type        = number
  default     = 6

  validation {
    condition     = var.order_ec2_max_instances >= 2 && var.order_ec2_max_instances <= 100
    error_message = "order_ec2_max_instances must be between 2 and 100."
  }
}

variable "trace_collector_image" {
  description = "Digest-pinned AWS Distro for OpenTelemetry Collector image. Null disables ECS tracing and managed metrics."
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

variable "managed_metrics_enabled" {
  description = "Create an Amazon Managed Service for Prometheus workspace and remote-write service metrics from ADOT sidecars. Requires trace_collector_image."
  type        = bool
  default     = false
}

variable "managed_metrics_retention_days" {
  description = "Retention period for the managed Prometheus workspace."
  type        = number
  default     = 30

  validation {
    condition     = var.managed_metrics_retention_days >= 1 && var.managed_metrics_retention_days <= 1095
    error_message = "managed_metrics_retention_days must be between 1 and 1095."
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

variable "matching_ec2_enabled" {
  description = "Provision a separate Matching ECS EC2 service without moving Order traffic. The mock engine cannot have overlapping Fargate and EC2 owners."
  type        = bool
  default     = false
}

variable "matching_fargate_desired_count" {
  description = "Mock Matching Fargate task count; set to zero before starting the EC2 task."
  type        = number
  default     = 1

  validation {
    condition     = contains([0, 1], var.matching_fargate_desired_count)
    error_message = "matching_fargate_desired_count must be zero or one."
  }
}

variable "matching_ec2_desired_count" {
  description = "Mock Matching EC2 task count; defaults to zero and must not overlap a Fargate task."
  type        = number
  default     = 0

  validation {
    condition     = contains([0, 1], var.matching_ec2_desired_count)
    error_message = "matching_ec2_desired_count must be zero or one."
  }
}

variable "matching_ec2_instance_type" {
  description = "Instance type for the opt-in Matching ECS EC2 capacity provider."
  type        = string
  default     = "m6i.large"
}

variable "matching_ec2_max_instances" {
  description = "Maximum EC2 instances for the Matching capacity provider, allowing headroom for replacement."
  type        = number
  default     = 2

  validation {
    condition     = var.matching_ec2_max_instances >= 2 && var.matching_ec2_max_instances <= 100 && floor(var.matching_ec2_max_instances) == var.matching_ec2_max_instances
    error_message = "matching_ec2_max_instances must be an integer from two to 100."
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
