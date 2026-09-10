variable "name" {
  description = "Name prefix used for CEX infrastructure."
  type        = string
  default     = "stablerail-cex"
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
