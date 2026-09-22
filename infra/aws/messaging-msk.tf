resource "aws_kms_key" "msk" {
  description             = "Encryption key for ${local.resource_name} MSK data at rest"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "msk" {
  name          = "alias/${local.resource_name}-msk"
  target_key_id = aws_kms_key.msk.key_id
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "/aws/msk/${local.resource_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_msk_configuration" "this" {
  kafka_versions = [var.kafka_version]
  name           = "${local.resource_name}-configuration"

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.io.threads=8
    num.network.threads=5
    num.partitions=24
    socket.request.max.bytes=104857600
    transaction.state.log.min.isr=2
    transaction.state.log.replication.factor=3
  PROPERTIES
}

resource "aws_msk_cluster" "this" {
  cluster_name           = local.resource_name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count
  enhanced_monitoring    = "PER_BROKER"

  broker_node_group_info {
    client_subnets  = aws_subnet.private[*].id
    instance_type   = var.broker_instance_type
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_size_gib
      }
    }
  }

  client_authentication {
    sasl {
      iam = true
    }
    unauthenticated = false
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = aws_kms_key.msk.arn

    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  lifecycle {
    precondition {
      condition     = var.broker_count % var.availability_zone_count == 0
      error_message = "broker_count must be a multiple of availability_zone_count."
    }
  }
}
