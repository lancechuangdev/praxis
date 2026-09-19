resource "aws_service_discovery_private_dns_namespace" "services" {
  name        = "${local.resource_name}.internal"
  description = "Private DNS for ${local.resource_name} ECS services"
  vpc         = aws_vpc.cex.id
}

locals {
  grpc_services = {
    ledger_service = {
      name = "ledger-service"
      port = 9091
    }
    matching_engine = {
      name = "matching-engine"
      port = 9092
    }
  }
}

resource "aws_service_discovery_service" "grpc" {
  for_each = local.grpc_services

  name = each.value.name

  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.services.id
    routing_policy = "MULTIVALUE"

    dns_records {
      ttl  = 10
      type = "A"
    }
  }

  health_check_custom_config {}
}
