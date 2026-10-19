mock_provider "aws" {
  mock_resource "aws_iam_role" { defaults = { arn = "arn:aws:iam::123456789012:role/projection-test" } }
  mock_resource "aws_secretsmanager_secret" { defaults = { arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:projection-test-AbCdEf" } }
}
mock_provider "random" {}
mock_provider "mongodbatlas" {}
override_resource {
  target = aws_vpc_endpoint.atlas
  values = { id = "vpce-test" }
}
variables {
  aws_region                = "us-east-1"
  atlas_region              = "US_EAST_1"
  atlas_project_id          = "000000000000000000000000"
  name                      = "praxis-projection-test"
  database                  = "projection_test"
  vpc_id                    = "vpc-test"
  private_subnet_ids        = ["subnet-a", "subnet-b"]
  client_security_group_ids = ["sg-app"]
  eks_cluster_name          = "test-cluster"
  msk_cluster_arn           = "arn:aws:kafka:us-east-1:123456789012:cluster/test/00000000-0000-0000-0000-000000000000-1"
  kafka_brokers             = ["broker.internal:9098"]
  dependency_cidrs          = ["10.0.0.0/24"]
  publisher_role_names      = { order = "order", ledger = "ledger" }
  operator_role_name        = "operator"
  image                     = "example.invalid/projection@sha256:0000000000000000000000000000000000000000000000000000000000000000"
  targets                   = { journal = { stream = "journal", mode = "rebuild", generation = "test-v1", group_namespace = "test-db", service_account = "projection-journal" } }
}
run "private_projection_plan" {
  command = plan
  assert {
    condition     = aws_vpc_endpoint.atlas.private_dns_enabled == false && mongodbatlas_advanced_cluster.projection.termination_protection_enabled
    error_message = "Atlas must use its private SRV path and retain deletion protection."
  }
  assert {
    condition     = !contains([for a in mongodbatlas_custom_db_role.runtime["journal"].actions : a.action], "REMOVE") && !contains([for a in mongodbatlas_custom_db_role.runtime["journal"].actions : a.action], "CREATE_COLLECTION")
    error_message = "Runtime MongoDB roles must not delete records or provision collections."
  }
  assert {
    condition     = !strcontains(aws_iam_role_policy.publisher["order"].policy, "*") && strcontains(aws_iam_role_policy.publisher["order"].policy, "kafka-cluster:WriteData")
    error_message = "Order publication must grant its exact topic without wildcards."
  }

}

run "workload_rendering" {
  command = plan
  assert {
    condition     = alltrue([for document in split("\n---\n", templatefile("${path.module}/workload.yaml.tftpl", { NAME = "projection-test", SERVICE_ACCOUNT = "projection-journal", STREAM = "journal", MODE = "rebuild", GENERATION = "test-v1", GROUP_NAMESPACE = "test-db", DATABASE = var.database, SECRET_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test", SECRETS_ENDPOINT = "https://vpce-test.secretsmanager.us-east-1.vpce.amazonaws.com", AWS_REGION = var.aws_region, BROKERS = join(",", var.kafka_brokers), IMAGE = var.image, READ_CLIENT_NAMESPACE = "gateway", DEPENDENCY_CIDRS = var.dependency_cidrs, READS_ENABLED = "false", JWT_ISSUER = "", JWT_AUDIENCE = "", JWT_KEY_ID = "" })) : can(yamldecode(document))])
    error_message = "All private workload documents must render as valid YAML."
  }
}
