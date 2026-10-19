terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws          = { source = "hashicorp/aws", version = ">= 6.62.0, < 7.0.0" }
    mongodbatlas = { source = "mongodb/mongodbatlas", version = "= 2.8.0" }
    random       = { source = "hashicorp/random", version = "~> 3.7" }
  }
}
provider "aws" { region = var.aws_region }
provider "mongodbatlas" {}
variable "aws_region" { type = string }
variable "atlas_project_id" { type = string }
variable "atlas_region" { type = string }
variable "name" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,30}[a-z0-9]$", var.name))
    error_message = "Use a lowercase DNS-style deployment name of 2..32 characters."
  }
}
variable "database" {
  type = string
  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_-]{0,62}$", var.database))
    error_message = "Use an explicit database name."
  }
}
variable "vpc_id" { type = string }
variable "private_subnet_ids" {
  type = set(string)
  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "Use private subnets in at least two availability zones."
  }
}
variable "client_security_group_ids" { type = set(string) }
variable "eks_cluster_name" { type = string }
variable "msk_cluster_arn" { type = string }
variable "targets" {
  description = "Exact enrolled stream/mode/generation identities. Independent databases must use independent namespaces."
  type        = map(object({ stream = string, mode = string, generation = string, group_namespace = string, service_account = string }))
  validation {
    condition     = alltrue([for t in values(var.targets) : contains(["workflow", "journal"], t.stream) && contains(["shadow", "rebuild"], t.mode) && can(regex("^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$", t.generation)) && can(regex("^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$", t.group_namespace))])
    error_message = "Enroll explicit workflow/journal shadow/rebuild targets."
  }
  validation {
    condition     = alltrue([for key, t in var.targets : can(regex("^[a-z][a-z0-9-]{0,25}$", key)) && can(regex("^[a-z][a-z0-9-]{0,61}[a-z0-9]$", t.service_account))]) && length(distinct([for t in values(var.targets) : t.service_account])) == length(var.targets)
    error_message = "Use short DNS-style target keys and unique service account names."
  }
}
variable "publisher_role_names" {
  description = "Existing Order and Ledger role names to receive exact-topic publication grants."
  type        = object({ order = string, ledger = string })
}
resource "mongodbatlas_advanced_cluster" "projection" {
  project_id                     = var.atlas_project_id
  name                           = var.name
  cluster_type                   = "REPLICASET"
  backup_enabled                 = true
  pit_enabled                    = true
  termination_protection_enabled = true
  replication_specs              = [{ region_configs = [{ provider_name = "AWS", region_name = var.atlas_region, priority = 7, electable_specs = { instance_size = "M10", node_count = 3 } }] }]
  lifecycle {
    prevent_destroy = true
    precondition {
      condition     = upper(replace(var.aws_region, "-", "_")) == var.atlas_region
      error_message = "Atlas and the EKS private endpoint must use the same AWS region."
    }
  }
}
resource "mongodbatlas_privatelink_endpoint" "atlas" {
  project_id    = var.atlas_project_id
  provider_name = "AWS"
  region        = var.atlas_region
}
resource "aws_security_group" "atlas_endpoint" {
  name_prefix = "${var.name}-atlas-"
  vpc_id      = var.vpc_id
  description = "Private Atlas endpoint for enrolled application security groups"
}
resource "aws_vpc_security_group_ingress_rule" "atlas_client" {
  for_each                     = var.client_security_group_ids
  security_group_id            = aws_security_group.atlas_endpoint.id
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  # Atlas PrivateLink uses mapped ports, not only MongoDB's conventional 27017.
  from_port = 1024
  to_port   = 65535
}
resource "aws_vpc_endpoint" "atlas" {
  vpc_id              = var.vpc_id
  vpc_endpoint_type   = "Interface"
  service_name        = mongodbatlas_privatelink_endpoint.atlas.endpoint_service_name
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.atlas_endpoint.id]
  private_dns_enabled = false
}
resource "mongodbatlas_privatelink_endpoint_service" "atlas" {
  project_id          = var.atlas_project_id
  private_link_id     = mongodbatlas_privatelink_endpoint.atlas.private_link_id
  endpoint_service_id = aws_vpc_endpoint.atlas.id
  provider_name       = "AWS"
}
data "mongodbatlas_advanced_cluster" "private" {
  project_id = var.atlas_project_id
  name       = mongodbatlas_advanced_cluster.projection.name
  depends_on = [mongodbatlas_privatelink_endpoint_service.atlas]
}
locals {
  namespaces   = { for key, t in var.targets : key => "${substr(t.group_namespace, 0, 40)}-${substr(sha256("${var.name}:${var.database}:${t.group_namespace}"), 0, 16)}" }
  private_srvs = [for pe in coalesce(data.mongodbatlas_advanced_cluster.private.connection_strings.private_endpoint, []) : pe.srv_connection_string if contains([for endpoint in pe.endpoints : endpoint.endpoint_id], aws_vpc_endpoint.atlas.id)]
  private_srv  = one(local.private_srvs)
  topic_prefix = replace(var.msk_cluster_arn, ":cluster/", ":topic/")
  group_prefix = replace(var.msk_cluster_arn, ":cluster/", ":group/")
  topics       = { workflow = "order.events.v1", journal = "ledger.journals.v1" }
  collections = { for key, target in var.targets : key => concat(
    target.stream == "workflow" ? ["workflow_${target.mode}_${target.generation}", "workflow_inbox_${target.mode}_${target.generation}", "workflow_history_${target.mode}_${target.generation}"] : ["journals_${target.mode}_${target.generation}", "journal_accounts_${target.mode}_${target.generation}", "journal_inbox_${target.mode}_${target.generation}"],
    ["queue_${target.stream}_${target.mode}_${target.generation}", "queue_quota_${target.stream}_${target.mode}_${target.generation}"]
  ) }
}
resource "aws_msk_topic" "lifecycle" {
  for_each           = local.topics
  name               = each.value
  cluster_arn        = var.msk_cluster_arn
  partition_count    = 24
  replication_factor = 3
  configs            = jsonencode({ "cleanup.policy" = "delete", "retention.ms" = "2592000000", "min.insync.replicas" = "2" })
  lifecycle { prevent_destroy = true }
}
resource "aws_iam_role" "projection" {
  for_each           = var.targets
  name               = "${var.name}-${each.key}"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["sts:AssumeRole", "sts:TagSession"], Principal = { Service = "pods.eks.amazonaws.com" }, Condition = { StringEquals = { "aws:SourceAccount" = split(":", var.msk_cluster_arn)[4], "aws:RequestTag/kubernetes-namespace" = "praxis", "aws:RequestTag/kubernetes-service-account" = each.value.service_account, "aws:RequestTag/eks-cluster-name" = var.eks_cluster_name } } }] })
}
resource "aws_eks_pod_identity_association" "projection" {
  for_each        = var.targets
  cluster_name    = var.eks_cluster_name
  namespace       = "praxis"
  service_account = each.value.service_account
  role_arn        = aws_iam_role.projection[each.key].arn
}
resource "random_password" "atlas" {
  for_each = var.targets
  length   = 40
  special  = false
}
resource "mongodbatlas_custom_db_role" "runtime" {
  for_each   = var.targets
  project_id = var.atlas_project_id
  role_name  = "${var.name}-${each.key}"
  dynamic "actions" {
    for_each = toset(["FIND", "INSERT", "UPDATE", "LIST_INDEXES"])
    content {
      action = actions.value
      dynamic "resources" {
        for_each = local.collections[each.key]
        content {
          database_name   = var.database
          collection_name = resources.value
        }
      }
    }
  }
  actions {
    action = "LIST_COLLECTIONS"
    resources {
      database_name   = var.database
      collection_name = ""
    }
  }
  actions {
    action = "FIND"
    dynamic "resources" {
      for_each = ["projection_routes", "projection_reports"]
      content {
        database_name   = var.database
        collection_name = resources.value
      }
    }
  }
}
resource "mongodbatlas_database_user" "runtime" {
  for_each           = var.targets
  project_id         = var.atlas_project_id
  username           = "${var.name}-${each.key}"
  password           = random_password.atlas[each.key].result
  auth_database_name = "admin"
  roles {
    role_name     = mongodbatlas_custom_db_role.runtime[each.key].role_name
    database_name = "admin"
  }
  scopes {
    name = mongodbatlas_advanced_cluster.projection.name
    type = "CLUSTER"
  }
}
resource "aws_secretsmanager_secret" "atlas" {
  for_each                = var.targets
  name                    = "${var.name}/${each.key}/atlas"
  recovery_window_in_days = 30
}
resource "aws_secretsmanager_secret_version" "atlas" {
  for_each      = var.targets
  secret_id     = aws_secretsmanager_secret.atlas[each.key].id
  secret_string = jsonencode({ uri = replace(coalesce(local.private_srv, "missing"), "mongodb+srv://", "mongodb+srv://${urlencode(mongodbatlas_database_user.runtime[each.key].username)}:${urlencode(random_password.atlas[each.key].result)}@") })
  lifecycle {
    precondition {
      condition     = local.private_srv != null && startswith(coalesce(local.private_srv, "missing"), "mongodb+srv://")
      error_message = "The Atlas private endpoint SRV URI must exist. Public URI fallback is prohibited."
    }
  }
}
resource "aws_iam_role_policy" "runtime" {
  for_each = var.targets
  name     = "projection-runtime"
  role     = aws_iam_role.projection[each.key].name
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["kafka-cluster:Connect"], Resource = [var.msk_cluster_arn] },
    { Effect = "Allow", Action = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"], Resource = ["${local.topic_prefix}/${local.topics[each.value.stream]}"] },
    { Effect = "Allow", Action = ["kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"], Resource = ["${local.group_prefix}/projection-${each.value.stream}-${local.namespaces[each.key]}.${each.value.mode}.${each.value.generation}"] },
    { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [aws_secretsmanager_secret.atlas[each.key].arn] }
  ] })
}
resource "aws_iam_role_policy" "publisher" {
  for_each = { order = { role = var.publisher_role_names.order != "" ? var.publisher_role_names.order : aws_iam_role.order_publisher[0].name, topic = local.topics.workflow }, ledger = { role = var.publisher_role_names.ledger, topic = local.topics.journal } }
  name     = "projection-lifecycle-publication"
  role     = each.value.role
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["kafka-cluster:Connect"], Resource = [var.msk_cluster_arn] },
    { Effect = "Allow", Action = ["kafka-cluster:DescribeTopic", "kafka-cluster:WriteData"], Resource = ["${local.topic_prefix}/${each.value.topic}"] }
  ] })
}
output "targets" {
  value = { for key, target in var.targets : key => { service_account = target.service_account, role_arn = aws_iam_role.projection[key].arn, mongo_uri_secret_arn = aws_secretsmanager_secret.atlas[key].arn, stream = target.stream, mode = target.mode, generation = target.generation, group_namespace = local.namespaces[key] } }
}
output "private_endpoint_id" { value = aws_vpc_endpoint.atlas.id }

variable "kafka_brokers" { type = list(string) }
variable "dependency_cidrs" {
  type = list(string)
  validation {
    condition     = length(var.dependency_cidrs) > 0 && alltrue([for c in var.dependency_cidrs : can(cidrnetmask(c)) && ((can(regex("^10\\.", c)) && try(tonumber(split("/", c)[1]), 0) >= 8) || (can(regex("^172\\.(1[6-9]|2[0-9]|3[0-1])\\.", c)) && try(tonumber(split("/", c)[1]), 0) >= 12) || (can(regex("^192\\.168\\.", c)) && try(tonumber(split("/", c)[1]), 0) >= 16))])
    error_message = "Dependency destinations must be explicit RFC1918 IPv4 CIDRs, never public ranges."
  }
}
variable "read_client_namespace" {
  type    = string
  default = "gateway"
}
variable "image" {
  type = string
  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[a-f0-9]{64}$", var.image))
    error_message = "Use a projection image pinned by SHA-256 digest."
  }
}
variable "jwt_issuer" {
  type    = string
  default = ""
}
variable "jwt_audience" {
  type    = string
  default = ""
}
variable "jwt_key_id" {
  type    = string
  default = ""
}
variable "reads_enabled" {
  type    = bool
  default = false
}
variable "operator_role_name" { type = string }
resource "random_password" "operator" {
  length  = 40
  special = false
}
resource "mongodbatlas_database_user" "operator" {
  project_id         = var.atlas_project_id
  username           = "${var.name}-operator"
  password           = random_password.operator.result
  auth_database_name = "admin"
  roles {
    role_name     = "readWrite"
    database_name = var.database
  }
  roles {
    role_name     = "dbAdmin"
    database_name = var.database
  }
  scopes {
    name = mongodbatlas_advanced_cluster.projection.name
    type = "CLUSTER"
  }
}
resource "aws_secretsmanager_secret" "operator" {
  name                    = "${var.name}/operator/atlas"
  recovery_window_in_days = 30
}
resource "aws_secretsmanager_secret_version" "operator" {
  secret_id     = aws_secretsmanager_secret.operator.id
  secret_string = jsonencode({ uri = replace(coalesce(local.private_srv, "missing"), "mongodb+srv://", "mongodb+srv://${urlencode(mongodbatlas_database_user.operator.username)}:${urlencode(random_password.operator.result)}@") })
  lifecycle {
    precondition {
      condition     = local.private_srv != null && startswith(coalesce(local.private_srv, "missing"), "mongodb+srv://")
      error_message = "Private Atlas SRV URI is required."
    }
  }
}
resource "aws_iam_role_policy" "operator" {
  name   = "projection-atlas-operator"
  role   = var.operator_role_name
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [aws_secretsmanager_secret.operator.arn] }] })
}
output "operator_uri_secret_arn" { value = aws_secretsmanager_secret.operator.arn }
output "workload_manifests" {
  value = { for key, target in var.targets : key => templatefile("${path.module}/workload.yaml.tftpl", {
    NAME                  = "${var.name}-${key}"
    SERVICE_ACCOUNT       = target.service_account
    STREAM                = target.stream
    MODE                  = target.mode
    GENERATION            = target.generation
    GROUP_NAMESPACE       = local.namespaces[key]
    DATABASE              = var.database
    SECRET_ARN            = aws_secretsmanager_secret.atlas[key].arn
    SECRETS_ENDPOINT      = "https://${aws_vpc_endpoint.secrets.dns_entry[0].dns_name}"
    AWS_REGION            = var.aws_region
    BROKERS               = join(",", var.kafka_brokers)
    IMAGE                 = var.image
    READ_CLIENT_NAMESPACE = var.read_client_namespace
    DEPENDENCY_CIDRS      = var.dependency_cidrs
    READS_ENABLED         = tostring(var.reads_enabled)
    JWT_ISSUER            = var.jwt_issuer
    JWT_AUDIENCE          = var.jwt_audience
    JWT_KEY_ID            = var.jwt_key_id
  }) }
}

resource "aws_iam_role" "order_publisher" {
  count              = var.publisher_role_names.order == "" ? 1 : 0
  name               = "${var.name}-order-publisher"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = ["sts:AssumeRole", "sts:TagSession"], Principal = { Service = "pods.eks.amazonaws.com" }, Condition = { StringEquals = { "aws:SourceAccount" = split(":", var.msk_cluster_arn)[4], "aws:RequestTag/kubernetes-namespace" = "praxis", "aws:RequestTag/kubernetes-service-account" = "order", "aws:RequestTag/eks-cluster-name" = var.eks_cluster_name } } }] })
}
resource "aws_eks_pod_identity_association" "order_publisher" {
  count           = var.publisher_role_names.order == "" ? 1 : 0
  cluster_name    = var.eks_cluster_name
  namespace       = "praxis"
  service_account = "order"
  role_arn        = aws_iam_role.order_publisher[0].arn
}

# A dedicated, explicitly addressed endpoint avoids changing VPC-wide DNS or
# endpoint policy behavior for existing financial workloads.
resource "aws_security_group" "secrets_endpoint" {
  name_prefix = "${var.name}-secrets-"
  vpc_id      = var.vpc_id
  description = "Private projection credential retrieval"
}
resource "aws_vpc_security_group_ingress_rule" "secrets_client" {
  for_each                     = var.client_security_group_ids
  security_group_id            = aws_security_group.secrets_endpoint.id
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}
resource "aws_vpc_endpoint" "secrets" {
  vpc_id              = var.vpc_id
  vpc_endpoint_type   = "Interface"
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  subnet_ids          = var.private_subnet_ids
  security_group_ids  = [aws_security_group.secrets_endpoint.id]
  private_dns_enabled = false
}
output "secrets_manager_endpoint" { value = "https://${aws_vpc_endpoint.secrets.dns_entry[0].dns_name}" }
