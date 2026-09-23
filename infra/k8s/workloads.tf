locals {
  aws_outputs     = data.terraform_remote_state.aws.outputs
  runtime         = local.aws_outputs.eks_runtime_config
  images          = local.aws_outputs.eks_image_refs
  migration       = local.aws_outputs.ledger_migration_config
  migration_image = coalesce(local.migration.image, "missing-ledger-migration-image")

  runtime_config_data = {
    AWS_REGION                  = local.runtime.aws_region
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://otel-collector.observability.svc.cluster.local:4317"
    OTEL_TRACE_SAMPLE_RATIO     = tostring(local.runtime.trace_sample_ratio)
    OTEL_RESOURCE_ATTRIBUTES    = "deployment.environment=${local.runtime.environment},service.namespace=praxis,k8s.cluster.name=${local.runtime.eks_cluster_name}"
    LEDGER_DB_WRITER_HOST       = local.runtime.ledger_db_writer_host
    LEDGER_DB_READER_HOST       = local.runtime.ledger_db_reader_host
    LEDGER_DB_NAME              = local.runtime.ledger_db_name
    LEDGER_DB_SECRET_ARN        = local.runtime.ledger_runtime_secret_arn
    MSK_BROKERS                 = local.runtime.bootstrap_brokers_iam
    LEDGER_COMMANDS_TOPIC       = local.runtime.ledger_commands_topic
    LEDGER_CONSUMER_GROUP       = local.runtime.ledger_consumer_group
    MATCHING_EVENTS_TOPIC       = local.runtime.matching_events_topic
  }

  workload_documents = [
    for document in split("\n---\n", templatefile("${path.module}/workloads.yaml", {
      ORDER_IMAGE         = coalesce(local.images.order, "missing-order-image")
      LEDGER_IMAGE        = coalesce(local.images.ledger, "missing-ledger-image")
      MATCHING_IMAGE      = coalesce(local.images.matching, "missing-matching-image")
      RUNTIME_CONFIG_HASH = sha256(jsonencode(local.runtime_config_data))
    })) : yamldecode(document)
  ]
  services    = { for document in local.workload_documents : document.metadata.name => document if document.kind == "Service" }
  deployments = { for document in local.workload_documents : document.metadata.name => document if document.kind == "Deployment" }
}

resource "kubernetes_namespace_v1" "praxis" {
  metadata {
    name = "praxis"
    labels = {
      "pod-security.kubernetes.io/enforce" = "restricted"
      "pod-security.kubernetes.io/audit"   = "restricted"
      "pod-security.kubernetes.io/warn"    = "restricted"
    }
  }
}

resource "kubernetes_service_account_v1" "workloads" {
  for_each = toset(["order", "ledger", "matching", "ledger-migration"])

  metadata {
    name      = each.key
    namespace = kubernetes_namespace_v1.praxis.metadata[0].name
  }
  automount_service_account_token = each.key == "order" ? false : true
}

resource "kubernetes_config_map_v1" "runtime" {
  count = var.deploy_workloads ? 1 : 0

  metadata {
    name      = "praxis-runtime"
    namespace = kubernetes_namespace_v1.praxis.metadata[0].name
  }

  data = local.runtime_config_data

  lifecycle {
    precondition {
      condition     = local.runtime.ledger_runtime_secret_arn != ""
      error_message = "Set ledger_runtime_secret_arn in infra/aws before deploying Kubernetes workloads."
    }
  }
}

resource "kubernetes_manifest" "services" {
  for_each = var.deploy_workloads ? local.services : {}
  manifest = each.value

  depends_on = [kubernetes_namespace_v1.praxis]
}

# A digest-specific name makes a new Ledger image run its migration once.
# The database advisory lock serializes it with the independent Outbox migration.
resource "kubernetes_manifest" "ledger_migration" {
  manifest = {
    apiVersion = "batch/v1"
    kind       = "Job"
    metadata = {
      name      = "ledger-migration-${substr(sha256(local.migration_image), 0, 40)}"
      namespace = "praxis"
    }
    spec = {
      backoffLimit          = 0
      activeDeadlineSeconds = 900
      template = {
        spec = {
          restartPolicy      = "Never"
          serviceAccountName = "ledger-migration"
          nodeSelector       = { "praxis.io/workload" = "hot-path" }
          securityContext = {
            runAsNonRoot   = true
            runAsUser      = 65532
            seccompProfile = { type = "RuntimeDefault" }
          }
          containers = [{
            name            = "ledger-migration"
            image           = local.migration_image
            imagePullPolicy = "IfNotPresent"
            args            = ["migrate"]
            securityContext = {
              allowPrivilegeEscalation = false
              readOnlyRootFilesystem   = true
              capabilities             = { drop = ["ALL"] }
            }
            env = [
              { name = "AWS_REGION", value = local.migration.aws_region },
              { name = "LEDGER_DB_WRITER_HOST", value = local.migration.db_writer_host },
              { name = "LEDGER_DB_USER", value = local.migration.db_user },
              { name = "LEDGER_DB_NAME", value = local.migration.db_name },
              { name = "LEDGER_DB_SECRET_ARN", value = local.migration.secret_arn }
            ]
            resources = {
              requests = { cpu = "250m", memory = "256Mi" }
              limits   = { cpu = "1", memory = "1Gi" }
            }
          }]
        }
      }
    }
  }

  wait {
    fields = { "status.succeeded" = "^1$" }
  }

  timeouts {
    create = "16m"
    update = "16m"
  }

  depends_on = [kubernetes_service_account_v1.workloads]

  lifecycle {
    precondition {
      condition     = can(regex("@sha256:[0-9a-f]{64}$", local.migration_image))
      error_message = "Set ledger_migration_image_digest in infra/aws and apply that stack before deploying Kubernetes workloads."
    }
  }
}

resource "kubernetes_manifest" "deployments" {
  for_each = var.deploy_workloads ? local.deployments : {}
  manifest = each.value

  wait {
    rollout = true
  }

  timeouts {
    create = "10m"
    update = "10m"
  }

  depends_on = [
    kubernetes_config_map_v1.runtime,
    kubernetes_service_account_v1.workloads,
    kubernetes_manifest.services,
    kubernetes_manifest.ledger_migration,
    kubernetes_manifest.collector_deployment
  ]

  lifecycle {
    precondition {
      condition = alltrue([
        for image in [local.images.order, local.images.ledger, local.images.matching] :
        can(regex("@sha256:[0-9a-f]{64}$", image))
      ])
      error_message = "Set digest-pinned Order, Ledger, and Matching images in infra/aws before deploying Kubernetes workloads."
    }
  }
}
