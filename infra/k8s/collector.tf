locals {
  collector_image = coalesce(local.runtime.eks_collector_image, "missing-eks-collector-image")
  collector_config = yamlencode({
    receivers = {
      otlp = { protocols = { grpc = { endpoint = "0.0.0.0:4317" } } }
      prometheus = { config = {
        global = { scrape_interval = "30s", scrape_timeout = "10s" }
        scrape_configs = [{
          job_name     = "praxis-eks-hot-path"
          metrics_path = "/metrics"
          sample_limit = 1000
          kubernetes_sd_configs = [{
            role       = "pod"
            namespaces = { names = ["praxis"] }
          }]
          relabel_configs = [
            { action = "keep", source_labels = ["__meta_kubernetes_pod_label_app"], regex = "order|ledger|matching" },
            { action = "keep", source_labels = ["__meta_kubernetes_pod_container_port_name"], regex = "http" },
            { action = "replace", source_labels = ["__meta_kubernetes_namespace"], target_label = "namespace" },
            { action = "replace", source_labels = ["__meta_kubernetes_pod_label_app"], target_label = "service" },
            { action = "replace", source_labels = ["__meta_kubernetes_pod_name"], target_label = "pod" },
            { action = "replace", target_label = "environment", replacement = local.runtime.environment }
          ]
        }]
      } }
    }
    processors = {
      memory_limiter = { check_interval = "5s", limit_mib = 512, spike_limit_mib = 128 }
      resource = { attributes = [
        { action = "upsert", key = "deployment.environment", value = local.runtime.environment },
        { action = "upsert", key = "k8s.cluster.name", value = local.runtime.eks_cluster_name }
      ] }
      batch = { timeout = "5s", send_batch_size = 128 }
    }
    exporters = {
      awsxray = { region = local.runtime.aws_region }
      prometheusremotewrite = {
        endpoint                         = local.runtime.amp_remote_write_endpoint
        auth                             = { authenticator = "sigv4auth" }
        resource_to_telemetry_conversion = { enabled = true }
      }
    }
    extensions = {
      health_check = { endpoint = "0.0.0.0:13133" }
      sigv4auth    = { service = "aps", region = local.runtime.aws_region }
    }
    service = {
      extensions = ["health_check", "sigv4auth"]
      pipelines = {
        metrics = {
          receivers  = ["prometheus"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["prometheusremotewrite"]
        }
        traces = {
          receivers  = ["otlp"]
          processors = ["memory_limiter", "resource", "batch"]
          exporters  = ["awsxray"]
        }
      }
    }
  })
}

resource "kubernetes_namespace_v1" "observability" {
  metadata {
    name = "observability"
    labels = {
      "pod-security.kubernetes.io/enforce" = "restricted"
      "pod-security.kubernetes.io/audit"   = "restricted"
      "pod-security.kubernetes.io/warn"    = "restricted"
    }
  }
}

resource "kubernetes_service_account_v1" "collector" {
  count = var.deploy_workloads ? 1 : 0

  metadata {
    name      = "otel-collector"
    namespace = kubernetes_namespace_v1.observability.metadata[0].name
  }
}

# The collector watches only Praxis pods to discover their named HTTP ports.
resource "kubernetes_manifest" "collector_pod_role" {
  count = var.deploy_workloads ? 1 : 0

  manifest = {
    apiVersion = "rbac.authorization.k8s.io/v1"
    kind       = "Role"
    metadata   = { name = "otel-collector-pod-discovery", namespace = kubernetes_namespace_v1.praxis.metadata[0].name }
    rules      = [{ apiGroups = [""], resources = ["pods"], verbs = ["get", "list", "watch"] }]
  }
}

resource "kubernetes_manifest" "collector_pod_role_binding" {
  count = var.deploy_workloads ? 1 : 0

  manifest = {
    apiVersion = "rbac.authorization.k8s.io/v1"
    kind       = "RoleBinding"
    metadata   = { name = "otel-collector-pod-discovery", namespace = kubernetes_namespace_v1.praxis.metadata[0].name }
    roleRef    = { apiGroup = "rbac.authorization.k8s.io", kind = "Role", name = "otel-collector-pod-discovery" }
    subjects = [{
      kind      = "ServiceAccount"
      name      = "otel-collector"
      namespace = kubernetes_namespace_v1.observability.metadata[0].name
    }]
  }

  depends_on = [kubernetes_manifest.collector_pod_role, kubernetes_service_account_v1.collector]
}

resource "kubernetes_config_map_v1" "collector" {
  count = var.deploy_workloads ? 1 : 0

  metadata {
    name      = "otel-collector"
    namespace = kubernetes_namespace_v1.observability.metadata[0].name
  }

  data = { "collector.yaml" = local.collector_config }
}

resource "kubernetes_manifest" "collector_service" {
  count = var.deploy_workloads ? 1 : 0

  manifest = {
    apiVersion = "v1"
    kind       = "Service"
    metadata   = { name = "otel-collector", namespace = kubernetes_namespace_v1.observability.metadata[0].name }
    spec = {
      type     = "ClusterIP"
      selector = { app = "otel-collector" }
      ports    = [{ name = "otlp-grpc", port = 4317, targetPort = "otlp-grpc", protocol = "TCP" }]
    }
  }
}

# A single replica avoids duplicate Prometheus scrapes; make this highly
# available with target allocation/deduplication before scaling it up.
resource "kubernetes_manifest" "collector_deployment" {
  count = var.deploy_workloads ? 1 : 0

  manifest = {
    apiVersion = "apps/v1"
    kind       = "Deployment"
    metadata   = { name = "otel-collector", namespace = kubernetes_namespace_v1.observability.metadata[0].name }
    spec = {
      replicas = 1
      strategy = { type = "Recreate" }
      selector = { matchLabels = { app = "otel-collector" } }
      template = {
        metadata = {
          labels      = { app = "otel-collector" }
          annotations = { "checksum/config" = sha256(local.collector_config) }
        }
        spec = {
          serviceAccountName = "otel-collector"
          nodeSelector       = { "praxis.io/workload" = "hot-path" }
          securityContext = {
            runAsNonRoot   = true
            runAsUser      = 65532
            seccompProfile = { type = "RuntimeDefault" }
          }
          containers = [{
            name            = "adot-collector"
            image           = local.collector_image
            imagePullPolicy = "IfNotPresent"
            securityContext = {
              allowPrivilegeEscalation = false
              readOnlyRootFilesystem   = true
              capabilities             = { drop = ["ALL"] }
            }
            env = [
              { name = "AWS_REGION", value = local.runtime.aws_region },
              { name = "AOT_CONFIG_CONTENT", valueFrom = { configMapKeyRef = { name = "otel-collector", key = "collector.yaml" } } }
            ]
            volumeMounts = [{ name = "tmp", mountPath = "/tmp" }]
            ports = [
              { name = "otlp-grpc", containerPort = 4317 },
              { name = "health", containerPort = 13133 }
            ]
            resources = {
              requests = { cpu = "250m", memory = "512Mi" }
              limits   = { cpu = "1", memory = "1Gi" }
            }
            readinessProbe = { httpGet = { path = "/", port = "health" }, periodSeconds = 10 }
            livenessProbe  = { httpGet = { path = "/", port = "health" }, periodSeconds = 20 }
          }]
          volumes = [{ name = "tmp", emptyDir = {} }]
        }
      }
    }
  }

  wait { rollout = true }

  depends_on = [
    kubernetes_service_account_v1.collector,
    kubernetes_config_map_v1.collector,
    kubernetes_manifest.collector_service,
    kubernetes_manifest.collector_pod_role_binding
  ]

  lifecycle {
    precondition {
      condition     = can(regex("@sha256:[0-9a-f]{64}$", local.collector_image))
      error_message = "Set eks_collector_image in infra/aws and apply that stack before deploying Kubernetes workloads."
    }
  }
}
