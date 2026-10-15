locals {
  mesh_workloads = toset(["order", "ledger", "matching"])
  mesh_backends  = toset(["ledger", "matching"])
  mesh_ports = {
    ledger   = 9091
    matching = 9092
  }
  mesh_admin_ports = {
    order    = 8083
    ledger   = 8081
    matching = 8084
  }
}

# Istio is installed and version-pinned outside this stack. Keeping enrollment
# behind one flag preserves a clean rollback boundary while the mesh is being
# accepted in non-production.
resource "kubernetes_manifest" "mesh_strict_mtls" {
  for_each = var.deploy_workloads && var.enable_istio_mesh ? local.mesh_workloads : toset([])

  manifest = {
    apiVersion = "security.istio.io/v1"
    kind       = "PeerAuthentication"
    metadata   = { name = "${each.key}-strict-mtls", namespace = "praxis" }
    spec = {
      selector = { matchLabels = { app = each.key } }
      mtls     = { mode = "STRICT" }
      portLevelMtls = {
        tostring(local.mesh_admin_ports[each.key]) = { mode = "DISABLE" }
      }
    }
  }

  depends_on = [kubernetes_manifest.deployments]
}

# Ledger and Matching accept application traffic only from Order's service
# identity. Kubelet probes are rewritten through the sidecar by Istio and do
# not traverse these gRPC ports.
resource "kubernetes_manifest" "mesh_backend_authorization" {
  for_each = var.deploy_workloads && var.enable_istio_mesh ? local.mesh_backends : toset([])

  manifest = {
    apiVersion = "security.istio.io/v1"
    kind       = "AuthorizationPolicy"
    metadata   = { name = "${each.key}-from-order", namespace = "praxis" }
    spec = {
      selector = { matchLabels = { app = each.key } }
      action   = "ALLOW"
      rules = [
        {
          from = [{ source = { principals = ["${var.istio_trust_domain}/ns/praxis/sa/order"] } }]
          to   = [{ operation = { ports = [tostring(local.mesh_ports[each.key])] } }]
        },
        {
          # Preserve kubelet probes and the node-local collector scrape. These
          # ports expose health and Prometheus data, never financial mutation.
          to = [{ operation = { ports = [tostring(local.mesh_admin_ports[each.key])] } }]
        }
      ]
    }
  }

  depends_on = [kubernetes_manifest.mesh_strict_mtls]
}

# Per-request endpoint selection fixes the long-lived HTTP/2 connection skew.
# No retry policy is configured: financial retries remain application-owned.
resource "kubernetes_manifest" "mesh_backend_traffic" {
  for_each = var.deploy_workloads && var.enable_istio_mesh ? local.mesh_backends : toset([])

  manifest = {
    apiVersion = "networking.istio.io/v1"
    kind       = "DestinationRule"
    metadata   = { name = each.key, namespace = "praxis" }
    spec = {
      host = "${each.key}.praxis.svc.cluster.local"
      trafficPolicy = {
        loadBalancer = { simple = "ROUND_ROBIN" }
        connectionPool = {
          tcp = { maxConnections = var.mesh_backend_connection_limits[each.key].max_connections, connectTimeout = "3s" }
          http = {
            http1MaxPendingRequests  = var.mesh_backend_connection_limits[each.key].max_pending_requests
            http2MaxRequests         = var.mesh_backend_connection_limits[each.key].max_active_requests
            maxRequestsPerConnection = 0
            maxRetries               = 0
          }
        }
      }
    }
  }

  depends_on = [kubernetes_manifest.deployments]
}

# Log only failed requests and connections without an HTTP/gRPC response. The
# built-in envoy provider writes to the proxy container's stdout, which the
# existing CloudWatch Observability add-on already collects. The filter field
# remains v1alpha1 in Istio even though the rest of Telemetry has graduated.
resource "kubernetes_manifest" "mesh_error_access_logs" {
  for_each = var.deploy_workloads && var.enable_istio_mesh ? local.mesh_workloads : toset([])

  manifest = {
    apiVersion = "telemetry.istio.io/v1alpha1"
    kind       = "Telemetry"
    metadata   = { name = "${each.key}-error-access-logs", namespace = "praxis" }
    spec = {
      selector = { matchLabels = { app = each.key } }
      accessLogging = [{
        providers = [{ name = "envoy" }]
        filter    = { expression = "!has(response.code) || response.code >= 400" }
      }]
    }
  }

  depends_on = [kubernetes_manifest.deployments]
}
