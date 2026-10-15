terraform {
  required_version = ">= 1.8.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.38.0, < 3.0.0"
    }
  }
}

variable "aws_state_path" {
  description = "Path to the already-applied infra/aws local Terraform state."
  type        = string
  default     = null
}

variable "deploy_workloads" {
  description = "Deploy Order, Ledger, and Matching. Set false only for the initial Ledger schema migration before runtime database roles exist."
  type        = bool
  default     = true
}

variable "enable_istio_mesh" {
  description = "Enroll Order, Ledger, and Matching in a pre-installed Istio sidecar mesh and apply the Phase 5 security and traffic policies."
  type        = bool
  default     = false
}

variable "mesh_enrolled_workloads" {
  description = "Workloads to enroll when the mesh is enabled. Enroll Order first, then one backend at a time; remove backends before Order during rollback."
  type        = set(string)
  default     = ["order", "ledger", "matching"]

  validation {
    condition = (
      length(setsubtract(var.mesh_enrolled_workloads, toset(["order", "ledger", "matching"]))) == 0 &&
      (length(var.mesh_enrolled_workloads) == 0 || contains(var.mesh_enrolled_workloads, "order"))
    )
    error_message = "Enroll only Order, Ledger, and Matching; any nonempty enrollment must include Order so strict backend mTLS retains its caller."
  }
}

output "mesh_enrolled_workloads" {
  value = var.enable_istio_mesh && var.deploy_workloads ? sort(tolist(var.mesh_enrolled_workloads)) : []
}

variable "mesh_backend_connection_limits" {
  description = "Per-caller-proxy connection, pending request, and active HTTP/2 request bounds for each backend; tune from measured capacity, not fleet-wide quotas."
  type = map(object({
    max_connections      = number
    max_pending_requests = number
    max_active_requests  = number
  }))
  default = {
    ledger   = { max_connections = 64, max_pending_requests = 64, max_active_requests = 256 }
    matching = { max_connections = 64, max_pending_requests = 64, max_active_requests = 256 }
  }

  validation {
    condition = (
      toset(keys(var.mesh_backend_connection_limits)) == toset(["ledger", "matching"]) &&
      alltrue(flatten([for limits in values(var.mesh_backend_connection_limits) : [
        for value in values(limits) : try(value >= 1 && value <= 2147483647 && floor(value) == value, false)
      ]]))
    )
    error_message = "Provide exactly ledger and matching, with positive integer limits no greater than 2147483647; zero/unlimited values are prohibited."
  }
}

output "mesh_backend_connection_limits" {
  description = "Applied backend pool limits used by the Phase 5 live preflight."
  value       = var.mesh_backend_connection_limits
}

variable "istio_trust_domain" {
  description = "Istio SPIFFE trust domain used in workload authorization principals."
  type        = string
  default     = "cluster.local"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]*[a-z0-9]$", var.istio_trust_domain))
    error_message = "istio_trust_domain must be a lowercase DNS-style trust domain."
  }
}

data "terraform_remote_state" "aws" {
  backend = "local"
  config = {
    path = var.aws_state_path == null ? "${path.module}/../aws/terraform.tfstate" : var.aws_state_path
  }
}

provider "kubernetes" {
  host                   = data.terraform_remote_state.aws.outputs.eks_cluster_endpoint
  cluster_ca_certificate = base64decode(data.terraform_remote_state.aws.outputs.eks_cluster_ca_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args = [
      "eks", "get-token",
      "--cluster-name", data.terraform_remote_state.aws.outputs.eks_cluster_name,
      "--region", data.terraform_remote_state.aws.outputs.eks_runtime_config.aws_region
    ]
  }
}
