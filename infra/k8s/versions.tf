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
