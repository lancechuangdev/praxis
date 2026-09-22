terraform {
  required_version = ">= 1.5.0"

  required_providers {
    grafana = {
      source  = "grafana/grafana"
      version = "~> 4.0"
    }
  }
}

# Set GRAFANA_URL and GRAFANA_AUTH (a service-account token) in the environment.
# Do not put the token in tfvars or Terraform state.
provider "grafana" {}
