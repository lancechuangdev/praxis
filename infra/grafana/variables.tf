variable "amp_endpoint" {
  description = "AMP workspace Prometheus endpoint from infra/aws output managed_metrics_query_endpoint."
  type        = string
  validation {
    condition     = can(regex("^https://aps-workspaces[.].+/workspaces/.+", var.amp_endpoint))
    error_message = "amp_endpoint must be an AMP workspace Prometheus endpoint."
  }
}

variable "aws_region" {
  description = "AWS Region of the AMP workspace."
  type        = string
}

variable "alert_email_addresses" {
  description = "Email recipients for Praxis alerts. Supply real, monitored addresses at apply time."
  type        = list(string)
  validation {
    condition     = length(var.alert_email_addresses) > 0 && alltrue([for address in var.alert_email_addresses : can(regex("^[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+$", address))])
    error_message = "Provide at least one valid alert email address."
  }
}
