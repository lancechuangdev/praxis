resource "grafana_data_source" "amp" {
  name        = "Praxis AMP"
  uid         = "praxis-amp"
  type        = "grafana-amazonprometheus-datasource"
  access_mode = "proxy"
  url         = trimsuffix(var.amp_endpoint, "/")

  json_data_encoded = jsonencode({
    httpMethod    = "POST"
    sigV4Auth     = true
    sigV4AuthType = "default"
    sigV4Region   = var.aws_region
    sigv4Service  = "aps"
    manageAlerts  = false
  })
}

resource "grafana_folder" "praxis" {
  title = "Praxis"
  uid   = "praxis"
}

locals {
  dashboard_source = jsondecode(file("${path.module}/../../observability/grafana/dashboards/cex-service-red.json"))
  dashboard = merge(local.dashboard_source, {
    id = null
    panels = [for panel in local.dashboard_source.panels : merge(panel, {
      datasource = { type = grafana_data_source.amp.type, uid = grafana_data_source.amp.uid }
    })]
    templating = { list = [] }
  })
}

resource "grafana_dashboard" "service_red" {
  folder      = grafana_folder.praxis.id
  config_json = jsonencode(local.dashboard)
  overwrite   = true
}

resource "grafana_contact_point" "email" {
  name = "Praxis email"

  email {
    addresses = var.alert_email_addresses
  }
}
