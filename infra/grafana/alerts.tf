# Grafana-managed copies of the AMP thresholds provide notification delivery.
# The AMP rule namespace remains useful for AMP-side evaluation but has no receiver.
locals {
  alert_rules = {
    order_error = {
      uid       = "praxis-order-error"
      title     = "Order high error rate"
      service   = "order"
      expr      = "sum(rate(order_failed_total[5m])) / clamp_min(sum(rate(order_requests_total[5m])), 0.001)"
      threshold = 0.05
      duration  = "10m"
    }
    order_latency = {
      uid       = "praxis-order-latency"
      title     = "Order high p95 latency"
      service   = "order"
      expr      = "histogram_quantile(0.95, sum by (le) (rate(order_total_duration_seconds_bucket[5m])))"
      threshold = 1
      duration  = "10m"
    }
    ledger_error = {
      uid       = "praxis-ledger-error"
      title     = "Ledger high error rate"
      service   = "ledger"
      expr      = "sum(rate(ledger_reserve_failures_total[5m])) / clamp_min(sum(rate(ledger_reserve_requests_total[5m])), 0.001)"
      threshold = 0.02
      duration  = "10m"
    }
    matching_error = {
      uid       = "praxis-matching-error"
      title     = "Matching high error rate"
      service   = "matching"
      expr      = "sum(rate(matching_orders_failed_total[5m])) / clamp_min(sum(rate(matching_orders_submitted_total[5m])), 0.001)"
      threshold = 0.02
      duration  = "10m"
    }
    outbox_failure = {
      uid       = "praxis-outbox-failure"
      title     = "Outbox publish failures"
      service   = "outbox"
      expr      = "sum(rate(outbox_events_failed_total[5m]))"
      threshold = 0
      duration  = "5m"
    }
  }
}

resource "grafana_rule_group" "praxis_red" {
  name             = "Praxis RED"
  folder_uid       = grafana_folder.praxis.uid
  interval_seconds = 60

  dynamic "rule" {
    for_each = local.alert_rules
    content {
      name           = rule.value.title
      uid            = rule.value.uid
      condition      = "B"
      for            = rule.value.duration
      no_data_state  = "NoData"
      exec_err_state = "Error"
      is_paused      = false
      labels         = { severity = "page", service = rule.value.service }
      annotations    = { summary = rule.value.title }

      data {
        ref_id         = "A"
        datasource_uid = grafana_data_source.amp.uid
        relative_time_range {
          from = 600
          to   = 0
        }
        model = jsonencode({
          datasource    = { type = grafana_data_source.amp.type, uid = grafana_data_source.amp.uid }
          editorMode    = "code"
          expr          = rule.value.expr
          instant       = true
          intervalMs    = 1000
          maxDataPoints = 43200
          refId         = "A"
        })
      }

      data {
        ref_id         = "B"
        datasource_uid = "-100"
        relative_time_range {
          from = 0
          to   = 0
        }
        model = jsonencode({
          conditions = [{
            evaluator = { params = [rule.value.threshold], type = "gt" }
            operator  = { type = "and" }
            query     = { params = ["A"] }
            reducer   = { params = [], type = "last" }
            type      = "query"
          }]
          datasource    = { type = "__expr__", uid = "-100" }
          hide          = false
          intervalMs    = 1000
          maxDataPoints = 43200
          refId         = "B"
          type          = "classic_conditions"
        })
      }

      notification_settings {
        contact_point = grafana_contact_point.email.name
      }
    }
  }
}
