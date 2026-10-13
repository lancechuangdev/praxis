resource "aws_prometheus_rule_group_namespace" "application" {
  name         = "praxis-application"
  workspace_id = aws_prometheus_workspace.application.id
  data = yamlencode({
    groups = [
      {
        name     = "praxis-red"
        interval = "1m"
        rules = [
          {
            alert       = "OrderHighErrorRate"
            expr        = "sum(rate(order_failed_total[5m])) / clamp_min(sum(rate(order_requests_total[5m])), 0.001) > 0.05"
            for         = "10m"
            labels      = { severity = "page", service = "order" }
            annotations = { summary = "Order failure rate exceeds 5%", runbook = "${local.resource_name}: investigate Order, Ledger, and Matching traces" }
          },
          {
            alert       = "OrderHighP95Latency"
            expr        = "histogram_quantile(0.95, sum by (le) (rate(order_total_duration_seconds_bucket[5m]))) > 1"
            for         = "10m"
            labels      = { severity = "page", service = "order" }
            annotations = { summary = "Order p95 latency exceeds one second", runbook = "${local.resource_name}: inspect order stage latency and X-Ray traces" }
          },
          {
            alert       = "LedgerHighErrorRate"
            expr        = "sum(rate(ledger_reserve_failures_total[5m])) / clamp_min(sum(rate(ledger_reserve_requests_total[5m])), 0.001) > 0.02"
            for         = "10m"
            labels      = { severity = "page", service = "ledger" }
            annotations = { summary = "Ledger reservation failure rate exceeds 2%", runbook = "${local.resource_name}: inspect database health and Ledger traces" }
          },
          {
            alert       = "MatchingHighErrorRate"
            expr        = "sum(rate(matching_orders_failed_total[5m])) / clamp_min(sum(rate(matching_orders_submitted_total[5m])), 0.001) > 0.02"
            for         = "10m"
            labels      = { severity = "page", service = "matching" }
            annotations = { summary = "Matching admission failure rate exceeds 2%", runbook = "${local.resource_name}: inspect matching and Kafka traces" }
          },
          {
            alert       = "OutboxPublishFailures"
            expr        = "sum(rate(outbox_events_failed_total[5m])) > 0"
            for         = "5m"
            labels      = { severity = "page", service = "outbox" }
            annotations = { summary = "Outbox publishing has failed for five minutes", runbook = "${local.resource_name}: inspect broker connectivity and relay leases" }
          },
          {
            alert       = "LedgerCommandDeadLettered"
            expr        = "sum(increase(ledger_dlq_handoffs_total[5m])) > 0"
            for         = "0m"
            labels      = { severity = "warning", service = "ledger" }
            annotations = { summary = "A Ledger command was moved to the DLQ", runbook = "${local.resource_name}: inspect the Ledger command DLQ and failure code before replay" }
          },
          {
            alert       = "LedgerDLQHandoffFailures"
            expr        = "sum(rate(ledger_dlq_handoff_failures_total[5m])) > 0"
            for         = "5m"
            labels      = { severity = "page", service = "ledger" }
            annotations = { summary = "Ledger cannot durably hand off poison commands", runbook = "${local.resource_name}: inspect Ledger database, outbox, and consumer logs" }
          }
        ]
      }
    ]
  })
}
