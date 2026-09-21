resource "aws_prometheus_workspace" "application" {
  alias = "${local.resource_name}-application"
  # AMP adds this tag when a managed scraper targets the workspace.
  tags = { AMPAgentlessScraper = "" }
}

resource "aws_prometheus_workspace_configuration" "application" {
  workspace_id             = aws_prometheus_workspace.application.id
  retention_period_in_days = var.managed_metrics_retention_days
}

data "aws_iam_policy_document" "managed_metrics_write" {
  statement {
    sid       = "WriteApplicationMetrics"
    actions   = ["aps:RemoteWrite"]
    resources = [aws_prometheus_workspace.application.arn]
  }
}

resource "aws_iam_role_policy" "managed_metrics_write" {
  for_each = local.trace_enabled_services

  name   = "managed-prometheus-remote-write"
  role   = aws_iam_role.service_task[local.trace_service_task_role_keys[each.key]].id
  policy = data.aws_iam_policy_document.managed_metrics_write.json
}

# The managed scraper runs outside the cluster, with private VPC interfaces.
# Keep its access separate from the EKS node and ECS task identities.
resource "aws_security_group" "amp_eks_scraper" {
  name        = "${local.resource_name}-amp-eks-scraper"
  description = "AMP managed scraper access to the EKS API and Praxis metrics pods"
  vpc_id      = aws_vpc.cex.id
}

locals {
  amp_eks_scrape_ports = toset(["443", "8081", "8083", "8084"])
}

resource "aws_vpc_security_group_egress_rule" "amp_eks_scraper" {
  for_each = local.amp_eks_scrape_ports

  security_group_id            = aws_security_group.amp_eks_scraper.id
  referenced_security_group_id = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
  description                  = "Scraper to EKS API or Praxis metrics port ${each.value}"
  ip_protocol                  = "tcp"
  from_port                    = tonumber(each.value)
  to_port                      = tonumber(each.value)
}

# EKS managed nodes use the cluster security group for pod VPC traffic.
resource "aws_vpc_security_group_ingress_rule" "amp_eks_scraper" {
  for_each = local.amp_eks_scrape_ports

  security_group_id            = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
  referenced_security_group_id = aws_security_group.amp_eks_scraper.id
  description                  = "AMP scraper to EKS API or Praxis metrics port ${each.value}"
  ip_protocol                  = "tcp"
  from_port                    = tonumber(each.value)
  to_port                      = tonumber(each.value)
}

resource "aws_prometheus_scraper" "eks_hot_path" {
  alias = "${local.resource_name}-eks-hot-path"

  source {
    eks {
      cluster_arn        = aws_eks_cluster.this.arn
      subnet_ids         = aws_subnet.private[*].id
      security_group_ids = [aws_security_group.amp_eks_scraper.id]
    }
  }

  destination {
    amp {
      workspace_arn = aws_prometheus_workspace.application.arn
    }
  }

  scrape_configuration = yamlencode({
    global = { scrape_interval = "30s", scrape_timeout = "10s" }
    scrape_configs = [{
      job_name              = "praxis-eks-hot-path"
      metrics_path          = "/metrics"
      sample_limit          = 1000
      kubernetes_sd_configs = [{ role = "pod" }]
      relabel_configs = [
        { action = "keep", source_labels = ["__meta_kubernetes_namespace"], regex = "praxis" },
        { action = "keep", source_labels = ["__meta_kubernetes_pod_label_app"], regex = "order|ledger|matching" },
        { action = "keep", source_labels = ["__meta_kubernetes_pod_container_port_name"], regex = "http" },
        { action = "replace", source_labels = ["__meta_kubernetes_namespace"], target_label = "namespace" },
        { action = "replace", source_labels = ["__meta_kubernetes_pod_label_app"], target_label = "service" },
        { action = "replace", source_labels = ["__meta_kubernetes_pod_name"], target_label = "pod" },
        { action = "replace", target_label = "environment", replacement = var.environment }
      ]
    }]
  })

  depends_on = [aws_vpc_security_group_egress_rule.amp_eks_scraper, aws_vpc_security_group_ingress_rule.amp_eks_scraper]
}
