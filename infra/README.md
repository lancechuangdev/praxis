# Infrastructure map

There are three independent Terraform root modules. Apply them in this order:

| Directory | Owns | Reads from |
|---|---|---|
| [`aws/`](aws/README.md) | VPC, MSK, PostgreSQL, ECR, EKS/EC2, ECS/Fargate, IAM, AMP, Managed Grafana workspace | AWS credentials, IAM Identity Center, and `terraform.tfvars` |
| [`k8s/`](k8s/README.md) | EKS namespaces, migration Job, Order/Ledger/Matching Deployments, shared Collector | Outputs from `aws/` and the private EKS API |
| [`grafana/`](grafana/README.md) | AMP data source, RED dashboard, email alert rules | AWS stack outputs, Grafana API token, alert addresses |

Each directory has its own provider configuration and Terraform state. Do not
run Terraform from `infra/` itself. The `.tf` files within a root module are
loaded together; filename order does **not** control creation or apply order.
Resource references and `depends_on` express dependencies.

Start with the README in the relevant directory for prerequisites and apply
commands. `aws/` provisions cloud resources, `k8s/` configures workloads on
the provisioned EKS cluster, and `grafana/` configures the Grafana workspace
created by `aws/`.
