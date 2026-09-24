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

## VPC network

The AWS stack creates one public and one private subnet in each of three
Availability Zones. The internet-facing Order ALB uses all three public
subnets and forwards HTTPS requests to healthy Order NodePort targets in the
same Availability Zone; target-group cross-zone forwarding is disabled. Each
private subnet has its own route table for outbound traffic.

```mermaid
flowchart TB
    Internet((Internet))
    IGW[Internet Gateway]

    subgraph VPC["VPC — var.vpc_cidr"]
        PublicRT["Public route table<br/>0.0.0.0/0 → Internet Gateway"]
        ALB["Order ALB<br/>spans public subnets<br/>HTTPS 443 → HTTP 30083"]

        subgraph AZ1["Availability Zone 1"]
            Pub1["Public subnet 1<br/>NAT Gateway 1"]
            RT1["Private route table 1<br/>0.0.0.0/0 → NAT Gateway 1"]
            Priv1["Private subnet 1<br/>EKS · MSK · PostgreSQL · ECS"]
        end

        subgraph AZ2["Availability Zone 2"]
            Pub2["Public subnet 2<br/>NAT Gateway 2"]
            RT2["Private route table 2<br/>0.0.0.0/0 → NAT Gateway 2"]
            Priv2["Private subnet 2<br/>EKS · MSK · PostgreSQL · ECS"]
        end

        subgraph AZ3["Availability Zone 3"]
            Pub3["Public subnet 3<br/>NAT Gateway 3"]
            RT3["Private route table 3<br/>0.0.0.0/0 → NAT Gateway 3"]
            Priv3["Private subnet 3<br/>EKS · MSK · PostgreSQL · ECS"]
        end

        PublicRT -.-> Pub1 & Pub2 & Pub3
        Priv1 -.-> RT1 --> Pub1
        Priv2 -.-> RT2 --> Pub2
        Priv3 -.-> RT3 --> Pub3
        ALB -.-|"enabled in"| Pub1 & Pub2 & Pub3
        ALB -->|"Order NodePort 30083"| Priv1 & Priv2 & Priv3
    end

    Internet -->|"HTTPS 443"| IGW --> ALB
    PublicRT --> IGW
```

The ALB path is inbound. NAT gateways serve outbound connections initiated
from private subnets. With `nat_gateway_per_az = false`, all private route
tables point to the NAT gateway in public subnet 1.
The dotted lines from the ALB show its placement; its solid arrows show request
flow. AWS manages ALB nodes in the public subnets; the ALB forwards requests
to Order targets on private EKS nodes.

## AZ-aware application topology

The EKS baseline has six nodes and six Order replicas. Hard zone and hostname
spread constraints place two Order replicas on distinct nodes in each AZ. Ledger
runs one replica per AZ. The ALB target group routes within its zone, and the
Order NodePort accepts external traffic only on a node with a local ready Order
Pod. Order-to-Ledger Service traffic prefers the same zone.

```mermaid
flowchart TB
    Internet((Internet))

    subgraph ALB["One logical Order ALB"]
        ALB1["ALB node · AZ1"]
        ALB2["ALB node · AZ2"]
        ALB3["ALB node · AZ3"]
    end

    subgraph AZ1["Availability Zone 1"]
        A["EKS Node A<br/>Order-1"]
        B["EKS Node B<br/>Order-2"]
        L1["Ledger-1<br/>on Node A or B"]
    end

    subgraph AZ2["Availability Zone 2"]
        C["EKS Node C<br/>Order-3"]
        D["EKS Node D<br/>Order-4"]
        L2["Ledger-2<br/>on Node C or D"]
    end

    subgraph AZ3["Availability Zone 3"]
        E["EKS Node E<br/>Order-5"]
        F["EKS Node F<br/>Order-6"]
        L3["Ledger-3<br/>on Node E or F"]
    end

    Matching["Matching pod ×1<br/>scheduler-selected AZ"]
    RDS[("RDS Multi-AZ cluster<br/>role-aware endpoints")]

    Internet --> ALB1 & ALB2 & ALB3
    ALB1 -->|"NodePort 30083 · local"| A & B
    ALB2 -->|"NodePort 30083 · local"| C & D
    ALB3 -->|"NodePort 30083 · local"| E & F

    A & B -->|"PreferSameZone"| L1
    C & D -->|"PreferSameZone"| L2
    E & F -->|"PreferSameZone"| L3

    A & B & C & D & E & F -.->|"may cross AZ"| Matching
    L1 & L2 & L3 -.->|"writer or reader may be remote"| RDS
```

Solid arrows show the intended same-AZ path. Dashed arrows show hops that can
still cross zones: Matching remains a singleton, and RDS endpoints follow
database roles rather than caller locality. If an AZ has no healthy local Order
target, the ALB removes that zonal node from DNS and new traffic fails away to a
healthy zone.

## Private tier

The application workloads share the three private subnets. This diagram shows
their main connections; a service drawn once may have resources in more than
one Availability Zone.

```mermaid
flowchart LR
    subgraph VPC["VPC: private tier across 3 Availability Zones"]
        subgraph EKS["EKS managed nodes and Kubernetes pods"]
            Order["Order Service<br/>ClusterIP + NodePort 30083"]
            Ledger["Ledger Service"]
            Matching["Matching Engine"]
            Collector["ADOT Collector"]
        end

        Outbox["ECS Fargate<br/>Outbox Relay"]
        OutboxCollector["ADOT sidecar<br/>when configured"]
        DB[("PostgreSQL<br/>Ledger database")]
        MSK[("Amazon MSK<br/>Kafka brokers")]
        RT["Private route tables<br/>one per subnet"]
    end

    AMP[("Amazon Managed Prometheus<br/>metrics workspace")]
    XRay["AWS X-Ray<br/>traces"]
    CloudWatch["Amazon CloudWatch<br/>logs and alarms"]
    Grafana["Amazon Managed Grafana<br/>workspace and dashboards"]

    Order -->|"gRPC via Kubernetes Service"| Ledger
    Order -->|"gRPC via Kubernetes Service"| Matching
    Ledger -->|"SQL :5432"| DB
    Ledger <-->|"Kafka IAM/TLS :9098"| MSK
    Matching -->|"Kafka IAM/TLS :9098"| MSK
    Outbox -->|"read outbox rows · SQL :5432"| DB
    Outbox -->|"publish ledger events · IAM/TLS :9098"| MSK

    Order & Ledger & Matching -->|"traces"| Collector
    Collector -->|"scrapes metrics"| Order & Ledger & Matching
    Collector -->|"remote write metrics"| AMP
    Collector -->|"export traces"| XRay
    Outbox -.->|"optional traces and metrics"| OutboxCollector
    OutboxCollector -.->|"remote write metrics"| AMP
    OutboxCollector -.->|"export traces"| XRay
    Grafana -->|"query metrics"| AMP
    Outbox -->|"task logs"| CloudWatch
    OutboxCollector -.->|"sidecar logs"| CloudWatch
    MSK -->|"broker logs"| CloudWatch
    DB -->|"database logs"| CloudWatch

    RT -->|"0.0.0.0/0"| NAT["NAT Gateway<br/>in public subnet"]
    NAT --> IGW["Internet Gateway"]
    Collector -->|"AWS service APIs via private default route"| RT
```

Traffic among workloads, MSK, and PostgreSQL uses the VPC's local route.
The private route tables and NAT gateways handle outbound traffic to
destinations outside the VPC. Grafana queries metrics from AMP; the collectors
send traces to X-Ray. CloudWatch receives the configured ECS, MSK, and
PostgreSQL logs and hosts infrastructure alarms. The Outbox ADOT sidecar is
created only when its image is configured.
