# Two ways to configure gRPC load balancing

## Purpose

Praxis can distribute Order Service RPCs across Ledger and Matching replicas in
two ways:

1. native gRPC client-side round robin using a headless Kubernetes Service; or
2. Istio-managed round robin using the existing ClusterIP Service.

Both approaches use the same Ledger and Matching Deployments. A headless
Service is only a discovery object; it does not duplicate application code,
pods, state, or business logic.

The default Praxis deployment currently uses neither endpoint-aware option. It
dials an ordinary ClusterIP Service with gRPC's default `pick_first` behavior,
so Kubernetes selects a backend when each long-lived connection is created.

## Why the default is connection-balanced

Order currently dials these addresses:

```text
ledger.praxis.svc.cluster.local:9091
matching.praxis.svc.cluster.local:9092
```

Each name belongs to a normal Kubernetes `ClusterIP` Service. DNS returns one
virtual IP, and each Order process creates one long-lived HTTP/2 connection to
that IP. Kubernetes selects a backend pod for the connection, after which many
gRPC calls can remain pinned to that pod.

```text
Order process
    |
    | one long-lived HTTP/2 connection
    v
Ledger ClusterIP
    |
    | backend selected when the connection is created
    v
Ledger pod B
    |-- Reserve RPC
    |-- Reserve RPC
    `-- Release RPC
```

Adding a gRPC `round_robin` policy without changing discovery does not solve
this. The client would see only one candidate address: the ClusterIP.

## Option 1: native gRPC round robin

Native gRPC balancing requires the client to discover individual pod addresses.
In Kubernetes, a headless Service provides those addresses through DNS.

### Kubernetes discovery

Add a second Service that selects the existing Ledger pods:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ledger-headless
  namespace: praxis
spec:
  clusterIP: None
  selector:
    app: ledger
  ports:
    - name: grpc
      port: 9091
      targetPort: grpc
```

The existing `ledger` Service and new `ledger-headless` Service may coexist and
select the same pods:

```text
                          /-- Ledger pod A
ledger Service ----------|-- Ledger pod B
                          \-- Ledger pod C

                           /-- Ledger pod A
ledger-headless Service --|-- Ledger pod B
                           \-- Ledger pod C
```

The difference is their DNS response:

```text
ledger.praxis.svc.cluster.local
    -> one virtual ClusterIP

ledger-headless.praxis.svc.cluster.local
    -> Ledger pod A IP
    -> Ledger pod B IP
    -> Ledger pod C IP
```

Kubernetes normally publishes only ready endpoints unless the Service is
configured to publish not-ready addresses.

### Go client configuration

Order must explicitly select the gRPC DNS resolver and round-robin policy:

```go
connection, err := grpc.NewClient(
    "dns:///ledger-headless.praxis.svc.cluster.local:9091",
    grpc.WithTransportCredentials(insecure.NewCredentials()),
    grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
    grpc.WithDefaultServiceConfig(
        `{"loadBalancingConfig":[{"round_robin":{}}]}`,
    ),
)
```

The DNS resolver supplies the pod addresses, and gRPC maintains subchannels to
the ready endpoints. The round-robin picker distributes calls across those
subchannels:

```text
Order gRPC client
    |-- RPC 1 -> Ledger pod A
    |-- RPC 2 -> Ledger pod B
    |-- RPC 3 -> Ledger pod C
    `-- RPC 4 -> Ledger pod A
```

The same pattern requires a separate `matching-headless` Service and Matching
client target.

### Operational responsibilities

With this option, each application client owns:

- resolver and balancing configuration;
- DNS refresh and endpoint-update behavior;
- transport security and server-name verification;
- connection and subchannel limits;
- per-endpoint telemetry;
- readiness and graceful-drain behavior; and
- retry policy and retry budgets.

Do not add transparent retries merely because multiple endpoints are visible.
Financial mutations require stable idempotency keys, server-side idempotent
replay, classified retryable failures, and a bounded end-to-end deadline.

## Option 2: Istio-managed round robin

With Istio, Order continues to dial the existing ClusterIP name:

```text
ledger.praxis.svc.cluster.local:9091
```

An Envoy sidecar intercepts the connection. Istiod watches Kubernetes Services,
pods, and EndpointSlices and sends the ready endpoint set to Envoy through xDS.

```text
Kubernetes Pods and EndpointSlices
                |
                v
              Istiod
                |
                | xDS endpoint updates
                v
Order Envoy sidecar
    |-- RPC 1 -> Ledger pod A sidecar
    |-- RPC 2 -> Ledger pod B sidecar
    |-- RPC 3 -> Ledger pod C sidecar
    `-- RPC 4 -> Ledger pod A sidecar
```

The Go client does not need a headless Service or a gRPC round-robin policy.
Envoy performs endpoint selection and maintains the downstream connection pool.

Praxis's opt-in `DestinationRule` configures:

```yaml
trafficPolicy:
  loadBalancer:
    simple: ROUND_ROBIN
  connectionPool:
    tcp:
      maxConnections: 64
      connectTimeout: 3s
    http:
      http1MaxPendingRequests: 64
      http2MaxRequests: 256
      maxRequestsPerConnection: 0
      maxRetries: 0
```

The same mesh configuration also provides workload identity, mTLS,
authorization, and per-endpoint transport telemetry. Praxis deliberately keeps
proxy retries disabled.

### Operational responsibilities

With this option, platform operators own:

- the Istio control-plane and proxy lifecycle;
- xDS delivery and proxy synchronization;
- certificate issuance and rotation;
- sidecar CPU, memory, latency, and connection overhead;
- mesh metrics, traces, and access-log volume;
- traffic-policy rollout and rollback; and
- compatibility between Istio and Kubernetes versions.

Applications still own business deadlines, idempotency, financial retry
semantics, and domain telemetry.

## Comparison

| Property | Headless Service plus gRPC | ClusterIP plus Istio |
| --- | --- | --- |
| Endpoint discovery | gRPC DNS resolver | Istiod watches Kubernetes and pushes xDS |
| Endpoint selection | Go gRPC client | Envoy sidecar |
| Application changes | Headless target and client policy | Usually none |
| Kubernetes Service | Headless | Existing ClusterIP |
| Workload mTLS and identity | Application/platform must provide it | Integrated |
| Authorization policy | Application or separate infrastructure | Istio policy |
| Per-endpoint telemetry | Must be implemented or exported by clients | Mesh telemetry |
| Resource overhead | More client subchannels | Sidecar per pod plus control plane |
| Policy consistency | Repeated in every client | Centrally managed |
| Control-plane dependency | Kubernetes DNS | Kubernetes and Istiod |
| Retry ownership | gRPC/application configuration | Application and optionally proxy |

## Do not enable both balancing layers casually

Using a headless target with gRPC round robin while also asking Istio to balance
the same calls creates two endpoint-selection layers. It complicates connection
counts, locality behavior, draining, metrics, and incident diagnosis without a
clear benefit.

Choose one component to own endpoint selection:

```text
headless DNS -> gRPC selects the pod

or

ClusterIP name -> Istio selects the pod
```

## Praxis decision

Praxis prefers Istio-managed balancing because Phase 5 also requires workload
identity, mTLS, authorization, and uniform transport telemetry. The repository
contains an opt-in implementation behind `enable_istio_mesh=false`.

Native gRPC round robin remains the preferred fallback if Istio adoption is
postponed or rejected. It is not currently implemented in the deployment.

## Acceptance tests for either option

Before enabling endpoint-aware balancing in production, verify that:

1. sustained traffic reaches every ready Ledger and Matching replica;
2. a new replica receives traffic without restarting Order;
3. an unready or terminating replica stops receiving new calls promptly;
4. in-flight calls drain within the termination deadline;
5. one failed endpoint does not cause unbounded retries;
6. whole-fleet overload produces bounded rejection instead of growing queues;
7. zone preference does not overload a small local endpoint set;
8. ambiguous commit failures remain safe through server-side idempotency; and
9. per-endpoint request, error, latency, and connection metrics identify the
   endpoint that actually handled each call.

## Related documentation

- [gRPC load balancing](grpc-load-balancing.md)
- [Observability and Istio](observability-and-istio.md)
- [Outlier detection and ejection spirals](outlier-detection-ejection-spirals.md)
- [Retries and failure policy](adr/0004-retries-and-failure-policy.md)
