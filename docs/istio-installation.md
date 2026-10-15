# Istio installation for Praxis

## Purpose and ownership

Praxis uses Istio's sidecar data plane for optional internal mTLS, workload
authorization, endpoint-aware load balancing, and transport telemetry. The
Istio control plane must be installed before the Kubernetes stack is applied
with `enable_istio_mesh=true`.

The Praxis Kubernetes Terraform stack manages workload enrollment and Istio
policy resources. It deliberately does not own the cluster-wide Istio
installation:

```text
Platform installation lifecycle
    |-- Istio CRDs and cluster roles
    |-- Istio CNI node agent
    |-- Istiod
    |-- admission and validation webhooks
    `-- injection templates and trust configuration
                    |
                    v
Praxis Kubernetes lifecycle
    |-- Order, Ledger, and Matching sidecar enrollment
    |-- PeerAuthentication
    |-- AuthorizationPolicy
    |-- DestinationRule
    `-- Telemetry
```

The platform team or a separately authorized deployment pipeline owns the
first lifecycle. This separation prevents the same Terraform apply from trying
to create custom resources before the Kubernetes API knows their CRDs.

## Scope

This runbook installs the Istio sidecar control plane with Helm. It does not
install an Istio ingress gateway. Praxis continues to use its AWS Application
Load Balancer and the Order NodePort for public ingress.

This runbook uses Istio CNI so the restricted Praxis namespace needs no
privileged network-init container. Ambient mode, proxy retries and outlier
detection remain disabled. Follow the [Phase 5 acceptance procedure](phase-5-acceptance.md)
for artifact preparation, incremental rollout, identity probes and failure
exercises; a successful installation does not complete those gates.

## Prerequisites

The operator needs:

- network access to the private EKS API;
- AWS credentials authorized for the EKS cluster-admin access entry;
- `aws`, `kubectl`, and Helm installed;
- a selected Istio release compatible with the deployed Kubernetes version;
- approval for cluster-scoped CRDs, RBAC, and admission webhooks; and
- a non-production environment for the first installation and mesh rollout.

Use an actively supported Istio patch release. Pin the exact chart version in
the deployment pipeline and retain it with the acceptance evidence. Do not use
an unversioned latest release for a production installation.

## 1. Connect to EKS

Read the cluster identity from the already-applied AWS stack:

```bash
PRAXIS_CLUSTER=$(terraform -chdir=infra/aws output -raw eks_cluster_name)
PRAXIS_REGION=$(terraform -chdir=infra/aws output -json eks_runtime_config | jq -r .aws_region)

aws eks update-kubeconfig \
  --name "${PRAXIS_CLUSTER}" \
  --region "${PRAXIS_REGION}"
```

Verify the target before making cluster-scoped changes:

```bash
kubectl config current-context
kubectl cluster-info
kubectl auth can-i create customresourcedefinitions.apiextensions.k8s.io
kubectl auth can-i create mutatingwebhookconfigurations.admissionregistration.k8s.io
```

Both authorization checks must return `yes`.

## 2. Select a pinned Istio version

Set the exact approved version returned by the Istio chart repository:

```bash
PRAXIS_ISTIO_VERSION="$(cat infra/istio/version)"
```

Keep the same version for the `base` and `istiod` charts. Record the version,
chart digest or provenance information, operator identity, target cluster, and
installation time in the deployment evidence.

## 3. Configure the Helm repository

```bash
helm repo add istio https://blob.istio.io/istio-release/charts
helm repo update
helm search repo istio/istiod --versions
```

Confirm that `PRAXIS_ISTIO_VERSION` is present before continuing.

## 4. Inspect the proposed resources

Render both charts before installation so the cluster-scoped changes can be
reviewed:

```bash
helm template istio-base istio/base \
  --namespace istio-system \
  --version "${PRAXIS_ISTIO_VERSION}" \
  --set defaultRevision=default

helm template istiod istio/istiod \
  --namespace istio-system \
  --version "${PRAXIS_ISTIO_VERSION}" \
  --values infra/istio/istiod-values.yaml
```

The review should confirm the pinned proxy and control-plane images, resource
requests and limits, replica count, pod security settings, webhook selectors,
trust domain, and compatibility with cluster policy.

## 5. Install the base chart

The base chart installs Istio CRDs and cluster-level RBAC before any Istio
custom resources are created:

```bash
helm upgrade --install istio-base istio/base \
  --namespace istio-system \
  --create-namespace \
  --version "${PRAXIS_ISTIO_VERSION}" \
  --set defaultRevision=default \
  --wait \
  --timeout 10m
```

Verify the Helm release and required APIs:

```bash
helm status istio-base --namespace istio-system
kubectl get crd destinationrules.networking.istio.io
kubectl get crd peerauthentications.security.istio.io
kubectl get crd authorizationpolicies.security.istio.io
kubectl get crd telemetries.telemetry.istio.io
```

Do not enable the Praxis mesh if any required CRD is absent.

## 6. Install CNI, then Istiod

Before Istiod, install the same pinned CNI release. Review node paths and AWS
VPC CNI compatibility; the platform namespace must allow its privileged node
agent. Keep the application namespace restricted:

```bash
helm upgrade --install istio-cni istio/cni \
  --namespace istio-system \
  --version "${PRAXIS_ISTIO_VERSION}" \
  --values infra/istio/cni-values.yaml \
  --wait --timeout 10m
kubectl rollout status daemonset/istio-cni-node --namespace istio-system --timeout=5m
```

Istiod watches Kubernetes discovery and Istio policy resources, operates the
sidecar injection webhook, issues workload certificates, and sends xDS
configuration to Envoy proxies.

```bash
helm upgrade --install istiod istio/istiod \
  --namespace istio-system \
  --version "${PRAXIS_ISTIO_VERSION}" \
  --values infra/istio/istiod-values.yaml \
  --wait \
  --timeout 10m
```

Verify the control plane and webhooks:

```bash
helm status istiod --namespace istio-system
kubectl rollout status deployment/istiod --namespace istio-system --timeout=5m
kubectl get pods,services --namespace istio-system
kubectl get mutatingwebhookconfigurations -l app=sidecar-injector
kubectl get validatingwebhookconfigurations -l app=istiod
```

Retain the command output as deployment evidence.

## 7. Analyze the cluster before enrollment

Use the `istioctl` binary from the same Istio release as the control plane:

```bash
istioctl version
istioctl analyze --all-namespaces
```

Resolve unexpected analysis errors before enabling sidecar injection. Existing
warnings must be understood and recorded rather than silently ignored.

## 8. Enable the Praxis mesh

Enroll Order first with `mesh_enrolled_workloads=["order"]`, then Ledger, then
Matching, retaining Order in every nonempty enrollment. Persist each accepted
set in the environment variable file and verify traffic after each rollout.
The full-enrollment plan below is the final step, not the initial canary:

First review the Kubernetes plan:

```bash
terraform -chdir=infra/k8s plan \
  -var='enable_istio_mesh=true'
```

The plan should enroll only Order, Ledger, and Matching and create the expected
security, traffic, and telemetry resources. Risk, database migrations, Outbox,
and the observability collector remain unenrolled.

Apply after approval:

```bash
terraform -chdir=infra/k8s apply \
  -var='enable_istio_mesh=true'
```

Persist this setting in the environment's approved Terraform variable file or
pipeline configuration. A later apply that omits the value will use the
default `false` and plan a mesh rollback.

Changing the pod-template annotation triggers rolling replacement. Istio's
admission webhook adds the `istio-proxy` container only to newly created pods.

## 9. Verify enrollment and synchronization

Wait for all enrolled workloads:

```bash
kubectl rollout status deployment/order --namespace praxis --timeout=5m
kubectl rollout status deployment/ledger --namespace praxis --timeout=5m
kubectl rollout status deployment/matching --namespace praxis --timeout=5m
```

Confirm that each pod contains its application container and `istio-proxy`:

```bash
kubectl get pods --namespace praxis \
  -l 'app in (order,ledger,matching)' \
  -o custom-columns='NAME:.metadata.name,CONTAINERS:.spec.containers[*].name,READY:.status.containerStatuses[*].ready'
```

Check that every proxy is connected to Istiod and has accepted its
configuration:

```bash
istioctl proxy-status
istioctl analyze --namespace praxis
```

Also inspect the created policies:

```bash
kubectl get peerauthentication,authorizationpolicy,destinationrule,telemetry \
  --namespace praxis
```

These checks prove configuration presence, not functional acceptance.

Run the repository's read-only preflight to repeat these checks and validate
the exact security, traffic, and access-log policy shape:

```bash
make phase5-verify
```

The preflight does not generate traffic, create an unauthorized test identity,
scale workloads, delete pods, or alter mesh configuration. Those actions remain
explicit non-production acceptance exercises.

## 10. Required acceptance evidence

Before accepting the mesh for production traffic, verify and retain evidence
that:

1. Order-to-Ledger and Order-to-Matching traffic reports `mutual_tls`;
2. an unauthorized meshed service identity cannot call either financial gRPC
   port;
3. application metrics and traces remain available and unchanged;
4. allowlisted Istio request metrics arrive in AMP;
5. error and authorization-denial access logs arrive in CloudWatch without
   credentials or financial payloads;
6. sustained requests reach every eligible backend endpoint;
7. scale-out, readiness removal, and rolling drain update endpoint selection;
8. proxy connection and pending-request limits produce bounded rejection;
9. task, node, zone, Istiod, collector, and shared-dependency failures degrade
   safely; and
10. proxy CPU, memory, latency, cardinality, and log-volume overhead stay within
    the approved budgets.

Proxy retries and outlier detection must remain disabled during this initial
acceptance.

The managed Grafana stack includes the `Praxis Mesh Acceptance` dashboard for
these exercises. Follow the [dashboard evidence procedure](../infra/grafana/README.md)
to compare destination-side per-pod traffic, transport security, RPC outcomes,
and application telemetry with ready endpoint inventories. Dashboard presence
does not complete any live acceptance gate.

## Rollback

Mesh policy and sidecar removal must precede control-plane removal. First plan
and apply the Praxis rollback:

```bash
terraform -chdir=infra/k8s plan \
  -var='enable_istio_mesh=false'

terraform -chdir=infra/k8s apply \
  -var='enable_istio_mesh=false'
```

Wait for the three application rollouts and confirm that no `istio-proxy`
containers remain. Confirm that the Praxis namespace has no Terraform-managed
Istio policies before uninstalling Istiod.

If the control plane must also be removed, uninstall the discovery chart first:

```bash
helm uninstall istiod --namespace istio-system
helm uninstall istio-cni --namespace istio-system
helm uninstall istio-base --namespace istio-system
```

Do not casually delete Istio CRDs. Deleting a CRD deletes all Kubernetes custom
resources of that type across the cluster. CRD removal is a separately approved
cluster-wide cleanup after every Istio consumer has been removed.

## Upgrades

Treat an Istio upgrade as an independent rollout. Review supported-version and
Kubernetes compatibility, use revisioned or canary control planes where
appropriate, verify proxy/control-plane skew rules, repeat the Phase 5
acceptance tests, and retain an explicit rollback path. Never upgrade Istio as
an incidental part of an application deployment.

## References

- [Istio installation with Helm](https://istio.io/latest/docs/setup/install/helm/)
- [Istio upgrade with Helm](https://istio.io/latest/docs/setup/upgrade/helm/)
- [Istio security best practices](https://istio.io/latest/docs/ops/best-practices/security/)
- [Praxis observability and Istio](observability-and-istio.md)
- [Praxis gRPC load-balancing options](grpc-load-balancing-options.md)
- [Praxis implementation roadmap](implementation-roadmap.md)
