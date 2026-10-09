# Phase 1 production-like baseline and failure validation

## Status

The repository-side Phase 1 controls are implemented. This runbook must be
executed in a non-production AWS environment before Phase 1 is accepted. No
Terraform apply, live ingestion check, authenticated request, alert delivery,
or failure drill is implied by committing the configuration.

## Implemented controls

- Order's public HTTPS ALB requires OIDC authentication before forwarding.
- Order, Ledger, and Matching have CPU HPAs with bounded scale-up and
  conservative scale-down behavior.
- Pod disruption budgets retain serving replicas during voluntary disruption.
- The EKS metrics-server add-on supplies resource metrics to the HPAs.
- The CloudWatch Observability add-on ships EKS application logs and Container
  Insights using Pod Identity and retained log groups.
- A digest-pinned ADOT collector DaemonSet runs on each hot-path node. Each
  collector scrapes only pods on its own node, avoiding duplicate Prometheus
  samples without retaining the former single-pod collector failure domain.
- AMP and Grafana rules cover Order, Ledger, Matching, and Outbox RED failures.
- `scripts/verify-phase1.sh` checks rollout, metrics-server, HPA metrics,
  disruption budgets, unauthenticated ingress rejection, AMP ingestion, and
  application-log arrival.
- `scripts/phase1-pod-failure-drill.sh` deletes one pod from each hot-path
  deployment and one collector pod, then verifies recovery. It refuses contexts
  whose names appear to be production and requires an explicit acknowledgement.

## Pre-deployment review

1. Use an isolated non-production AWS account or environment.
2. Review the complete `infra/aws` and `infra/k8s` plans.
3. Confirm that every application and collector image is digest-pinned.
4. Resolve supported, pinned EKS add-on versions for the configured Kubernetes
   version and region.
5. Register `https://<order-alb-dns>/oauth2/idpresponse` with the OIDC provider.
6. Supply the OIDC client secret through protected CI input or a protected
   tfvars file. Terraform state contains the secret; use encrypted remote state
   with tightly scoped access before applying outside a disposable environment.
7. Confirm that the alert mailbox is monitored and contains no sensitive
   payload data.
8. Confirm database backup retention, deletion protection, Kafka retention,
   and rollback image availability.

## Deployment sequence

Follow `infra/k8s/README.md` for migrations and workload deployment. In summary:

1. Apply `infra/aws` after reviewing its saved plan.
2. Apply `infra/k8s` in migration-only mode.
3. Run the Outbox migration task.
4. Apply digest-pinned runtime images and the AWS stack again.
5. Apply the full Kubernetes workload stack.
6. Apply `infra/grafana` after AMP ingestion succeeds.

Do not use `-auto-approve`. Retain the plans, apply logs, image digests, add-on
versions, Terraform state version, operator identity, and timestamps as test
evidence.

## Automated verification

From an operator machine with AWS and private EKS access:

```bash
./scripts/verify-phase1.sh
```

The check intentionally stops short of claiming complete success. OIDC requires
an authenticated browser or test client, X-Ray requires a sampled request, and
email delivery requires confirmation outside Terraform.

## Required traffic profile

Run normal, burst, and sustained profiles through authenticated ingress. Record:

- offered, admitted, rejected, failed, and completed request rates;
- p50, p95, and p99 end-to-end and service-stage latency;
- replicas, CPU, memory, restarts, throttling, and HPA decisions;
- request distribution by pod and zone;
- Ledger and Matching database pool use and acquisition delay;
- database CPU, connections, lock waits, storage latency, IOPS, and replica lag;
- Kafka partition lag, consumer lag, and outbox oldest-unpublished age;
- collector export failures, scrape health, log delay, and trace sampling;
- financial-integrity and reconciliation results; and
- infrastructure and telemetry cost for the test window.

Do not derive production rate limits, connection budgets, or SLOs from the
local Phase 0 profile.

## Failure exercises

Begin with the bounded pod drill:

```bash
ACK_PHASE1_FAILURE_DRILL=delete-one-pod ./scripts/phase1-pod-failure-drill.sh
```

After that passes, separately review and execute node termination, Availability
Zone isolation, broker interruption, database failover, collector/export
failure, identity denial, and alert-backend failure. Those exercises affect
shared infrastructure and require an experiment-specific plan, abort criteria,
and operator approval; they are not automated by the pod-deletion script.

For every experiment, record detection time, rejected and failed work, maximum
latency, recovery time, lost or duplicate effects, financial reconciliation,
and alert delivery.

## Rollback

Keep the previous image digests and Terraform plans until acceptance. If an
application revision fails, restore its prior digest and apply Kubernetes. If
the collector fails, restore its prior digest or configuration without changing
application instrumentation. If OIDC prevents intended clients from accessing
Order, correct or roll back the identity configuration; never bypass
authentication by forwarding the public listener directly.

Database migrations must remain backward compatible throughout this window.
Do not destroy topics, log groups, old projection data, or database columns as
part of a Phase 1 rollback.

## Acceptance record

Phase 1 is complete only when an operator records:

- environment and AWS account identifier;
- git commit and all deployed image digests;
- Terraform plan and apply identifiers;
- Kubernetes and add-on versions;
- traffic-profile results and financial-integrity outcome;
- authenticated-ingress evidence;
- AMP, X-Ray, CloudWatch Logs, Grafana, and email-delivery evidence;
- every failure exercise and recovery time;
- remaining exceptions with owners and due dates; and
- approval to proceed to Phase 2.
