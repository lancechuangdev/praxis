# Risk service

Risk owns the independently deployable order-admission policy exposed through
`risk.v1.RiskService/CheckOrder`. The current policy deliberately preserves the
embedded deterministic behavior so Order can compare decisions in shadow mode
before authority moves.

Configuration:

- `RISK_GRPC_ADDRESS` defaults to `:9095`.
- `RISK_HTTP_ADDRESS` defaults to `:8085`.
- `RISK_POLICY_VERSION` defaults to `embedded-v1`.
- `RISK_REJECT_BPS` defaults to `0` and must be between 0 and 10000.
- `RISK_DECISION_LATENCY` defaults to `1ms`.

The service exposes `/healthz`, `/readyz`, and `/metrics`. Decision metrics use
bounded labels only. The policy version and deterministic decision ID make
shadow comparisons auditable without using account or order IDs as metric
labels. Persistent rule management and decision-audit storage remain future
work; this slice owns immutable startup policy configuration.
