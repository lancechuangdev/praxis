"""Offline command-boundary tests for the mesh preflight."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


MOCK = r'''#!/usr/bin/env python3
import json, os, sys
tool = os.path.basename(sys.argv[0])
args = sys.argv[1:]
limits = dict(max_connections=12, max_pending_requests=8, max_active_requests=40)
def emit(value): print(json.dumps(value))
if tool == "terraform":
    if args[-1] == "eks_cluster_name": print("test-cluster")
    elif args[-1] == "eks_runtime_config": emit(dict(aws_region="us-west-2"))
    elif args[-1] == "mesh_enrolled_workloads": emit(["order"] if os.environ.get("TEST_FAULT") == "partial" else ["order", "ledger", "matching"])
    else: emit(dict(ledger=limits, matching=limits))
elif tool == "aws": pass
elif tool == "istioctl":
    if args[0] == "proxy-status":
        for name in ("order", "ledger", "matching"): print(name + "-1.praxis SYNCED SYNCED SYNCED")
elif tool == "kubectl":
    if args[0] == "rollout" or args[1] == "crd": sys.exit(0)
    kind = args[1]
    name = args[2]
    if kind == "pods":
        name = args[args.index("-l") + 1].split("=")[1]
        if "jsonpath" in args[-1]: print(name + "-1")
        else: emit(dict(items=[dict(spec=dict(containers=[dict(name=name), dict(name="istio-proxy")]), status=dict(containerStatuses=[dict(ready=True), dict(ready=True)]))]))
    elif kind == "deployment": print(1)
    elif kind == "endpointslice": emit(dict(items=[dict(endpoints=[dict(conditions=dict(ready=True))])]))
    elif kind == "peerauthentication":
        app = name.replace("-strict-mtls", "")
        port = dict(order="8083", ledger="8081", matching="8084")[app]
        emit(dict(spec=dict(selector=dict(matchLabels=dict(app=app)), mtls=dict(mode="STRICT"), portLevelMtls={port: dict(mode="DISABLE")})))
    elif kind == "telemetry":
        app = name.replace("-error-access-logs", "")
        emit(dict(spec=dict(selector=dict(matchLabels=dict(app=app)), accessLogging=[dict(providers=[dict(name="envoy")], filter=dict(expression="!has(response.code) || response.code >= 400"))])))
    elif kind == "authorizationpolicy":
        app = name.replace("-from-order", "")
        emit({"spec": {"selector": {"matchLabels": {"app": app}}, "action": "ALLOW", "rules": [{"from": [{"source": {"principals": ["cluster.local/ns/praxis/sa/order"]}}], "to": [{"operation": {"ports": [dict(ledger="9091", matching="9092")[app]]}}]}]}})
    elif kind == "destinationrule":
        policy = dict(loadBalancer=dict(simple="ROUND_ROBIN"), connectionPool=dict(tcp=dict(maxConnections=12), http=dict(http1MaxPendingRequests=8, http2MaxRequests=40, maxRetries=0)))
        fault = os.environ.get("TEST_FAULT")
        if fault == "limit": policy["connectionPool"]["http"]["http2MaxRequests"] = 256
        if fault == "retry": policy["connectionPool"]["http"]["maxRetries"] = 1
        if fault == "ejection": policy["outlierDetection"] = {}
        emit(dict(spec=dict(host=name + ".praxis.svc.cluster.local", trafficPolicy=policy)))
'''


class PreflightTest(unittest.TestCase):
    def test_applied_limits_and_policy_drift(self):
        script = Path(__file__).resolve().parents[1] / "verify-phase5.sh"
        with tempfile.TemporaryDirectory() as directory:
            for name in ("terraform", "aws", "kubectl", "istioctl"):
                path = Path(directory) / name
                path.write_text(MOCK)
                path.chmod(0o755)
            for fault in ("", "partial", "limit", "retry", "ejection"):
                with self.subTest(fault=fault):
                    env = dict(os.environ, PATH=directory + ":" + os.environ["PATH"], TEST_FAULT=fault)
                    result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
                    self.assertEqual(result.returncode == 0, fault in ("", "partial"), result.stdout + result.stderr)
                    if fault == "partial":
                        self.assertIn("full Phase 5 acceptance is incomplete", result.stdout)


if __name__ == "__main__":
    unittest.main()
