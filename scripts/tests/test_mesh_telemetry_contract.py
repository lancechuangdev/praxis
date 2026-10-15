"""Keep the mesh dashboard compatible with the collector's label filter."""
import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]


class MeshTelemetryContractTest(unittest.TestCase):
    def test_histogram_and_dashboard_labels_survive_collection(self):
        collector = (ROOT / "infra/k8s/collector.tf").read_text()
        labels = set(re.search(r'regex\s*=\s*"(__name__\|[^"\n]+)"', collector)[1].split("|"))
        self.assertIn("le", labels, "Dropping le collapses histogram bucket series")
        dashboard = json.loads((ROOT / "observability/grafana/dashboards/praxis-mesh-acceptance.json").read_text())
        for panel in dashboard["panels"]:
            for target in panel["targets"]:
                expr = target["expr"]
                with self.subTest(panel=panel["title"], query=expr):
                    self.assertIn('environment=~"${environment:regex}"', expr)
                    self.assertIn('namespace="praxis"', expr)
                    if "istio_" not in expr:
                        continue
                    self.assertIn('reporter="destination"', expr)
                    required = set(re.findall(r'([a-z_]+)(?:=~|=)"', expr))
                    for group in re.findall(r'(?:by|on)\s*\(([^)]+)\)', expr):
                        required.update(x.strip() for x in group.split(","))
                    self.assertFalse(required - labels, f"Dropped labels: {required - labels}")


if __name__ == "__main__":
    unittest.main()
