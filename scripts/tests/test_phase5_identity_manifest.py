import json
from pathlib import Path
import subprocess
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'render-phase5-identity-probes.py'


class IdentityManifestTest(unittest.TestCase):
    def test_probe_identity_is_separate_and_calls_only_health_probe(self):
        image = 'registry.example/probe@sha256:' + 'a' * 64
        result = subprocess.run(['python3', str(SCRIPT), '--image', image, '--run-id', 'test'], check=True, capture_output=True, text=True)
        items = json.loads(result.stdout)['items']
        self.assertEqual(items[0]['kind'], 'ServiceAccount')
        self.assertNotEqual(items[0]['metadata']['name'], 'order')
        jobs = items[1:]
        self.assertEqual(len(jobs), 4)
        for job in jobs:
            spec = job['spec']
            self.assertEqual(spec['backoffLimit'], 0)
            pod = spec['template']['spec']
            args = pod['containers'][0]['args']
            expectation = args[args.index('--expect') + 1]
            self.assertEqual(pod['serviceAccountName'], 'order' if expectation == 'allow' else items[0]['metadata']['name'])
            self.assertIn('--quit-sidecar', args)
            self.assertFalse(pod['automountServiceAccountToken'])
            self.assertEqual(pod['containers'][0]['image'], image)

    def test_reject_mutable_image_and_invalid_name(self):
        for image, name in [('probe:latest', 'test'), ('probe@sha256:' + 'a' * 64, '../bad')]:
            result = subprocess.run(['python3', str(SCRIPT), '--image', image, '--run-id', name], capture_output=True)
            self.assertNotEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()
