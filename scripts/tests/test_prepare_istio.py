import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class PrepareIstioTest(unittest.TestCase):
    def test_only_pulls_pinned_charts_and_renders(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            helm = root / 'helm'
            helm.write_text('''#!/usr/bin/env python3
import pathlib, sys
args = sys.argv[1:]
if args[0] == 'pull':
    assert args[args.index('--version') + 1] == '1.31.1'
    assert args[args.index('--repo') + 1] == 'https://blob.istio.io/istio-release/charts'
    (pathlib.Path(args[-1]) / (args[1] + '-1.31.1.tgz')).write_text('test chart')
elif args[0] == 'template':
    assert pathlib.Path(args[2]).is_file()
    print('kind: ConfigMap')
else: raise AssertionError('Unexpected mutating Helm command')
''')
            helm.chmod(0o755)
            artifacts = root / 'artifacts'
            env = dict(os.environ, PATH=directory + ':' + os.environ['PATH'], ISTIO_ARTIFACT_DIR=str(artifacts))
            result = subprocess.run(['bash', str(ROOT / 'scripts/prepare-istio.sh')], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('istiod-1.31.1.tgz', (artifacts / 'SHA256SUMS').read_text())
            result = subprocess.run(['bash', str(ROOT / 'scripts/prepare-istio.sh')], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0, 'Existing artifacts must not be overwritten')


if __name__ == '__main__':
    unittest.main()
