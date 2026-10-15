#!/usr/bin/env python3
"""Render health-only identity probe Jobs; does not apply cluster changes."""
import argparse
import json
import re

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--image', required=True, help='Digest-pinned mesh-probe image built from api/cmd/mesh-probe/Dockerfile')
parser.add_argument('--run-id', required=True, help='Unique DNS label, up to 30 characters')
args = parser.parse_args()
if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', args.image):
    parser.error('A digest-pinned image is required')
if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,28}[a-z0-9])?', args.run_id):
    parser.error('run-id must be a DNS label up to 30 characters')
account = 'mesh-probe-' + args.run_id
items = [dict(apiVersion='v1', kind='ServiceAccount', metadata=dict(name=account, namespace='praxis'), automountServiceAccountToken=False)]
for backend, port in (('ledger', 9091), ('matching', 9092)):
    for expectation in ('allow', 'deny'):
        name = f'{args.run_id}-{backend}-{expectation}'
        container = dict(name='probe', image=args.image,
                         args=['--target', f'{backend}.praxis.svc.cluster.local:{port}', '--expect', expectation, '--quit-sidecar'],
                         resources=dict(requests=dict(cpu='20m', memory='32Mi'), limits=dict(cpu='100m', memory='64Mi')),
                         securityContext=dict(allowPrivilegeEscalation=False, readOnlyRootFilesystem=True, capabilities=dict(drop=['ALL']), runAsNonRoot=True, seccompProfile=dict(type='RuntimeDefault')))
        pod = dict(serviceAccountName='order' if expectation == 'allow' else account,
                   automountServiceAccountToken=False, restartPolicy='Never', terminationGracePeriodSeconds=20,
                   containers=[container])
        items.append(dict(apiVersion='batch/v1', kind='Job', metadata=dict(name=name, namespace='praxis'),
                          spec=dict(backoffLimit=0, activeDeadlineSeconds=90,
                                    template=dict(metadata=dict(labels=dict(app='mesh-identity-probe'), annotations={'sidecar.istio.io/inject':'true', 'proxy.istio.io/config':'{"holdApplicationUntilProxyStarts":true}'}), spec=pod))))
print(json.dumps(dict(apiVersion='v1', kind='List', items=items), indent=2))
