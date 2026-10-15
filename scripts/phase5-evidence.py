#!/usr/bin/env python3
"""Capture read-only mesh evidence; assess only traffic, mTLS and scrape coverage."""
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from urllib.parse import urlencode


def run(*args):
    return subprocess.check_output(args, text=True, timeout=60)


def inventory(context):
    result = {}
    for backend in ('ledger', 'matching'):
        response = json.loads(run('kubectl', '--context', context, 'get', 'endpointslice',
                                  '-n', 'praxis', '-l', f'kubernetes.io/service-name={backend}', '-o', 'json'))
        pods = {}
        for item in response['items']:
            for endpoint in item.get('endpoints', []):
                conditions = endpoint.get('conditions', {})
                if conditions.get('ready') is not True or conditions.get('terminating') is True:
                    continue
                ref = endpoint.get('targetRef', {})
                if ref.get('kind') != 'Pod' or not ref.get('name') or not ref.get('uid'):
                    raise ValueError('Ready endpoint lacks a Pod identity')
                pods[ref['name']] = {'uid': ref['uid'], 'zone': endpoint.get('zone', 'unknown')}
        result[backend] = pods
    return result


def collect(context, environment, endpoint, region):
    if not endpoint.startswith('https://aps-workspaces.') or '/workspaces/' not in endpoint:
        raise ValueError('AMP_ENDPOINT must be an HTTPS AMP workspace endpoint')
    before = inventory(context)
    timestamp = time.time()
    queries = {}
    for backend in ('ledger', 'matching'):
        scope = ('environment=' + json.dumps(environment) + ',namespace="praxis",reporter="destination",'
                 'source_workload="order",source_workload_namespace="praxis",'
                 f'destination_workload="{backend}",destination_workload_namespace="praxis"')
        queries[backend + '_requests'] = ('sum by (pod, connection_security_policy) '
                                         f'(increase(istio_requests_total{{{scope}}}[10m]))')
        queries[backend + '_scrape'] = ('up{job="praxis-istio-proxies",namespace="praxis",'
                                        f'environment={json.dumps(environment)},service="{backend}"' + '}')
        # Instant-vector timestamps reflect evaluation time, not scrape time.
        # Query timestamp(up) explicitly so stale successful scrapes cannot pass.
        queries[backend + '_scrape_age'] = 'time() - timestamp(' + queries[backend + '_scrape'] + ')'
    responses = {}
    for name, query in queries.items():
        response = json.loads(run('awscurl', '--region', region, '--service', 'aps', '-X', 'POST',
                                  endpoint.rstrip('/') + '/api/v1/query', '--header',
                                  'Content-Type: application/x-www-form-urlencoded', '--data',
                                  urlencode({'query': query, 'time': str(timestamp)})))
        if response.get('status') != 'success' or response.get('warnings'):
            raise ValueError(f'AMP query {name} failed or returned warnings')
        responses[name] = response['data']
    return dict(schema_version=1, context=context, environment=environment, timestamp=timestamp,
                captured_at=datetime.now(timezone.utc).isoformat(), before=before,
                after=inventory(context), queries=queries, responses=responses)


def assess(evidence, min_requests, max_share_ratio, min_endpoints):
    failures = []
    if evidence.get('schema_version') != 1:
        raise ValueError('Unsupported evidence schema')
    timestamp = float(evidence['timestamp'])
    if not math.isfinite(timestamp):
        raise ValueError('Invalid query timestamp')
    for backend in ('ledger', 'matching'):
        pods = evidence['before'][backend]
        if pods != evidence['after'][backend]:
            failures.append(f'{backend}: endpoint inventory changed during capture; repeat in a stable window')
        if len(pods) < min_endpoints:
            failures.append(f'{backend}: fewer than {min_endpoints} ready endpoints')
        totals = dict.fromkeys(pods, 0.0)
        for suffix in ('requests', 'scrape', 'scrape_age'):
            data = evidence['responses'][backend + '_' + suffix]
            if data.get('resultType') != 'vector':
                raise ValueError('Expected an instant vector')
            healthy = set()
            for series in data['result']:
                pod = series['metric'].get('pod', '')
                sample_time, raw = series['value']
                value = float(raw)
                if not math.isfinite(value) or value < 0 or not 0 <= timestamp - float(sample_time) <= 180:
                    failures.append(f'{backend}/{pod}: invalid or stale {suffix} sample')
                    continue
                if suffix == 'requests':
                    if pod in totals:
                        totals[pod] += value
                    if value > 0 and series['metric'].get('connection_security_policy') != 'mutual_tls':
                        failures.append(f'{backend}/{pod}: traffic lacks mutual_tls')
                elif pod in pods and ((suffix == 'scrape' and value == 1) or (suffix == 'scrape_age' and value <= 180)):
                    healthy.add(pod)
                elif pod in pods:
                    failures.append(f'{backend}/{pod}: unhealthy or stale scrape target')
            if suffix in ('scrape', 'scrape_age'):
                for pod in pods.keys() - healthy:
                    failures.append(f'{backend}/{pod}: no healthy scrape sample')
        for pod, total in totals.items():
            if total < min_requests:
                failures.append(f'{backend}/{pod}: {total:.2f} requests, requires {min_requests}')
        if totals and min(totals.values()) > 0 and max(totals.values()) / min(totals.values()) > max_share_ratio:
            failures.append(f'{backend}: observed max/min request ratio exceeds {max_share_ratio}')
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, help='Reassess a saved evidence capture offline')
    parser.add_argument('--output', type=Path, required=True, help='New JSON evidence file; never overwritten')
    parser.add_argument('--context', help='Explicit kubectl context for live capture')
    parser.add_argument('--environment', help='Exact collector environment label')
    parser.add_argument('--min-requests', type=int, default=100, help='Required requests per ready pod over ten minutes')
    parser.add_argument('--min-endpoints', type=int, default=3)
    parser.add_argument('--max-share-ratio', type=float, default=2.0)
    args = parser.parse_args()
    if args.min_requests < 1 or args.min_endpoints < 1 or not math.isfinite(args.max_share_ratio) or args.max_share_ratio < 1:
        parser.error('Positive sample/endpoint requirements and a finite share ratio >= 1 are required')
    try:
        if args.input:
            evidence = json.loads(args.input.read_text())
        else:
            if not args.context or not args.environment or not re.fullmatch(r'[a-zA-Z0-9_.-]+', args.environment):
                parser.error('Live capture requires --context and a DNS-style --environment label')
            evidence = collect(args.context, args.environment, os.environ['AMP_ENDPOINT'], os.environ['AWS_REGION'])
        failures = assess(evidence, args.min_requests, args.max_share_ratio, args.min_endpoints)
        evidence['assessment'] = dict(passed=not failures, failures=failures,
                                      thresholds=dict(min_requests=args.min_requests, min_endpoints=args.min_endpoints,
                                                      max_share_ratio=args.max_share_ratio),
                                      scope='Observed traffic distribution, mTLS and scrape coverage only; not Phase 5 acceptance')
        with args.output.open('x') as target:
            json.dump(evidence, target, indent=2, allow_nan=False)
            target.write('\n')
        print(json.dumps(evidence['assessment'], indent=2))
        return 1 if failures else 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f'Evidence capture failed: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
