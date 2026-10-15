import copy
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('evidence', Path(__file__).resolve().parents[1] / 'phase5-evidence.py')
evidence = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(evidence)


def fixture():
    result = dict(schema_version=1, timestamp=1000, before={}, after={}, responses={})
    for backend in ('ledger', 'matching'):
        pods = {f'{backend}-{n}': dict(uid=f'uid-{n}', zone=f'zone-{n}') for n in range(3)}
        result['before'][backend] = pods
        result['after'][backend] = copy.deepcopy(pods)
        def vector(security, value):
            return dict(resultType='vector', result=[dict(metric=dict(pod=pod, connection_security_policy=security), value=[1000, value]) for pod in pods])
        result['responses'][backend + '_requests'] = vector('mutual_tls', '200')
        result['responses'][backend + '_scrape'] = vector('', '1')
        result['responses'][backend + '_scrape_age'] = vector('', '15')
    return result


class EvidenceTest(unittest.TestCase):
    def test_healthy(self):
        self.assertEqual(evidence.assess(fixture(), 100, 2, 3), [])

    def test_missing_pod_traffic_cannot_pass(self):
        data = fixture()
        data['responses']['ledger_requests']['result'].pop()
        self.assertTrue(evidence.assess(data, 100, 2, 3))

    def test_security_scrape_inventory_and_sample_failures(self):
        for fault in ('plaintext', 'down', 'missing_scrape', 'churn', 'empty', 'skew', 'stale', 'scrape_age', 'nan', 'future'):
            with self.subTest(fault=fault):
                data = fixture()
                requests = data['responses']['ledger_requests']['result']
                if fault == 'plaintext': requests[0]['metric']['connection_security_policy'] = 'none'
                if fault == 'down': data['responses']['ledger_scrape']['result'][0]['value'][1] = '0'
                if fault == 'missing_scrape': data['responses']['ledger_scrape']['result'].clear()
                if fault == 'churn': data['after']['ledger']['ledger-0']['uid'] = 'replacement'
                if fault == 'empty': data['before']['ledger'].clear()
                if fault == 'skew': requests[0]['value'][1] = '1000'
                if fault == 'stale': requests[0]['value'][0] = 100
                if fault == 'scrape_age': data['responses']['ledger_scrape_age']['result'][0]['value'][1] = '240'
                if fault == 'nan': requests[0]['value'][1] = 'NaN'
                if fault == 'future': requests[0]['value'][0] = 2000
                self.assertTrue(evidence.assess(data, 100, 2, 3))

    def test_counter_dimensions_are_aggregated(self):
        data = fixture()
        rows = data['responses']['ledger_requests']['result']
        extra = copy.deepcopy(rows[0])
        extra['value'][1] = '1'
        rows.append(extra)
        self.assertEqual(evidence.assess(data, 100, 2, 3), [])


if __name__ == '__main__':
    unittest.main()
