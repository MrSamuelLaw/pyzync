import unittest

from pyzync.interfaces import SnapshotNode, SnapshotGraph
from pyzync.retention.policies.last_n import LastNSnapshotsPolicy


class TestRetentionPolicies(unittest.TestCase):

    def test_last_n_snapshots_policy(self):
        policy = LastNSnapshotsPolicy(n_snapshots=2)
        nodes = [
            SnapshotNode(dt='20250601T120000', dataset_id='tank0/foo'),
            SnapshotNode(dt='20250602T120000', dataset_id='tank0/foo'),
        ]
        graph = SnapshotGraph(dataset_id='tank0/foo')
        [graph.add(n) for n in nodes]
        keep, destroy = policy.split(graph)
        self.assertEqual(keep, set(nodes))
        self.assertEqual(destroy, set())

        nodes.append(SnapshotNode(dt='20250603T120000', dataset_id='tank0/foo'))
        graph.add(nodes[-1])
        keep, destroy = policy.split(graph)
        self.assertEqual(keep, set(nodes[1:]))
        self.assertEqual(destroy, set(nodes[0:1]))


if __name__ == '__main__':
    unittest.main(verbosity=2)
