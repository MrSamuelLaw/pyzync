"""
Docs go here
"""

import unittest

from pyzync.interfaces import SnapshotRef
from pyzync.retention_policies import LastNSnapshotsPolicy


class TestRetentionPolicies(unittest.TestCase):

    def test_last_n_snapshots_policy(self):
        policy = LastNSnapshotsPolicy(n_snapshots=2)
        refs = [
            SnapshotRef(datetime='20250601T120000', zfs_dataset_path='tank0/foo'),
            SnapshotRef(datetime='20250602T120000', zfs_dataset_path='tank0/foo'),
        ]
        keep, destroy = policy.split(refs=refs)
        self.assertEqual(set(keep), set(refs))
        self.assertEqual(destroy, [])

        refs.append(SnapshotRef(datetime='20250603T120000', zfs_dataset_path='tank0/foo'))
        keep, destroy = policy.split(refs=refs)
        self.assertEqual(set(keep), set(refs[1:]))
        self.assertEqual(set(destroy), set(refs[0:1]))


if __name__ == '__main__':
    unittest.main(verbosity=2)
