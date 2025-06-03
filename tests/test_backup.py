import unittest
from pathlib import PurePath
from typing import Optional
from datetime import datetime as Datetime

from pyzync.backup import BackupConfig
from pyzync.interfaces import SnapshotRef, SnapshotStream
from pyzync.managers import HostSnapshotManager
from pyzync.storage_adapters import SnapshotStorageAdapter
from pyzync.retention_policies import LastNSnapshotsPolicy


class FakeHostSnapshotManager(HostSnapshotManager):

    def __init__(self, refs: list[SnapshotRef]):
        self._refs = set(refs)

    def create(self, zfs_dataset_path: PurePath, datetime: Datetime):
        ref = SnapshotRef(datetime=datetime, zfs_dataset_path=zfs_dataset_path)
        self._refs.add(ref)
        return ref

    def query(self, zfs_dataset_path: Optional[PurePath] = None):
        return sorted([
            ref for ref in self._refs
            if (zfs_dataset_path is None) or (ref.zfs_dataset_path == zfs_dataset_path)
        ],
                      key=lambda ref: ref.datetime)

    def destroy(self, ref: SnapshotRef):
        self._refs.remove(ref)
        return ref

    def send(self, ref: SnapshotRef, base: SnapshotRef):
        pass


class FakeAdapter(SnapshotStorageAdapter):

    def __init__(self, refs: list[SnapshotRef]):
        self._refs = set(refs)

    def query(self, zfs_dataset_path: Optional[PurePath] = None):
        return sorted([
            ref for ref in self._refs
            if (zfs_dataset_path is None) or (ref.zfs_dataset_path == zfs_dataset_path)
        ],
                      key=lambda ref: ref.datetime)

    def destroy(self, ref: SnapshotRef):
        self._refs.remove(ref)
        return ref

    def send(self, ref: SnapshotRef, base: SnapshotRef):
        pass

    def recv(self, stream: SnapshotStream):
        pass


class TestBackupConfig(unittest.TestCase):

    def test_can_rotate(self):
        config = BackupConfig(zfs_dataset_path='tank0/foo',
                              retention_policy=LastNSnapshotsPolicy(n_snapshots=3),
                              adapters=[FakeAdapter([])])

        # rotate using an empty list should create a new snapshot for the date given
        host = FakeHostSnapshotManager([])
        datetime = Datetime.fromisoformat('20250602T120000')
        config.rotate(host=host, datetime=datetime)
        ref = list(host._refs)[0]
        self.assertEqual(ref.datetime, datetime)

        # rotate again, with a new datetime
        datetime = Datetime.fromisoformat('20250602T120001')
        config.rotate(host=host, datetime=datetime)
        self.assertEqual(len(host._refs), 2)

        # rotate two more times, there should be no more than 3 snapshots
        datetime = Datetime.fromisoformat('20250602T120002')
        config.rotate(host=host, datetime=datetime)
        self.assertEqual(len(host._refs), 3)

        datetime = Datetime.fromisoformat('20250602T120003')
        config.rotate(host=host, datetime=datetime)
        self.assertEqual(len(host._refs), 3)

    def test_can_sync(self):
        pass


if __name__ == '__main__':
    unittest.main(verbosity=2)
