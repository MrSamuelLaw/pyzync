import unittest
from datetime import date as Date
from pathlib import Path, PurePath

from pyzync.managers import HostSnapshotManager, FileSnapshotManager
from pyzync.storage_adapters import LocalFileSnapshotDataAdapter


class TestLocalFileSnapshotDataAdapter(unittest.TestCase):

    def test_can_manage_local_snapshots(self):
        # query snapshots that are related to the file system
        zfs_root = PurePath("tank0/foo")
        refs = HostSnapshotManager.query(zfs_root)

        # destroy duplicates if they exist
        date = Date.fromisoformat("20250322")
        [HostSnapshotManager.destroy(ref) for ref in refs if ref.date == date]

        # create a new snapshot
        ref = HostSnapshotManager.create(zfs_root, date)

        # get the data stream object
        stream = HostSnapshotManager.send(ref)
        directory = Path(__file__).resolve().parent.joinpath('local_storage_adapter_files')
        adapter = LocalFileSnapshotDataAdapter(directory)
        filepath = FileSnapshotManager.recv(adapter, stream)
        self.assertEqual(filepath.name, '20250322.zfs')

        # destroy the snapshot from the host
        HostSnapshotManager.destroy(ref)

        # destroy from the local file system
        FileSnapshotManager.destroy(adapter, ref)

        # destroy the snapshot from the local filesystem
