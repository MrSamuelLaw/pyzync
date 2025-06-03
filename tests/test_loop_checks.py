"""
This file is used to test the app as a whole, where it interfaces directly with the host.
The host is expeted to have two datasets named test0/foo and test0/bar.
"""

import unittest
from datetime import datetime as Datetime
from pathlib import Path, PurePath

from pyzync.managers import HostSnapshotManager, FileSnapshotManager
from pyzync.storage_adapters import LocalFileSnapshotDataAdapter


class TestLocalFileLoopCheck(unittest.TestCase):

    def test_can_backup_and_restore_using_incremental_backups(self):
        """Test that ensures that data is able to be sent to a local file
        and used to recover lost data.
        """

        # get an instance of the adapter
        directory = Path(__file__).resolve().parent
        directory = directory.joinpath('local_storage_adapter_files')
        adapter = LocalFileSnapshotDataAdapter(directory=directory)
        manager = FileSnapshotManager(adapter)

        # clean up any lingering files
        refs = manager.query()
        refs = sorted(refs, key=lambda r: r.datetime, reverse=True)
        [manager.destroy(r) for r in refs]

        # query snapshots that are related to the file system
        zfs_root = PurePath("tank0/foo")
        refs = HostSnapshotManager.query(zfs_root)
        [HostSnapshotManager.destroy(ref) for ref in refs]

        # case 1 from the node_diagram
        datetimes = [
            '20250122T120000',
            '20250123T120000',
            '20250124T120000',
            '20250125T120000',
            '20250126T120000',
        ]
        refs = []
        # add a fake file for testing for each date
        for d in datetimes:
            path = Path(f'/tank0/foo/{d}.txt')
            path.touch(exist_ok=True)
            refs.append(HostSnapshotManager.create(zfs_root, d))
            path.unlink()

        # create the first complete stream
        streams = HostSnapshotManager.send(refs[0])

        # create the incremental streams
        streams.extend([HostSnapshotManager.send(ref, base)[0] for base, ref in zip(refs, refs[1:])])

        # send those streams to local storage
        filepaths = manager.recv(streams)

        # verify the ref is queryable now that it exists
        file_refs = manager.query()
        self.assertEqual(set(refs), set(file_refs))

        # destroy the snapshots from the host
        [HostSnapshotManager.destroy(ref) for ref in refs]

        # verify that the dataset is empty
        path = Path('/tank0/foo')
        self.assertFalse(any(path.iterdir()))

        # restore the files in order and check that the directory contains the file expected
        refs = manager.query()
        ref = max(refs, key=lambda r: r.datetime)
        streams = manager.send(ref)
        for s, d in zip(streams, datetimes):
            HostSnapshotManager.recv([s])
            path = Path(f'/tank0/foo/{d}.txt')
            self.assertTrue(path.exists())

        # verify that every snapshot was received
        path = Path(f'/tank0/foo/{datetimes[-1]}.txt')
        self.assertTrue(path.exists())
