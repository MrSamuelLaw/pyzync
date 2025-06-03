import unittest
from unittest import mock
from datetime import datetime as Datetime
from pathlib import PurePath

from pyzync.errors import DataIntegrityError, DataCorruptionError
from pyzync.interfaces import SnapshotRef, SnapshotStream
from pyzync.managers import HostSnapshotManager, FileSnapshotManager


class TestSnapshotRef(unittest.TestCase):

    def test_can_instantiate_HostSnapshotRef(self):
        ref = SnapshotRef(zfs_dataset_path="tank/foo", datetime="20250322T120000")

    def test_cannot_create_absolute_root(self):
        with self.assertRaises(ValueError) as ct:
            SnapshotRef(zfs_dataset_path="/tank/foo", datetime="20250322T120000")

    def test_cannot_create_with_empty_root(self):
        with self.assertRaises(ValueError) as ct:
            SnapshotRef(zfs_dataset_path="", datetime="20250322T120000")


class TestHostSnapshotManager(unittest.TestCase):

    def test_can_manage_host_snapshots(self):
        # query snapshots that are related to the file system
        zfs_root = PurePath("tank0/foo")
        refs = HostSnapshotManager.query(zfs_root)
        [HostSnapshotManager.destroy(ref) for ref in refs]

        # destroy duplicates if they exist
        date = Datetime.fromisoformat("20250322T120000")

        # create a new snapshot
        ref = HostSnapshotManager.create(zfs_root, date)
        self.assertEqual(ref.zfs_snapshot_id, "tank0/foo@20250322T120000")
        self.assertTrue(ref.datetime, date)
        self.assertTrue(ref.zfs_dataset_path, ref.zfs_dataset_path)

        # get the data stream object
        stream = HostSnapshotManager.send(ref)[0]
        chunk = next(stream.snapshot_stream)
        self.assertTrue(type(chunk) is bytes, "Data is not bytes")
        self.assertGreater(len(chunk), 0)

        # re-query the host snapshots and verify that the snapshot exists
        host_refs = HostSnapshotManager.query()
        self.assertIn(ref, host_refs)

        # destroy it
        HostSnapshotManager.destroy(ref)

        # requry and verify that its gone
        host_refs = HostSnapshotManager.query()
        self.assertNotIn(ref, host_refs)


class TestFileSnapshotManager(unittest.TestCase):

    def test_can_build_table_from_linear_lineage_graphs(self):
        # test collecting a single graph
        paths = ['tank/foo/20250404T120000.zfs', 'tank/foo/20250404T120000_20250405T120000.zfs']
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)

        # assert only one zfs_dataset_path found
        self.assertEqual(len(tables), 1)

        # assert that the graph goes two deep
        table = tables[0]
        self.assertEqual(table.index[0], Datetime.fromisoformat('20250404T120000'))

        # lets build a more complex table with multiple roots
        paths = [
            'tank/foo/20250122T120000.zfs',
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250122T120000_20250124T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        rows = tuple(map(tuple, zip(*table.data)))

        # verify that Nones are in the expected places in the table
        self.assertIsNone(rows[0][1])
        self.assertIsNone(rows[0][2])
        self.assertIsNone(rows[1][0])
        self.assertIsNone(rows[1][2])

    def test_raises_exception_on_non_linear_lineage_graph(self):
        paths = [
            'tank/foo/20250404T120000.zfs',
            'tank/foo/20250404T120000_20250405T120000.zfs',
            'tank/foo/20250405T120000_20250404T120000.zfs',  # loop backwards
            'tank/foo/20250404T120000_20250406T120000.zfs',  # skip two forwards
        ]
        paths = [PurePath(p) for p in paths]
        with self.assertRaises(DataCorruptionError) as ct:
            FileSnapshotManager._compute_lineage_tables(paths)
        self.assertTrue(
            'Incremental snapshot that increments back in time found with filename = 20250405T120000_20250404T120000.zfs'
            in str(ct.exception), 'unexpected error message for snapshot that increments back in time')

    def test_can_query(self):
        # single complete file test
        adapter = mock.NonCallableMagicMock()
        paths = ['test/foo/20250504T120000.zfs']
        adapter.query = lambda _: [PurePath(p) for p in paths]
        manager = FileSnapshotManager(adapter)
        refs = manager.query()
        self.assertIsInstance(refs[0], SnapshotRef)
        self.assertEqual(refs[0].zfs_snapshot_id, 'test/foo@20250504T120000')

        # multiple complete files
        paths = ['test/foo/20250504T120000.zfs', 'test/foo/20250505T120000.zfs']
        refs = manager.query()
        self.assertEqual(len(refs), 2)

        # a single chain of snapshots
        paths = [
            'test/foo/20250504T120000.zfs', 'test/foo/20250504T120000_20250505T120000.zfs',
            'test/foo/20250505T120000_20250506T120000.zfs'
        ]
        refs = manager.query()
        self.assertEqual(len(refs), 3)

        # two chains of snapshots
        paths = [
            'test/foo/20250504T120000.zfs', 'test/foo/20250504T120000_20250505T120000.zfs',
            'test/foo/20250505T120000_20250506T120000.zfs', 'test/bar/20250504T120000.zfs',
            'test/bar/20250504T120000_20250505T120000.zfs',
            'test/bar/20250505T120000_20250506T120000.zfs'
        ]
        refs = manager.query()
        self.assertEqual(len(refs), 6)

        # one good chain and one broken
        paths = [
            'test/foo/20250504T120000.zfs', 'test/foo/20250504T120000_20250505T120000.zfs',
            'test/foo/20250505T120000_20250506T120000.zfs', 'test/bar/20250504T120000.zfs',
            'test/bar/20250505T120000_20250506T120000.zfs'
        ]
        # will log some stuff
        refs = manager.query()

        # two chains and one with a newer complete snapshot
        paths = [
            'test/foo/20250504T120000.zfs', 'test/foo/20250504T120000_20250505T120000.zfs',
            'test/foo/20250505T120000.zfs', 'test/bar/20250504T120000.zfs',
            'test/bar/20250504T120000_20250505T120000.zfs',
            'test/bar/20250505T120000_20250506T120000.zfs'
        ]
        refs = manager.query()
        self.assertEqual(len(refs), 5)

    def test_can_compute_redundant_nodes(self):
        # Please see diagram node_diagram.drawio for a graphical represnetation of the LineageTable

        # case 1
        paths = [
            'tank/foo/20250122T120000.zfs',
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250122T120000_20250124T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        redundant_nodes = FileSnapshotManager._compute_redundant_nodes(table)
        # 124 should be redundant as it is not needed to go from 122 to 126
        # 122_124 should be redundant as 124 can be derived from 123_124 and has a null in the column
        self.assertEqual({rn.filename for rn in redundant_nodes},
                         {'20250124T120000.zfs', '20250122T120000_20250124T120000.zfs'})

        # case 2
        paths = [
            'tank/foo/20250122T120000.zfs',
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250122T120000_20250125T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        redundant_nodes = FileSnapshotManager._compute_redundant_nodes(table)
        # 124 should be redundant as it is not needed to go from 122 to 126
        # 122_125 should be redundant as 125 can be derived from 124_125 and has a null in the column
        self.assertEqual({rn.filename for rn in redundant_nodes},
                         {'20250124T120000.zfs', '20250122T120000_20250125T120000.zfs'})

        # case 3
        paths = [
            'tank/foo/20250122T120000.zfs',
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250121T120000_20250122T120000.zfs',  # this node would exist if ref 121 has just been deleted
            'tank/foo/20250122T120000_20250125T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        # will generate some warnings
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        redundant_nodes = FileSnapshotManager._compute_redundant_nodes(table)

        # case 4 is an invalid version of case 3, where the earliest incremental snapshot end date
        # is prior to the earliest complete snapshot date.
        paths = [
            'tank/foo/20250122T120000.zfs',
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250120T120000_20250121T120000.zfs',  # this node should never exist
            'tank/foo/20250122T120000_20250125T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        # will generate some warning logs
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        redundant_nodes = FileSnapshotManager._compute_redundant_nodes(table)

    def test_can_compute_deleteable_refs(self):
        # case 1
        paths = [
            'tank/foo/20250122T120000.zfs',
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250122T120000_20250124T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        ref = SnapshotRef(datetime='20250122T120000', zfs_dataset_path='tank/foo')
        is_deletable = FileSnapshotManager._is_ref_deletable(table, ref)
        self.assertTrue(is_deletable)
        ref = SnapshotRef(datetime='20250123T120000', zfs_dataset_path='tank/foo')
        is_deletable = FileSnapshotManager._is_ref_deletable(table, ref)
        self.assertFalse(is_deletable)
        ref = SnapshotRef(datetime='20250126T120000', zfs_dataset_path='tank/foo')
        is_deletable = FileSnapshotManager._is_ref_deletable(table, ref)
        self.assertTrue(is_deletable)

    def test_can_destroy(self):
        # case 1
        paths = [
            'tank/foo/20250122T120000.zfs',
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250122T120000_20250124T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        adapter = mock.NonCallableMagicMock()
        adapter.query = lambda _: paths
        adapter.destroy = lambda x: [True] * len(x)
        manager = FileSnapshotManager(adapter)

        ref = SnapshotRef(datetime='20250122T120000', zfs_dataset_path='tank/foo')
        filepaths, success_flags = manager.destroy(ref)
        self.assertTrue(all(success_flags))

        # if I try to delete 123 it should fail because 122 still exists
        ref = SnapshotRef(datetime='20250123T120000', zfs_dataset_path='tank/foo')
        with self.assertRaises(DataIntegrityError) as ct:
            manager.destroy(ref)

    def test_can_prune(self):
        # pretend we just deleted 122
        paths = [
            'tank/foo/20250123T120000.zfs',
            'tank/foo/20250124T120000.zfs',
            'tank/foo/20250122T120000_20250124T120000.zfs',
            'tank/foo/20250123T120000_20250124T120000.zfs',
            'tank/foo/20250124T120000_20250125T120000.zfs',
            'tank/foo/20250125T120000_20250126T120000.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        adapter = mock.NonCallableMagicMock()
        adapter.query = lambda _: paths
        adapter.destroy = lambda x: [True] * len(x)
        manager = FileSnapshotManager(adapter)

        # will generate some logs
        file_paths, success_flags = manager.prune('tank/foo')
        self.assertTrue(all(success_flags))
        self.assertEqual(set(file_paths), {paths[1], paths[2]})

    def test_can_recv(self):
        ref = SnapshotRef(datetime='20250404T120000', zfs_dataset_path='tank/foo')
        stream = SnapshotStream(ref=ref, snapshot_stream=[b'somebytes'])
        adapter = mock.NonCallableMagicMock()
        adapter.query = lambda _: []
        adapter.recv = lambda _: stream.ref.zfs_dataset_path.joinpath(stream.filename)
        manager = FileSnapshotManager(adapter)
        filenames = manager.recv([stream])
        self.assertEqual(filenames, [PurePath('tank/foo/20250404T120000.zfs')])


if __name__ == '__main__':
    unittest.main(verbosity=2)
