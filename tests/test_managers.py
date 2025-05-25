import unittest
from unittest import mock
from datetime import date as Date
from pathlib import PurePath

from pyzync.errors import DataIntegrityError, DataCorruptionError
from pyzync.interfaces import SnapshotRef, SnapshotStream
from pyzync.managers import HostSnapshotManager, FileSnapshotManager


class TestSnapshotRef(unittest.TestCase):

    def test_can_instantiate_HostSnapshotRef(self):
        ref = SnapshotRef(zfs_prefix="tank/foo", date="20250322")

    def test_cannot_create_absolute_root(self):
        with self.assertRaises(ValueError) as ct:
            SnapshotRef(zfs_prefix="/tank/foo", date="20250322")

    def test_cannot_create_with_empty_root(self):
        with self.assertRaises(ValueError) as ct:
            SnapshotRef(zfs_prefix="", date="20250322")


class TestHostSnapshotManager(unittest.TestCase):

    def test_can_manage_host_snapshots(self):
        # query snapshots that are related to the file system
        zfs_root = PurePath("tank0/foo")
        refs = HostSnapshotManager.query(zfs_root)

        # destroy duplicates if they exist
        date = Date.fromisoformat("20250322")
        [HostSnapshotManager.destroy(ref) for ref in refs if ref.date == date]

        # create a new snapshot
        ref = HostSnapshotManager.create(zfs_root, date)
        self.assertEqual(ref.zfs_handle, "tank0/foo@20250322")
        self.assertTrue(ref.date, date)
        self.assertTrue(ref.zfs_prefix, ref.zfs_prefix)

        # get the data stream object
        stream = HostSnapshotManager.send(ref)
        chunk = next(stream.iterable)
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
        paths = ['tank/foo/20250404.zfs', 'tank/foo/20250404_20250405.zfs']
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)

        # assert only one zfs_prefix found
        self.assertEqual(len(tables), 1)

        # assert that the graph goes two deep
        table = tables[0]
        self.assertEqual(table.index[0], Date.fromisoformat('20250404'))

        # lets build a more complex table with multiple roots
        paths = [
            'tank/foo/20250122.zfs',
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250122_20250124.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
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

    def test_can_handle_non_linear_lineage_graphs(self):
        paths = [
            'tank/foo/20250404.zfs',
            'tank/foo/20250404_20250405.zfs',
            'tank/foo/20250405_20250404.zfs',  # loop backwards
            'tank/foo/20250404_20250406.zfs',  # skip two forwards
        ]
        paths = [PurePath(p) for p in paths]
        with self.assertRaises(DataCorruptionError) as ct:
            FileSnapshotManager._compute_lineage_tables(paths)
        self.assertTrue(
            'Incremental snapshot that increments back in time found with filename = 20250405_20250404.zfs'
            in str(ct.exception), 'unexpected error message for snapshot that increments back in time')

    def test_can_query(self):
        # single complete file test
        adapter = mock.NonCallableMagicMock()
        paths = ['test/foo/20250504.zfs']
        adapter.query = lambda _: [PurePath(p) for p in paths]
        refs = FileSnapshotManager.query(adapter)
        self.assertIsInstance(refs[0], SnapshotRef)
        self.assertEqual(refs[0].zfs_handle, 'test/foo@20250504')

        # multiple complete files
        paths = ['test/foo/20250504.zfs', 'test/foo/20250505.zfs']
        refs = FileSnapshotManager.query(adapter)
        self.assertEqual(len(refs), 2)

        # a single chain of snapshots
        paths = [
            'test/foo/20250504.zfs', 'test/foo/20250504_20250505.zfs', 'test/foo/20250505_20250506.zfs'
        ]
        refs = FileSnapshotManager.query(adapter)
        self.assertEqual(len(refs), 3)

        # two chains of snapshots
        paths = [
            'test/foo/20250504.zfs', 'test/foo/20250504_20250505.zfs', 'test/foo/20250505_20250506.zfs',
            'test/bar/20250504.zfs', 'test/bar/20250504_20250505.zfs', 'test/bar/20250505_20250506.zfs'
        ]
        refs = FileSnapshotManager.query(adapter)
        self.assertEqual(len(refs), 6)

        # one good chain and one broken
        paths = [
            'test/foo/20250504.zfs', 'test/foo/20250504_20250505.zfs', 'test/foo/20250505_20250506.zfs',
            'test/bar/20250504.zfs', 'test/bar/20250505_20250506.zfs'
        ]
        # with self.assertRaises(DataCorruptionError) as ct:
        with self.assertWarns(Warning) as ct:
            refs = FileSnapshotManager.query(adapter)

        # two chains and one with a newer complete snapshot
        paths = [
            'test/foo/20250504.zfs', 'test/foo/20250504_20250505.zfs', 'test/foo/20250505.zfs',
            'test/bar/20250504.zfs', 'test/bar/20250504_20250505.zfs', 'test/bar/20250505_20250506.zfs'
        ]
        refs = FileSnapshotManager.query(adapter)
        self.assertEqual(len(refs), 5)

    def test_can_compute_redundent_nodes(self):
        # Please see diagram node_diagram.drawio for a graphical represnetation of the LineageTable

        # case 1
        paths = [
            'tank/foo/20250122.zfs',
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250122_20250124.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        redundent_nodes = FileSnapshotManager._compute_redundent_nodes(table)
        # 124 should be redundent as it is not needed to go from 122 to 126
        # 122_124 should be redundent as 124 can be derived from 123_124 and has a null in the column
        self.assertEqual({rn.filename for rn in redundent_nodes},
                         {'20250124.zfs', '20250122_20250124.zfs'})

        # case 2
        paths = [
            'tank/foo/20250122.zfs',
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250122_20250125.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        redundent_nodes = FileSnapshotManager._compute_redundent_nodes(table)
        # 124 should be redundent as it is not needed to go from 122 to 126
        # 122_125 should be redundent as 125 can be derived from 124_125 and has a null in the column
        self.assertEqual({rn.filename for rn in redundent_nodes},
                         {'20250124.zfs', '20250122_20250125.zfs'})

        # case 3
        paths = [
            'tank/foo/20250122.zfs',
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250121_20250122.zfs',  # this node would exist if ref 121 has just been deleted
            'tank/foo/20250122_20250125.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        with self.assertWarns(Warning) as ct:
            tables = FileSnapshotManager._compute_lineage_tables(paths)
            table = tables[0]
            redundent_nodes = FileSnapshotManager._compute_redundent_nodes(table)

        # case 4 is an invalid version of case 3, where the earliest incremental snapshot end date
        # is prior to the earliest complete snapshot date.
        paths = [
            'tank/foo/20250122.zfs',
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250120_20250121.zfs',  # this node should never exist
            'tank/foo/20250122_20250125.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        with self.assertWarns(Warning) as ct:
            tables = FileSnapshotManager._compute_lineage_tables(paths)
            table = tables[0]
            redundent_nodes = FileSnapshotManager._compute_redundent_nodes(table)

    def test_can_compute_deleteable_refs(self):
        # case 1
        paths = [
            'tank/foo/20250122.zfs',
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250122_20250124.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        tables = FileSnapshotManager._compute_lineage_tables(paths)
        table = tables[0]
        ref = SnapshotRef(date='20250122', zfs_prefix='tank/foo')
        is_deletable = FileSnapshotManager._is_ref_deletable(table, ref)
        self.assertTrue(is_deletable)
        ref = SnapshotRef(date='20250123', zfs_prefix='tank/foo')
        is_deletable = FileSnapshotManager._is_ref_deletable(table, ref)
        self.assertFalse(is_deletable)
        ref = SnapshotRef(date='20250126', zfs_prefix='tank/foo')
        is_deletable = FileSnapshotManager._is_ref_deletable(table, ref)
        self.assertTrue(is_deletable)

    def test_can_destroy(self):
        # case 1
        paths = [
            'tank/foo/20250122.zfs',
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250122_20250124.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        adapter = mock.NonCallableMagicMock()
        adapter.query = lambda _: paths
        adapter.destroy = lambda x: [True] * len(x)

        ref = SnapshotRef(date='20250122', zfs_prefix='tank/foo')
        filepaths, success_flags = FileSnapshotManager.destroy(adapter, ref)
        self.assertTrue(all(success_flags))

        # if I try to delete 123 it should fail because 122 still exists
        ref = SnapshotRef(date='20250123', zfs_prefix='tank/foo')
        with self.assertRaises(DataIntegrityError) as ct:
            FileSnapshotManager.destroy(adapter, ref)

    def test_can_prune(self):
        # pretend we just deleted 122
        paths = [
            'tank/foo/20250123.zfs',
            'tank/foo/20250124.zfs',
            'tank/foo/20250122_20250124.zfs',
            'tank/foo/20250123_20250124.zfs',
            'tank/foo/20250124_20250125.zfs',
            'tank/foo/20250125_20250126.zfs',
        ]
        paths = [PurePath(p) for p in paths]
        adapter = mock.NonCallableMagicMock()
        adapter.query = lambda _: paths
        adapter.destroy = lambda x: [True] * len(x)

        # we should have a warning about orphaned nodes
        with self.assertWarns(Warning) as ct:
            file_paths, success_flags = FileSnapshotManager.prune(adapter, 'tank/foo')
            self.assertTrue(all(success_flags))
            self.assertEqual(set(file_paths), {paths[1], paths[2]})

    def test_can_recv(self):
        ref = SnapshotRef(date='20250404', zfs_prefix='tank/foo')
        stream = SnapshotStream(ref=ref, iterable=[b'somebytes'])
        adapter = mock.NonCallableMagicMock()
        adapter.query = lambda _: []
        adapter.recv = lambda _: stream.ref.zfs_prefix.joinpath(stream.filename)
        filename = FileSnapshotManager.recv(adapter, stream)
        self.assertEqual(filename, PurePath('tank/foo/20250404.zfs'))
