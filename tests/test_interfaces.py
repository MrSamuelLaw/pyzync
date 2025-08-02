import unittest
from datetime import datetime

from pyzync.interfaces import (ZfsDatasetId, ZfsFilePath, ZfsSnapshotId, Datetime, SnapshotNode,
                               SnapshotStream, SnapshotGraph)


class TestZfsDatasetId(unittest.TestCase):

    def test_can_create_valid(self):
        path = 'tank0/foo'
        dspath = ZfsDatasetId(path)
        self.assertEqual(path, dspath)

    def test_fails_with_leading_slash(self):
        path = '/tank0/foo'
        with self.assertRaises(ValueError) as ct:
            ZfsDatasetId(path)
        msg = str(ct.exception)
        self.assertTrue("ZfsDatasetPath must not start with '.' or '/'" in msg,
                        "Unexpected error message for ZfsDatasetPath with leading slash")

    def test_fails_with_leading_dot(self):
        path = './tank0/foo'
        with self.assertRaises(ValueError) as ct:
            ZfsDatasetId(path)
        msg = str(ct.exception)
        self.assertTrue("ZfsDatasetPath must not start with '.' or '/'" in msg,
                        "Unexpected error message for ZfsDatasetPath with leading slash")


class TestZfsFilePath(unittest.TestCase):

    def test_can_create_valid(self):
        path = 'tank0/foo/20250505T120000.zfs'
        fpath = ZfsFilePath(path)
        self.assertEqual(path, fpath)

    def test_fails_with_out_file_extension(self):
        path = 'tank0/foo/20250505T120000'
        with self.assertRaises(ValueError):
            ZfsFilePath(path)

    def test_fails_with_invalid_datetime(self):
        path = 'tank0/foo/20250505T250000.zfs'
        with self.assertRaisesRegex(ValueError, 'hour must be in 0..23'):
            ZfsFilePath(path)


class TestZfsSnapshotId(unittest.TestCase):

    def test_can_create_valid(self):
        str_id = 'tank0/foo@20250505T120000'
        snapshot_id = ZfsSnapshotId(str_id)
        self.assertEqual(snapshot_id, str_id)

    def test_fails_with_missing_at_symbol(self):
        str_id = 'tank0/foo20250505T120000'
        with self.assertRaises(ValueError):
            ZfsSnapshotId(str_id)


class TestDatetime(unittest.TestCase):

    def test_can_create(self):
        dt = Datetime('20250505T120000')
        self.assertEqual(dt.year, 2025)
        self.assertEqual(dt.month, 5)
        self.assertEqual(dt.day, 5)
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.minute, 0)
        self.assertEqual(dt.second, 0)
        self.assertEqual(dt.microsecond, 0)


class TestSnapshotNode(unittest.TestCase):

    def test_can_create_from_iso_string(self):
        node = SnapshotNode(dt='20250505T120000', dataset_id='tank0/foo')
        self.assertEqual(node.dt.year, 2025)
        self.assertEqual(node.dt.month, 5)
        self.assertEqual(node.dt.day, 5)
        self.assertEqual(node.dt.hour, 12)
        self.assertEqual(node.dt.minute, 0)
        self.assertEqual(node.dt.second, 0)
        self.assertEqual(node.dt.microsecond, 0)
        self.assertEqual(node.dataset_id, 'tank0/foo')

    def test_can_create_from_datetime(self):
        dt = datetime(2025, 5, 5, 12, 0, 0)
        node = SnapshotNode(dt=dt, dataset_id='tank0/foo')
        self.assertEqual(node.dt.year, 2025)
        self.assertEqual(node.dt.month, 5)
        self.assertEqual(node.dt.day, 5)
        self.assertEqual(node.dt.hour, 12)
        self.assertEqual(node.dt.minute, 0)
        self.assertEqual(node.dt.second, 0)
        self.assertEqual(node.dt.microsecond, 0)
        self.assertEqual(node.dataset_id, 'tank0/foo')

    def test_fails_with_newer_parent(self):
        with self.assertRaises(ValueError) as ct:
            SnapshotNode(dt='20250505T120001', parent_dt='20250505T120000')

    def test_can_build_from_snapshot_id(self):
        node = SnapshotNode.from_zfs_snapshot_id('tank0/foo@20250505T120000')

    def test_can_build_from_complete_filepath(self):
        node = SnapshotNode.from_zfs_filepath('tank0/foo/20250505T120000.zfs')
        self.assertEqual(node.dataset_id, 'tank0/foo')
        self.assertIsNone(node.parent_dt)
        self.assertEqual(node.dt, Datetime('20250505T120000'))

    def test_can_build_from_incremental_filepath(self):
        node = SnapshotNode.from_zfs_filepath('tank0/foo/20250505T120000_20250505T120001.zfs')
        self.assertEqual(node.dataset_id, 'tank0/foo')
        self.assertEqual(node.parent_dt, Datetime('20250505T120000'))
        self.assertEqual(node.dt, Datetime('20250505T120001'))


class TestSnapshotGraph(unittest.TestCase):

    def test_can_build_single_node_graph(self):
        graph = SnapshotGraph(dataset_id='tank0/foo')
        node = SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000')
        graph.add(node)
        self.assertIn(node, graph.get_nodes())

    def test_can_remove_nodes(self):
        graph = SnapshotGraph(dataset_id='tank0/foo')
        node = SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000')
        graph.add(node)
        self.assertIn(node, graph.get_nodes())
        graph.remove(node)
        self.assertEqual(len(graph.get_nodes()), 0)

    def test_can_build_multi_root_node_graph(self):
        graph = SnapshotGraph(dataset_id='tank0/foo')
        nodes = [
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002')
        ]
        [graph.add(node) for node in nodes]
        self.assertEqual(set(nodes), graph.get_nodes())

    def test_can_build_single_chain_graph(self):
        graph = SnapshotGraph(dataset_id='tank0/foo')
        nodes = {
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001', parent_dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002', parent_dt='20250505T120001')
        }
        [graph.add(node) for node in nodes]
        self.assertEqual(set(nodes), graph.get_nodes())

    def test_can_build_split_chain_graph(self):
        # case 4 from the node_diagram
        graph = SnapshotGraph(dataset_id='tank0/foo')
        nodes = [
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001', parent_dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002', parent_dt='20250505T120001'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002', parent_dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120003', parent_dt='20250505T120002'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120004', parent_dt='20250505T120003')
        ]
        [graph.add(node) for node in nodes]
        chains = graph.get_chains()
        self.assertEqual(len(chains), 2)
        self.assertEqual({len(c) for c in chains}, {4, 5})

        # case 1 from the node graph
        graph = SnapshotGraph(dataset_id='tank0/foo')
        nodes = [
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002', parent_dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002', parent_dt='20250505T120001'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120003', parent_dt='20250505T120002'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120004', parent_dt='20250505T120003')
        ]
        [graph.add(node) for node in nodes]
        chains = graph.get_chains()
        self.assertEqual(len(chains), 3)
        self.assertEqual({len(c) for c in chains}, {4, 3})

    def test_can_compute_orphaned_nodes(self):
        # this is what a tree might look like after rolling the snapshots forward by one
        graph = SnapshotGraph(dataset_id='tank0/foo')
        nodes = [
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001', parent_dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002', parent_dt='20250505T120001'),
        ]
        [graph.add(node) for node in nodes]
        orphans = graph.get_orphans()
        self.assertEqual(len(orphans), 0)
        graph.remove(nodes[0])
        orphans = graph.get_orphans()
        self.assertEqual(len(orphans), 2)
        self.assertEqual(orphans, set(nodes[1:]))

    def test_does_remove_entry_when_group_is_empty(self):
        graph = SnapshotGraph(dataset_id='tank0/foo')
        nodes = [
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001'),
        ]
        [graph.add(n) for n in nodes]
        graph.remove(nodes[0])
        self.assertTrue(nodes[0].dt not in graph._table.keys(),
                        'Group expected to be removed upon last node bting deleted')


class TestSnapshotStream(unittest.TestCase):

    def test_can_build_stream(self):
        node = SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000')
        stream = SnapshotStream(node=node, bytes_stream=(x for x in b'somebytes'))
        self.assertEqual(stream.node, node)

    def test_can_register_and_publish(self):

        context = 0

        def consumer():
            nonlocal context
            while True:
                chunk = yield
                if chunk is None:
                    break
                context += 1

        consumer = consumer()
        next(consumer)

        node = SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000')
        stream = SnapshotStream(node=node, bytes_stream=(bytes(x) for x in b'0123456789'))
        stream.register(consumer)
        stream.publish()
        self.assertEqual(context, 10)


if __name__ == '__main__':
    unittest.main(verbosity=2)
