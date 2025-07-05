import unittest
from typing import Optional

from pyzync.errors import DataIntegrityError
from pyzync.interfaces import SnapshotStorageAdapter, ZfsDatasetId, SnapshotStream, SnapshotGraph, SnapshotNode
from pyzync.storage_adapters import RemoteSnapshotManager


class FakeStorageAdapter(SnapshotStorageAdapter):

    def query(self, dataset_id: Optional[ZfsDatasetId]):
        dataset_id = dataset_id or ZfsDatasetId('tank0/foo')
        filenames = [
            '20250505T120000.zfs',
            '20250505T120001.zfs',
            '20250505T120002.zfs',
        ]
        filepaths = [dataset_id + '/' + fn for fn in filenames]
        return filepaths

    def destroy(self, *args, **kwargs):
        pass

    def send(self, *args, **kwargs):
        return [b'fake_bytes']

    def recv(self, *args, **kwargs):
        pass


class TestFileSnapshotManager(unittest.TestCase):

    def test_can_use_file_snapshot_manager(self):
        manager = RemoteSnapshotManager(adapter=FakeStorageAdapter())

        # verify the query works
        graphs = manager.query()
        self.assertEqual(len(graphs), 1)

        graph = graphs[0]
        self.assertEqual(len(graph.get_nodes()), 3)
        self.assertEqual(len(graph.get_chains()), 3)

        # verify the send works
        stream = manager.send(graph.get_nodes().pop(), graph)

        # verify the destroy works
        [manager.destroy(node, graph, force=True) for node in graph.get_nodes()]
        self.assertEqual(len(graph.get_nodes()), 0)
        self.assertEqual(len(graph.get_chains()), 0)

    def test_can_prune_on_destroy(self):
        # build the graph
        snapshots = [
            'tank0/foo/20250505T120000.zfs', 'tank0/foo/20250505T120000_20250505T120001.zfs',
            'tank0/foo/20250505T120001_20250505T120002.zfs', 'tank0/foo/20250505T120002.zfs',
            'tank0/foo/20250505T120002_20250505T120003.zfs'
        ]
        nodes = [SnapshotNode.from_zfs_filepath(ss) for ss in snapshots]
        graph = SnapshotGraph(dataset_id='tank0/foo')
        [graph.add(node) for node in nodes]
        # destroy the first 3 in the chain using a fake adapter
        manager = RemoteSnapshotManager(adapter=FakeStorageAdapter())
        manager.destroy(nodes[0], graph, prune=True)
        # check that only one chain exists
        chains = graph.get_chains()
        self.assertEqual(len(chains), 1)
        self.assertEqual(chains[0], nodes[-2:])

    def test_requires_force_to_delete_last_complete_snapshot(self):
        # build the graph
        snapshots = [
            'tank0/foo/20250505T120000.zfs',
            'tank0/foo/20250505T120000_20250505T120001.zfs',
        ]
        nodes = [SnapshotNode.from_zfs_filepath(ss) for ss in snapshots]
        graph = SnapshotGraph(dataset_id='tank0/foo')
        [graph.add(node) for node in nodes]
        # # attempt to destroy the root node
        manager = RemoteSnapshotManager(adapter=FakeStorageAdapter())
        with self.assertRaisesRegex(DataIntegrityError, 'must set force = True to delete node'):
            manager.destroy(nodes[0], graph, prune=True)
        manager.destroy(nodes[0], graph, prune=True, force=True)
        self.assertEqual(graph.get_nodes(), set())

    def test_can_roll_a_snapshot_forward(self):
        # build the graph
        snapshots = [
            'tank0/foo/20250505T120000.zfs',
            'tank0/foo/20250505T120000_20250505T120001.zfs',
            'tank0/foo/20250505T120001_20250505T120002.zfs',
        ]
        nodes = [SnapshotNode.from_zfs_filepath(ss) for ss in snapshots]
        graph = SnapshotGraph(dataset_id='tank0/foo')
        [graph.add(node) for node in nodes]
        # add a new nodes using a fake adapter
        manager = RemoteSnapshotManager(adapter=FakeStorageAdapter())
        new_node = SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001')
        stream = SnapshotStream(node=new_node, bytes_stream=(b'some_bytes',))
        manager.recv(stream, graph)
        manager.destroy(nodes[0], graph, prune=True)
        chains = graph.get_chains()
        # only one chain should exist
        self.assertEqual(len(chains), 1)
        # the first node should be the new node, not 20250505T120000
        self.assertEqual(chains[0][0], new_node)
        # the last node should still be the last node
        self.assertEqual(chains[0][-1], nodes[-1])
        # prune should have removed the 00 -> 01 link
        self.assertFalse(graph.get_orphans())
        self.assertFalse(nodes[1] in graph.get_nodes())

    # def test_can_recv_a_stream_into_chunks(self):
    #     self.assertTrue(False)

    # def test_can_query_chunked_streams(self):
    #     self.assertTrue(False)


if __name__ == '__main__':
    unittest.main(verbosity=2)
