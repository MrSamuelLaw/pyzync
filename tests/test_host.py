import unittest

from pyzync.interfaces import SnapshotNode, Datetime, SnapshotGraph, SnapshotStream
from pyzync.host import HostSnapshotManager


class TestHostSnapshotManager(unittest.TestCase):

    def test_can_manage_host_snapshots_with_dryrun(self):
        """Tests that create, destroy, send, and recv work using dryrun"""
        # query snapshots that are related to the file system
        dataset_id = "tank0/foo"
        graph = SnapshotGraph(dataset_id=dataset_id)
        nodes = [
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120000'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120001'),
            SnapshotNode(dataset_id='tank0/foo', dt='20250505T120002')
        ]
        [graph.add(node) for node in nodes]

        # destroy all host snapshots
        [HostSnapshotManager.destroy(node, graph, dryrun=True) for node in nodes]
        self.assertEqual(len(graph.get_nodes()), 0)

        dt = "20250322T120000"
        node = HostSnapshotManager.create(dt, graph, dryrun=True)
        self.assertEqual(graph.get_nodes(), set([node]))
        self.assertEqual(node.snapshot_id, dataset_id + '@' + dt)
        self.assertEqual(node.dt, Datetime(dt))
        self.assertEqual(node.dataset_id, dataset_id)

        # get the data stream object
        stream = HostSnapshotManager.send(dt, graph, dryrun=True)
        chunk = next(stream.bytes_stream)
        self.assertTrue(type(chunk) is bytes, "Data is not bytes")
        self.assertGreater(len(chunk), 0)

        # destroy the node
        HostSnapshotManager.destroy(node, graph, dryrun=True)

        # verify that the graph doesn't have nodes
        self.assertFalse(graph.get_nodes())

    def test_can_fail_with_duplicate_create(self):
        """Tests that attempting to create duplicate nodes raise an exception"""
        dataset_id = "tank0/foo"
        graph = SnapshotGraph(dataset_id=dataset_id)

        # Create initial node
        dt = "20250322T120000"
        HostSnapshotManager.create(dt, graph, dryrun=True)

        # Attempt to create duplicate node - should raise ValueError
        with self.assertRaisesRegex(ValueError, "Cannot add duplicate node"):
            HostSnapshotManager.create(dt, graph, dryrun=True)

    def test_can_fail_with_duplicate_destroy(self):
        """Tests that attempting to destroy the same node twice raises an exception"""
        dataset_id = "tank0/foo"
        graph = SnapshotGraph(dataset_id=dataset_id)
        dt = "20230101T000000"
        node = SnapshotNode(dataset_id=dataset_id, dt=dt)
        graph.add(node)

        # First destroy should succeed
        HostSnapshotManager.destroy(node, graph, dryrun=True)

        # Second destroy should raise ValueError since node was already removed from graph
        with self.assertRaisesRegex(ValueError, "Unable to remove node"):
            HostSnapshotManager.destroy(node, graph, dryrun=True)

    def test_can_manage_with_host(self):
        # query snapshots that are related to dataset and destroy them
        dataset_id = "tank0/foo"
        graph = HostSnapshotManager.query(dataset_id)[0]
        [HostSnapshotManager.destroy(node, graph) for node in graph.get_nodes()]

        # create some new nodes and verify they were created correctly
        nodes = [
            SnapshotNode(dataset_id=dataset_id, dt='20250505T120000'),
            SnapshotNode(dataset_id=dataset_id, dt='20250505T120001'),
            SnapshotNode(dataset_id=dataset_id, dt='20250505T120002')
        ]
        new_nodes = [HostSnapshotManager.create(node.dt, graph) for node in nodes]
        self.assertEqual(nodes, new_nodes)

        # test that the new nodes show up in a query
        query_nodes = HostSnapshotManager.query(dataset_id)[0].get_nodes()
        self.assertEqual(query_nodes, set(new_nodes))

        parent = nodes[0]
        stream = HostSnapshotManager.send(parent.dt, graph)
        in_mem_streams = [SnapshotStream(node=stream.node, bytes_stream=list(stream.bytes_stream))]
        for node in nodes[1:]:
            stream = HostSnapshotManager.send(node.dt, graph, parent.dt)
            in_mem_streams.append(
                SnapshotStream(node=stream.node, bytes_stream=list(stream.bytes_stream)))
            parent = node

        # destroy the snapshots
        [HostSnapshotManager.destroy(node, graph) for node in nodes]
        query_nodes = HostSnapshotManager.query(dataset_id)[0].get_nodes()
        self.assertEqual(query_nodes, set())

        # rebuild from the streams using -F to overwrite existing dataset
        [HostSnapshotManager.recv(stream, graph, zfs_args=['-F']) for stream in in_mem_streams]
        query_nodes = HostSnapshotManager.query(dataset_id)[0].get_nodes()
        self.assertEqual(query_nodes, set(nodes))


if __name__ == '__main__':
    unittest.main(verbosity=2)
