import shutil
import asyncio
import unittest
from os import environ
from pathlib import Path

from pyzync.backup import BackupJob
from pyzync.host import HostSnapshotManager
from pyzync.streaming import SnapshotStreamManager
from pyzync.interfaces import SnapshotGraph, Datetime
from pyzync.storage.interfaces import RemoteSnapshotManager
from pyzync.storage.adapters.file import LocalFileStorageAdapter
from pyzync.storage.adapters.dropbox import DropboxStorageAdapter
from pyzync.retention.policies.last_n_days import LastNSnapshotsPolicy


class TestLocalFileLoopCheck(unittest.IsolatedAsyncioTestCase):

    async def test_stream_using_pubsub(self):

        # get an instance of the adapter
        directory = Path(__file__).resolve().parent

        directory1 = directory.joinpath('test_loop_check_files/three')
        adapter = LocalFileStorageAdapter(directory=directory1, max_file_size=2**12)
        manager0 = RemoteSnapshotManager(adapter=adapter)
        directory0 = directory.joinpath('test_loop_check_files/four')
        adapter = LocalFileStorageAdapter(directory=directory0, max_file_size=2**12)
        manager1 = RemoteSnapshotManager(adapter=adapter)

        # clean up any lingering files
        dataset_id = 'tank0/foo'
        graphs = manager0.query(dataset_id)
        remote_graph0 = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [manager0.destroy(n, remote_graph0, force=True) for n in remote_graph0.get_nodes()]
        graphs = manager1.query(dataset_id)
        remote_graph1 = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [manager1.destroy(n, remote_graph1, force=True) for n in remote_graph1.get_nodes()]

        # query snapshots that are related to the file system
        graphs = HostSnapshotManager.query(dataset_id)
        host_graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [HostSnapshotManager.destroy(node, host_graph) for node in host_graph.get_nodes()]

        # # case 1 from the node_diagram
        datetimes = [
            '20250122T120000',
            '20250123T120000',
            '20250124T120000',
            '20250125T120000',
            '20250126T120000',
        ]
        # add a fake file for testing for each date
        nodes = []
        for dt in datetimes:
            path = Path(f'/tank0/foo/{dt}.txt')
            path.touch(exist_ok=True)
            nodes.append(HostSnapshotManager.create(dt, host_graph))
            path.unlink()

        # create the first complete stream
        chain = sorted(list(host_graph.get_nodes()), key=lambda node: node.dt)
        producers = [HostSnapshotManager.get_producer(chain[0].dt, host_graph)]

        # create the incremental streams
        producers.extend(
            [HostSnapshotManager.get_producer(n.dt, host_graph, p.dt) for n, p in zip(chain[1:], chain)])

        # send those streams to local storage
        for producer in producers:
            consumers = []
            for manager, graph in ((manager0, remote_graph0), (manager1, remote_graph1)):
                consumer = manager.get_consumer(producer.node, graph)
                consumers.append(consumer)
            stream_manager = SnapshotStreamManager(producer, consumers)
            await stream_manager.transmit()

        # verify the ref is queryable now that it exists
        graph = manager0.query(dataset_id)[0]
        self.assertEqual(graph.get_nodes(), remote_graph0.get_nodes())
        graph = manager1.query(dataset_id)[0]
        self.assertEqual(graph.get_nodes(), remote_graph1.get_nodes())

        for manager, graph in ((manager0, remote_graph0), (manager1, remote_graph1)):
            # destroy the snapshots from the host
            for node in chain:
                HostSnapshotManager.destroy(node, host_graph)

            # verify that the dataset is empty
            path = Path('/tank0/foo')
            self.assertFalse(any(path.iterdir()))

            # restore the files in order and check that the directory contains the file expected
            chain_ = remote_graph0.get_chains()[0]
            producers = [manager.get_producer(n, graph) for n in chain_]
            for producer, dt in zip(producers, datetimes):
                HostSnapshotManager.recv(producer, host_graph, zfs_args=['-F'])
                path = Path(f'/tank0/foo/{dt}.txt')
                self.assertTrue(path.exists())

            # verify that every snapshot was received
            path = Path(f'/tank0/foo/{datetimes[-1]}.txt')
            self.assertTrue(path.exists())
            path.unlink()

    async def test_can_use_backup_job(self):
        # define the backup config for each job
        directory = Path(__file__).resolve().parent
        directory = directory.joinpath('test_loop_check_files')

        # destroy the backups that exist in the directory already
        for f in directory.iterdir():
            if f.is_file():
                f.unlink()
            elif f.is_dir():
                shutil.rmtree(f)

        # setup a backup config
        config = {
            'tank0/bar':
                BackupJob(retention_policy=LastNSnapshotsPolicy(n_snapshots=5),
                          adapters=[LocalFileStorageAdapter(directory=directory)])
        }

        # destroy all nodes from host
        graphs = HostSnapshotManager.query('tank0/bar')
        host_graph = graphs[0] if graphs else SnapshotGraph(dataset_id='tank0/bar')

        [HostSnapshotManager.destroy(node, host_graph) for node in host_graph.get_nodes()]

        # perform a rotate and run each job
        datetimes = [
            '20250101T120000',
            '20250101T120001',
            '20250101T120002',
            '20250101T120003',
            '20250101T120004',
        ]
        datetimes = [Datetime(dt) for dt in datetimes]
        for dataset_id, job in config.items():
            for dt in datetimes:
                job.rotate(dataset_id, dt=dt)
            await job.sync(dataset_id, duplicate_policy='overwrite')

        # verify the expected files exist
        dataset_id = 'tank0/bar'
        # Check host snapshots
        host_graphs = HostSnapshotManager.query(dataset_id)
        self.assertTrue(host_graphs)
        host_graph = host_graphs[0]
        host_dts = sorted([n.dt for n in host_graph.get_nodes()])
        self.assertEqual(host_dts, datetimes)

        # Check backup snapshots in storage
        adapter = LocalFileStorageAdapter(directory=directory)
        manager = RemoteSnapshotManager(adapter=adapter)
        backup_graphs = manager.query(dataset_id)
        self.assertTrue(backup_graphs)
        backup_graph = backup_graphs[0]
        backup_dts = sorted([n.dt for n in backup_graph.get_nodes()])
        self.assertEqual(backup_dts, datetimes)


class TestDropboxLoopCheck(unittest.IsolatedAsyncioTestCase):

    async def test_stream_using_pubsub(self):
        """Test that ensures that data is able to be stored in chunks and restored
        using the pubsub method across moultiple managers.
        """

        # get an instance of the adapter
        adapter = DropboxStorageAdapter(directory='/pyzync/three',
                                        key=environ.get('DROPBOX_KEY'),
                                        secret=environ.get('DROPBOX_SECRET'),
                                        refresh_token=environ.get("DROPBOX_REFRESH_TOKEN"),
                                        access_token=environ.get("DROPBOX_TOKEN"),
                                        max_file_size=4E+6)
        manager0 = RemoteSnapshotManager(adapter=adapter)
        adapter = DropboxStorageAdapter(directory='/pyzync/four',
                                        key=environ.get('DROPBOX_KEY'),
                                        secret=environ.get('DROPBOX_SECRET'),
                                        refresh_token=environ.get("DROPBOX_REFRESH_TOKEN"),
                                        access_token=environ.get("DROPBOX_TOKEN"),
                                        max_file_size=4E+6)
        manager1 = RemoteSnapshotManager(adapter=adapter)

        # clean up any lingering files
        dataset_id = 'tank0/foo'
        graphs = manager0.query(dataset_id)
        remote_graph0 = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [manager0.destroy(n, remote_graph0, force=True) for n in remote_graph0.get_nodes()]
        graphs = manager1.query(dataset_id)
        remote_graph1 = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [manager1.destroy(n, remote_graph1, force=True) for n in remote_graph1.get_nodes()]

        # query snapshots that are related to the file system
        graphs = HostSnapshotManager.query(dataset_id)
        host_graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [HostSnapshotManager.destroy(node, host_graph) for node in host_graph.get_nodes()]

        # # case 1 from the node_diagram
        datetimes = [
            '20250122T120000',
            '20250123T120000',
            '20250124T120000',
            '20250125T120000',
            '20250126T120000',
        ]
        # add a fake file for testing for each date
        nodes = []
        for dt in datetimes:
            path = Path(f'/tank0/foo/{dt}.txt')
            path.touch(exist_ok=True)
            nodes.append(HostSnapshotManager.create(dt, host_graph))
            path.unlink()

        # create the first complete stream
        chain = sorted(list(host_graph.get_nodes()), key=lambda node: node.dt)
        producers = [HostSnapshotManager.get_producer(chain[0].dt, host_graph)]

        # create the incremental streams
        producers.extend(
            [HostSnapshotManager.get_producer(n.dt, host_graph, p.dt) for n, p in zip(chain[1:], chain)])

        # send those streams to local storage
        stream_managers = []
        for producer in producers:
            consumers = []
            for manager, graph in ((manager0, remote_graph0), (manager1, remote_graph1)):
                consumer = manager.get_consumer(producer.node, graph)
                consumers.append(consumer)
            stream_manager = SnapshotStreamManager(producer, consumers)
            stream_managers.append(stream_manager)
        await asyncio.gather(*[sm.transmit() for sm in stream_managers])

        # verify the ref is queryable now that it exists
        graph = manager0.query(dataset_id)[0]
        self.assertEqual(graph.get_nodes(), remote_graph0.get_nodes())
        graph = manager1.query(dataset_id)[0]
        self.assertEqual(graph.get_nodes(), remote_graph1.get_nodes())

        for manager, graph in ((manager0, remote_graph0), (manager1, remote_graph1)):
            # destroy the snapshots from the host
            for node in chain:
                HostSnapshotManager.destroy(node, host_graph)

            # verify that the dataset is empty
            path = Path('/tank0/foo')
            self.assertFalse(any(path.iterdir()))

            # restore the files in order and check that the directory contains the file expected
            chain_ = remote_graph0.get_chains()[0]
            producers = [manager.get_producer(n, graph) for n in chain_]
            for producer, dt in zip(producers, datetimes):
                HostSnapshotManager.recv(producer, host_graph, zfs_args=['-F'])
                path = Path(f'/tank0/foo/{dt}.txt')
                self.assertTrue(path.exists())

            # verify that every snapshot was received
            path = Path(f'/tank0/foo/{datetimes[-1]}.txt')
            self.assertTrue(path.exists())
            path.unlink()


if __name__ == '__main__':
    unittest.main(verbosity=2)
