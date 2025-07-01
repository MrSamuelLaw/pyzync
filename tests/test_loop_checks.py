import shutil
import unittest
from os import environ
from pathlib import Path

from pyzync.host import HostSnapshotManager
from pyzync.retention_policies import LastNSnapshotsPolicy
from pyzync.interfaces import SnapshotGraph, Datetime
from pyzync.storage_adapters import RemoteSnapshotManager, LocalFileStorageAdapter, DropboxStorageAdapter
from pyzync.backup import BackupJob


class TestLocalFileLoopCheck(unittest.TestCase):

    def test_can_backup_and_restore_using_incremental_backups(self):
        """Test that ensures that data is able to be sent to a local file
        and used to recover lost data.
        """

        # get an instance of the adapter
        directory = Path(__file__).resolve().parent
        directory = directory.joinpath('test_loop_check_files')
        adapter = LocalFileStorageAdapter(directory=directory)
        manager = RemoteSnapshotManager(adapter=adapter)

        # clean up any lingering files
        dataset_id = 'tank0/foo'
        graphs = manager.query(dataset_id)
        remote_graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [manager.destroy(n, remote_graph, force=True) for n in remote_graph.get_nodes()]

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
        streams = [HostSnapshotManager.send(chain[0].dt, host_graph)]

        # create the incremental streams
        streams.extend(
            [HostSnapshotManager.send(n.dt, host_graph, p.dt) for n, p in zip(chain[1:], chain)])

        # send those streams to local storage
        [manager.recv(stream, remote_graph) for stream in streams]

        # verify the ref is queryable now that it exists
        graph = manager.query(dataset_id)[0]
        self.assertEqual(graph.get_nodes(), remote_graph.get_nodes())

        # destroy the snapshots from the host
        [HostSnapshotManager.destroy(node, host_graph) for node in chain]

        # verify that the dataset is empty
        path = Path('/tank0/foo')
        self.assertFalse(any(path.iterdir()))

        # restore the files in order and check that the directory contains the file expected
        chain = remote_graph.get_chains()[0]
        streams = [manager.send(n, remote_graph) for n in chain]
        for stream, dt in zip(streams, datetimes):
            HostSnapshotManager.recv(stream, host_graph, zfs_args=['-F'])
            path = Path(f'/tank0/foo/{dt}.txt')
            self.assertTrue(path.exists())

        # verify that every snapshot was received
        path = Path(f'/tank0/foo/{datetimes[-1]}.txt')
        self.assertTrue(path.exists())
        path.unlink()

    def test_can_use_backup_job(self):
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
                job.sync(dataset_id)

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


class TestDropboxLoopCheck(unittest.TestCase):

    def test_can_backup_and_restore_using_incremental_backups(self):
        """Test that ensures that data is able to be sent to a local file
        and used to recover lost data.
        """
        dataset_id = 'tank0/foo'
        adapter = DropboxStorageAdapter(directory=environ["DROPBOX_DIR"],
                                        access_token=environ["DROPBOX_TOKEN"])
        manager = RemoteSnapshotManager(adapter=adapter)

        # clean up any lingering files
        graphs = manager.query(dataset_id)

        remote_graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        [manager.destroy(n, remote_graph, force=True) for n in remote_graph.get_nodes()]

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
        streams = [HostSnapshotManager.send(chain[0].dt, host_graph)]

        # create the incremental streams
        streams.extend(
            [HostSnapshotManager.send(n.dt, host_graph, p.dt) for n, p in zip(chain[1:], chain)])

        # send those streams to local storage
        [manager.recv(stream, remote_graph) for stream in streams]

        # verify the ref is queryable now that it exists
        graph = manager.query(dataset_id)[0]
        self.assertEqual(graph.get_nodes(), remote_graph.get_nodes())

        # destroy the snapshots from the host
        [HostSnapshotManager.destroy(node, host_graph) for node in chain]

        # verify that the dataset is empty
        path = Path('/tank0/foo')
        self.assertFalse(any(path.iterdir()))

        # restore the files in order and check that the directory contains the file expected
        chain = remote_graph.get_chains()[0]
        streams = [manager.send(n, remote_graph) for n in chain]
        for stream, dt in zip(streams, datetimes):
            HostSnapshotManager.recv(stream, host_graph, zfs_args=['-F'])
            path = Path(f'/tank0/foo/{dt}.txt')
            self.assertTrue(path.exists())

        # verify that every snapshot was received
        path = Path(f'/tank0/foo/{datetimes[-1]}.txt')
        self.assertTrue(path.exists())
        path.unlink()

    # def test_can_use_backup_job(self):
    #     # define the backup config for each job
    #     directory = Path(__file__).resolve().parent
    #     directory = directory.joinpath('test_loop_check_files')

    #     # destroy the backups that exist in the directory already
    #     for f in directory.iterdir():
    #         if f.is_file():
    #             f.unlink()
    #         elif f.is_dir():
    #             shutil.rmtree(f)

    #     # setup a backup config
    #     config = {
    #         'tank0/bar':
    #             BackupJob(retention_policy=LastNSnapshotsPolicy(n_snapshots=5),
    #                       adapters=[LocalFileStorageAdapter(directory=directory)])
    #     }

    #     # destroy all nodes from host
    #     graphs = HostSnapshotManager.query('tank0/bar')
    #     host_graph = graphs[0] if graphs else SnapshotGraph(dataset_id='tank0/bar')
    #     [HostSnapshotManager.destroy(node, host_graph) for node in host_graph.get_nodes()]

    #     # perform a rotate and run each job
    #     datetimes = [
    #         '20250101T120000',
    #         '20250101T120001',
    #         '20250101T120002',
    #         '20250101T120003',
    #         '20250101T120004',
    #     ]
    #     datetimes = [Datetime(dt) for dt in datetimes]
    #     for dataset_id, job in config.items():
    #         for dt in datetimes:
    #             job.rotate(dataset_id, dt=dt)
    #             job.sync(dataset_id)

    #     # verify the expected files exist
    #     dataset_id = 'tank0/bar'
    #     # Check host snapshots
    #     host_graphs = HostSnapshotManager.query(dataset_id)
    #     self.assertTrue(host_graphs)
    #     host_graph = host_graphs[0]
    #     host_dts = sorted([n.dt for n in host_graph.get_nodes()])
    #     self.assertEqual(host_dts, datetimes)

    # # Check backup snapshots in storage
    # adapter = LocalFileStorageAdapter(directory=directory)
    # manager = RemoteSnapshotManager(adapter=adapter)
    # backup_graphs = manager.query(dataset_id)
    # self.assertTrue(backup_graphs)
    # backup_graph = backup_graphs[0]
    # backup_dts = sorted([n.dt for n in backup_graph.get_nodes()])
    # self.assertEqual(backup_dts, datetimes)
