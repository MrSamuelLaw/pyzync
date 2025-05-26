import unittest
from datetime import date as Date
from pathlib import Path, PurePath

from pyzync.managers import HostSnapshotManager, FileSnapshotManager
from pyzync.storage_adapters import LocalFileSnapshotDataAdapter

# class TestLocalFileLoopCheck(unittest.TestCase):

#     def test_can_use_local_file_snapshot_data_adapter(self):
#         """Test that ensures that data is able to be sent to a local file
#         and used to recover lost data.
#         """

#         # get an instance of the adapter
#         directory = Path(__file__).resolve().parent
#         directory = directory.joinpath('local_storage_adapter_files')
#         adapter = LocalFileSnapshotDataAdapter(directory)
#         file_manager = FileSnapshotManager(adapter)

#         # clean up any lingering files
#         refs = FileSnapshotManager.query(adapter)
#         refs = sorted(refs, key=lambda r: r.date, reverse=True)
#         [FileSnapshotManager.destroy(adapter, r) for r in refs]

#         # query snapshots that are related to the file system
#         zfs_root = PurePath("tank0/foo")
#         refs = HostSnapshotManager.query(zfs_root)
#         [HostSnapshotManager.destroy(ref) for ref in refs]

#         # case 1 from the node_diagram
#         dates = [
#             '20250122',
#             '20250123',
#             '20250124',
#             '20250125',
#             '20250126',
#         ]
#         refs = [HostSnapshotManager.create(zfs_root, d) for d in dates]

#         # create the first complete stream
#         streams = [HostSnapshotManager.send(refs[0])]

#         # create the incremental streams
#         streams.extend([HostSnapshotManager.send(ref, anchor) for anchor, ref in zip(refs, refs[1:])])

#         # send those streams to local storage
#         filepaths = [FileSnapshotManager.recv(adapter, stream) for stream in streams]

#         # verify the ref is queryable now that it exists
#         file_refs = FileSnapshotManager.query(adapter)
#         self.assertEqual(set(refs), set(file_refs))

#         # destroy the snapshot from the host
#         [HostSnapshotManager.destroy(ref) for ref in refs]

#         # restore them from the files we saved
#         streams = [FileSnapshotManager.send(adapter, ref) for ref in refs]
#         [HostSnapshotManager.recv(stream) for stream in streams]

#         # # requery to verify that its gone
#         # host_refs = HostSnapshotManager.query()
#         # self.assertNotIn(ref, host_refs)

#         # # destroy from the local file system
#         # FileSnapshotManager.destroy(adapter, ref)

#         # # requery to verify that its gone
#         # file_refs = FileSnapshotManager.query(adapter)
#         # self.assertNotIn(ref, file_refs)
