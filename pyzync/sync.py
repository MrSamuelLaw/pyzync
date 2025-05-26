"""This module syncs the current snapshot with remotes
"""

from pathlib import Path

from pyzync.managers import HostSnapshotManager as host
from pyzync.managers import FileSnapshotManager
from pyzync.storage_adapters import Adapter


def sync(remotes: list[dict]):
    for remote in remotes:
        # get the correct adapter and build the manager
        adapter = Adapter[remote['type']](**remote['config'])
        manager = FileSnapshotManager(adapter)

        # for each dataset to sync, perform the sync
        datasets = remote['zfs_dataset_paths']
        for dataset in datasets:
            host_refs = host.query(datasets)
            remote_refs = manager.query(datasets)

            # send the new ones
            newest_remote_ref = max(remote_refs, key=lambda r: r.date)
            new_host_refs = [r for r in host_refs if r.date > newest_remote_ref]
            streams = [host.send(ref)[0] for ref in new_host_refs]
            [manager.recv(stream) for stream in streams]

            # roll the base snapshot forward
            oldest_host_ref =  min(host_refs, key=lambda ref: ref.date)
            oldest_remote_ref = min(remote_refs, key=lambda r: r.date)
            if oldest_remote_ref.date == oldest_host_ref.date:
                pass
            elif oldest_remote_ref.date > oldest_host_ref.date:
                old_host_refs = 

            # delete the old snapshots

            # prune the snapshots on the remote side
