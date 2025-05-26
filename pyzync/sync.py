"""This module syncs the current snapshot with remotes
"""

from pathlib import PurePath, Path

from pyzync.managers import HostSnapshotManager as host
from pyzync.managers import FileSnapshotManager
from pyzync.storage_adapters import Adapter


def sync(remotes: list[dict]):
    for remote in remotes:
        # get the correct adapter and build the manager
        adapter = Adapter[remote['type']].value(**remote['config'])
        manager = FileSnapshotManager(adapter)

        # for each dataset to sync, perform the sync
        datasets = remote['zfs_dataset_paths']
        for dataset in datasets:
            dataset = PurePath(dataset)
            host_refs = host.query(dataset)
            remote_refs = manager.query(dataset)

            # move the oldest complete snapshot forward if it doesn't already exist
            oldest_host_ref = min(host_refs, key=lambda ref: ref.date, default=None)
            if oldest_host_ref is None:
                break  # do nothing if no host snapshots exist
            oldest_remote_ref = min(remote_refs, key=lambda r: r.date, default=None)
            if (oldest_remote_ref is None) or (oldest_host_ref.date != oldest_remote_ref.date):
                manager.recv(host.send(oldest_host_ref)[0])
                if oldest_remote_ref is None:
                    remote_refs = [oldest_host_ref]
                    oldest_remote_ref = remote_refs[0]

            # delete everything older than the oldest ref
            old_remote_refs = [ref for ref in remote_refs if ref.date < oldest_host_ref.date]
            [manager.destroy(ref) for ref in old_remote_refs]

            # send all new incremental refs to the remote
            newest_remote_ref = max(remote_refs, key=lambda r: r.date)
            new_host_refs = [ref for ref in host_refs if ref.date > newest_remote_ref.date]
            if new_host_refs:
                new_host_refs.append(newest_remote_ref)
                new_host_refs = sorted(new_host_refs, key=lambda ref: ref.date)
                [
                    manager.recv(host.send(ref, base)[0])
                    for ref, base in zip(new_host_refs[1:], new_host_refs)
                ]

            # prune the remote
            manager.prune(dataset)
