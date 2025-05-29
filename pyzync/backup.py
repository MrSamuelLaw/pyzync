"""This module performs backup related tasks, like rotating the host snapshots and syncing
them with remotes using the sync method
"""

from pathlib import PurePath
from datetime import date as Date

from pydantic import BaseModel, ConfigDict, field_validator

from pyzync.interfaces import SnapshotRef, SnapshotStorageAdapter
from pyzync.managers import HostSnapshotManager, FileSnapshotManager
from pyzync.retention_polcies import RetentionPolicy


class BackupConfig(BaseModel):

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    zfs_dataset_path: PurePath
    retention_policy: RetentionPolicy
    storage_adapters: list[SnapshotStorageAdapter]

    @field_validator('zfs_dataset_path')
    @classmethod
    def format_zfs_dataset_path(cls, zfs_dataset_path: PurePath):
        return SnapshotRef.format_zfs_dataset_path(zfs_dataset_path)

    def rotate(self, host: HostSnapshotManager = HostSnapshotManager, date=Date.today()):
        self.retention_policy.rotate(self.zfs_dataset_path, date=date, host=host)

    def sync(self, host: HostSnapshotManager = HostSnapshotManager):

        for adapter in self.storage_adapters:
            manager = FileSnapshotManager(adapter=adapter)

            # get the refs from the host
            host_refs = host.query(self.zfs_dataset_path)
            remote_refs = manager.query(self.zfs_dataset_path)

            # move the oldest complete snapshot forward if it doesn't already exist
            oldest_host_ref = min(host_refs, key=lambda ref: ref.date, default=None)
            # only proceed if host snapshots exist
            if oldest_host_ref is not None:
                oldest_remote_ref = min(remote_refs, key=lambda r: r.date, default=None)
                if (oldest_remote_ref is None) or (oldest_host_ref.date != oldest_remote_ref.date):
                    manager.recv(host.send(oldest_host_ref))
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
                        manager.recv(host.send(ref, base))
                        for ref, base in zip(new_host_refs[1:], new_host_refs)
                    ]

                # prune the remote
                manager.prune(self.zfs_dataset_path)


def backup(backup_configs: list[BackupConfig]):
    if len({bc.zfs_dataset_path for bc in backup_configs}) != len(backup_configs):
        raise ValueError('Each BackupConfig.zfs_dataset_path must be unique.')
    for bc in backup_configs:
        bc.rotate()
        bc.sync()
