"""This module performs backup related tasks, like rotating the host snapshots and syncing
them with remotes using the sync method
"""

import logging
from pathlib import PurePath
from datetime import date as Date

from pydantic import BaseModel, ConfigDict, field_validator

from pyzync.interfaces import SnapshotRef, SnapshotStorageAdapter
from pyzync.managers import HostSnapshotManager, FileSnapshotManager
from pyzync.retention_policies import RetentionPolicy

logger = logging.getLogger(__name__)


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
        logger.info(f"Rotating snapshots for dataset {self.zfs_dataset_path} on {date}")
        try:
            self.retention_policy.rotate(self.zfs_dataset_path, date=date, host=host)
        except Exception as e:
            logger.exception(f"Exception during rotation for {self.zfs_dataset_path}")
            raise

    def sync(self, host: HostSnapshotManager = HostSnapshotManager):
        for adapter in self.storage_adapters:
            manager = FileSnapshotManager(adapter=adapter)
            logger.info(f"Syncing dataset {self.zfs_dataset_path} with remote {adapter}")
            try:
                # get the refs from the host
                host_refs = host.query(self.zfs_dataset_path)
                remote_refs = manager.query(self.zfs_dataset_path)

                # move the oldest complete snapshot forward if it doesn't already exist
                oldest_host_ref = min(host_refs, key=lambda ref: ref.date, default=None)
                # only proceed if host snapshots exist
                if oldest_host_ref is not None:
                    oldest_remote_ref = min(remote_refs, key=lambda r: r.date, default=None)
                    if (oldest_remote_ref is None) or (oldest_host_ref.date != oldest_remote_ref.date):
                        logger.info(f"Sending oldest complete host snapshot {oldest_host_ref} to remote")
                        manager.recv(host.send(oldest_host_ref))
                        if oldest_remote_ref is None:
                            remote_refs = [oldest_host_ref]
                            oldest_remote_ref = remote_refs[0]

                # delete everything older than the oldest ref
                old_remote_refs = [ref for ref in remote_refs if ref.date < oldest_host_ref.date]
                if old_remote_refs:
                    logger.info(f"Deleting old remote refs: {old_remote_refs}")
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
                logger.info(f"Pruning remote for dataset {self.zfs_dataset_path}")
                manager.prune(self.zfs_dataset_path)
            except Exception as e:
                logger.exception(
                    f"Exception during sync for {self.zfs_dataset_path} with adapter {adapter}")


def backup(backup_configs: list[BackupConfig]):
    logger.info("Starting backup process")
    if len({bc.zfs_dataset_path for bc in backup_configs}) != len(backup_configs):
        msg = 'Each BackupConfig.zfs_dataset_path must be unique.'
        logger.error(msg)
        raise ValueError(msg)
    for bc in backup_configs:
        try:
            bc.rotate()
            bc.sync()
        except Exception as e:
            logger.exception(f"Exception during backup for {bc.zfs_dataset_path}")
