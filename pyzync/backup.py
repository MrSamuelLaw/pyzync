"""This module performs backup related tasks, like rotating the host snapshots and syncing
them with remotes using the sync method
"""

import logging
from pathlib import PurePath
from datetime import datetime as Datetime

from pydantic import BaseModel, ConfigDict, field_validator

from pyzync.interfaces import SnapshotRef, SnapshotStorageAdapter, DuplicateDetectedPolicy
from pyzync.managers import HostSnapshotManager, FileSnapshotManager
from pyzync.retention_policies import RetentionPolicy

logger = logging.getLogger(__name__)


class BackupConfig(BaseModel):
    """
    Represents a backup configuration for a single zfs dataset on the host os.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    zfs_dataset_path: PurePath
    retention_policy: RetentionPolicy
    adapters: list[SnapshotStorageAdapter]

    @field_validator('zfs_dataset_path')
    @classmethod
    def format_zfs_dataset_path(cls, zfs_dataset_path: PurePath):
        return SnapshotRef.format_zfs_dataset_path(zfs_dataset_path)

    def rotate(self,
               host: HostSnapshotManager = HostSnapshotManager,
               datetime: Datetime = Datetime.now(),
               dryrun: bool = False):
        """
        Rotates the snapshots on the host. If dryrun is true, it will only simulate the rotation,
        it won't actually rotate the snapshots.
        """
        # create a new snapshot and delete the old ones using the retention policy
        logger.info(f"Rotating snapshots for dataset {self.zfs_dataset_path}")
        try:
            refs = host.query(zfs_dataset_path=self.zfs_dataset_path)
            if not dryrun:
                new_ref = host.create(zfs_dataset_path=self.zfs_dataset_path, datetime=datetime)
            else:
                new_ref = SnapshotRef(datetime=datetime, zfs_dataset_path=self.zfs_dataset_path)
            refs.append(new_ref)
            keep, destroy = self.retention_policy.split(refs)
            if not dryrun:
                [host.destroy(d) for d in destroy]
            return (keep, destroy)
        except Exception as e:
            logger.exception(f"Exception during rotation for {self.zfs_dataset_path}")
            raise

    def sync(self,
             host: HostSnapshotManager = HostSnapshotManager,
             force: bool = False,
             dryrun: bool = False,
             on_duplicate_detected: DuplicateDetectedPolicy = 'error'):

        for adapter in self.adapters:
            host_refs = host.query(self.zfs_dataset_path)
            host_refs = sorted(host_refs, key=lambda ref: ref.datetime)
            manager = FileSnapshotManager(adapter=adapter)
            remote_refs = manager.query(self.zfs_dataset_path)

            # compute which remotes to destroy
            oldest_host_ref = min(host_refs, key=lambda ref: ref.datetime, default=None)
            destroy = [ref for ref in remote_refs if ref.datetime < oldest_host_ref.datetime]

            # figure out how to send the refs, i.e. incremental or complete
            streams = host.send(oldest_host_ref)
            streams.extend([
                host.send(ref, base)[0]
                for ref, base in zip(host_refs[1:], host_refs)
                if ref not in remote_refs
            ])

            for s in streams:
                logger.info(f"Sending stream {s} to remote {adapter}")
                manager.recv([s], on_duplicate_detected=on_duplicate_detected, dryrun=dryrun)

            for d in destroy:
                logger.info(f"Deleting old remote refs: {d}")
                manager.destroy(d, force=force, dryrun=dryrun)

            manager.prune(zfs_dataset_path=self.zfs_dataset_path)


class BackupJob(BaseModel):
    """
    Represents a backup job that can be scheduled to run using cron or similar programs.
    """

    backup_configs: list[BackupConfig]

    @field_validator('backup_configs')
    @classmethod
    def validate_backup_configs(cls, value):
        if len({bc.zfs_dataset_path for bc in value}) != len(value):
            msg = 'Each BackupConfig.zfs_dataset_path must be unique.'
            logger.error(msg)
            raise ValueError(msg)
        return value

    def backup(self,
               force: bool = False,
               dryrun: bool = False,
               on_duplicate_detected: DuplicateDetectedPolicy = 'error'):
        logger.info("Starting backup process")
        for bc in self.backup_configs:
            try:
                bc.rotate(dryrun=dryrun)
                bc.sync(force=force, dryrun=dryrun, on_duplicate_detected=on_duplicate_detected)
            except Exception as e:
                logger.exception(f"Exception during backup for {bc.zfs_dataset_path}")
