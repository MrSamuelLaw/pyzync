"""
Policies for snapshot retention.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import PurePath
from datetime import date as Date

from pydantic import BaseModel, Field, ConfigDict

from pyzync.managers import HostSnapshotManager

logger = logging.getLogger(__name__)


class RetentionPolicy(ABC, BaseModel):

    model_config = ConfigDict(frozen=True)

    @abstractmethod
    def rotate(self,
               zfs_dataset_path: PurePath,
               host: HostSnapshotManager = HostSnapshotManager,
               date: Date = Date.today()):
        pass


class LastNSnapshotsPolicy(RetentionPolicy):

    n_snapshots: int = Field(gt=0, le=30)

    def rotate(self, zfs_dataset_path, host=HostSnapshotManager, date=Date.today()):
        logger.info(f"Applying LastNSnapshotsPolicy for {zfs_dataset_path} on {date}")

        # create a new snapshot
        refs = host.query(zfs_dataset_path)
        logger.info(f"Creating snapshot for {zfs_dataset_path} with date {date}")
        refs.append(host.create(zfs_dataset_path, date))

        # sort by date and destroy the oldest ones
        refs = sorted(refs, key=lambda ref: ref.date, reverse=True)
        old_refs = [ref for i, ref in enumerate(refs) if i > self.n_snapshots]
        for ref in old_refs:
            logger.info(f"Destroying old snapshot: {ref}")
            host.destroy(ref)


class LastNDaysPolicy(RetentionPolicy):

    n_days: int = Field(gt=1, le=30)

    def rotate(self,
               zfs_dataset_path: PurePath,
               host: HostSnapshotManager = HostSnapshotManager,
               date=Date.today()):
        logger.info(f"Applying LastNDaysPolicy for {zfs_dataset_path} on {date}")

        # create todays snapshot if it doesn't exist
        refs = host.query(zfs_dataset_path)
        if not any((r for r in refs if r.date == date)):
            logger.info(f"Creating snapshot for {zfs_dataset_path} with date {date}")
            refs.append(host.create(zfs_dataset_path, date))

        # destroy ones older than n days
        old_refs = [ref for ref in refs if (date - ref.date).days > self.n_days]
        for ref in old_refs:
            logger.info(f"Destroying old snapshot: {ref}")
            host.destroy(ref)
