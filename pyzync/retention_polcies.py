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


class RetentionPolicy(ABC):

    @abstractmethod
    def rotate(self,
               zfs_dataset_path: PurePath,
               host: HostSnapshotManager = HostSnapshotManager,
               date: Date = Date.today()):
        pass


class RollingNDaysPolicy(RetentionPolicy, BaseModel):

    model_config = ConfigDict(frozen=True)

    n_days: int = Field(gt=1, le=30)

    def rotate(self,
               zfs_dataset_path: PurePath,
               host: HostSnapshotManager = HostSnapshotManager,
               date=Date.today()):
        logger.info(f"Applying RollingNDaysPolicy for {zfs_dataset_path} on {date}")

        # validate the inputs
        if self.n_days < 1:
            msg = f'Ndays must be greater than one, recieved value {self.n_days}'
            logger.error(msg)
            raise ValueError(msg)

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
