"""
Policies for snapshot retention.
"""
from abc import ABC, abstractmethod
from pathlib import PurePath
from datetime import date as Date

from pydantic import BaseModel, Field, ConfigDict

from pyzync.managers import HostSnapshotManager


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
        # validate the inputs
        if self.n_days < 1:
            raise ValueError(f'Ndays must be greater than one, recieved value {self.n_days}')

        # create todays snapshot if it doesn't exist
        refs = host.query(zfs_dataset_path)
        if not any((r for r in refs if r.date == date)):
            refs.append(host.create(zfs_dataset_path, date))

        # destroy ones older than n days
        [host.destroy(ref) for ref in refs if (date - ref.date).days > self.n_days]
