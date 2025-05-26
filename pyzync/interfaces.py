"""Core interfaces and data models for managing ZFS snapshots.

This module defines the fundamental interfaces and data structures used throughout
the pyzync package for representing and managing ZFS snapshots and their data streams.
"""

import re
from abc import ABC, abstractmethod
from pathlib import PurePath
from datetime import date as Date
from typing import Self, Iterable, Optional

from pydantic import (
    BaseModel,
    Field,
    model_validator,
    field_validator,
    computed_field,
    ConfigDict,
)


class SnapshotRef(BaseModel):
    """Reference to a ZFS snapshot with date and filesystem path information.

    Attributes:
        date (Date): The date of the snapshot in YYYY-MM-DD format.
        zfs_dataset_path (PurePath): The ZFS filesystem path (e.g., "tank/data").
    """

    model_config = ConfigDict(frozen=True)

    date: Date
    zfs_dataset_path: PurePath

    def __eq__(self, other: Self):
        return self.zfs_snapshot_id == other.zfs_snapshot_id

    @field_validator("date", mode="before")
    @classmethod
    def convert_date(cls, value: str | Date) -> Date:
        if isinstance(value, str):
            return Date.fromisoformat(value)
        elif isinstance(value, Date):
            return value
        raise TypeError(f"Unable to coerce value = {value} to datetime.date")

    @field_validator("zfs_dataset_path", mode="before")
    @classmethod
    def format_zfs_dataset_path(cls, value: str | PurePath):
        value = str(value)
        if re.fullmatch(r"(\w)+(\w|\/)+", value):
            return PurePath(value)
        raise ValueError("root must not start with '/' or '.'")

    @computed_field()
    def zfs_snapshot_id(self) -> str:
        """Returns the ZFS snapshot identifier in the format <dataset>@<YYYYMMDD>."""
        return f'{str(self.zfs_dataset_path)}@{self.date.strftime(r"%Y%m%d")}'


class SnapshotStream(BaseModel):
    """Represents a stream of snapshot data, either complete or incremental.

    Wraps snapshot data and metadata about the source snapshot and, for incrementals,
    the base snapshot.

    Attributes:
        ref (SnapshotRef): The snapshot being streamed.
        snapshot_stream (Iterable[bytes]): The actual snapshot data stream.
        base (Optional[SnapshotRef]): For incremental streams, the base snapshot.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    ref: SnapshotRef
    base: Optional[SnapshotRef] = Field(None)
    snapshot_stream: Iterable[bytes]

    @model_validator(mode="after")
    def validate_snapshot_refs(self):
        """Validates that the ref and base have the same zfs_root."""
        if (self.base is not None) and (self.base.zfs_dataset_path != self.ref.zfs_dataset_path):
            raise ValueError("ref and base must have matching zfs_root")
        return self

    @computed_field
    @property
    def filename(self) -> str:
        """Returns the filename for the snapshot data, based on the snapshot date(s)."""
        if self.base is None:
            filename = f'{self.ref.date.strftime(r"%Y%m%d")}.zfs'
        else:
            filename = f'{self.base.date.strftime(r"%Y%m%d")}_{self.ref.date.strftime(r"%Y%m%d")}.zfs'
        return filename

    @computed_field
    @property
    def filepath(self) -> PurePath:
        """Returns the relative filepath (including filename) for the snapshot data."""
        return self.ref.zfs_dataset_path.joinpath(self.filename)


class SnapshotStorageAdapter(ABC):
    """Abstract interface for storing and retrieving snapshot data.

    Implementations must handle both complete and incremental snapshots.
    """

    @abstractmethod
    def query(zfs_dataset_path: PurePath) -> list[PurePath]:
        """Query for stored snapshots matching the given ZFS prefix.

        Args:
            zfs_dataset_path: The ZFS filesystem path to query snapshots for.

        Returns:
            List of paths to stored snapshot data files.
        """
        pass

    @abstractmethod
    def destroy(filepaths: list[PurePath]) -> list[bool]:
        """Remove stored snapshot data files.

        Args:
            filepaths: List of paths to snapshot files to delete.

        Returns:
            List of boolean success flags, one per input path.
        """
        pass

    @abstractmethod
    def recv(stream: SnapshotStream) -> PurePath:
        """Store a snapshot data stream.

        Args:
            stream: The snapshot stream to store.

        Returns:
            Path where the snapshot data was stored.
        """
        pass

    @abstractmethod
    def send(filepath: PurePath, blocksize: int = 4096) -> Iterable[bytes]:
        """Read stored snapshot data as a stream.

        Args:
            filepath: Path to snapshot file to read.
            blocksize: Size of chunks to read.

        Returns:
            Iterator[bytes], yielding the data for a single snapshot in chunks.
        """
        pass
