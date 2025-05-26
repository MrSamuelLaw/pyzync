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
        date (Date): The date of the snapshot in YYYYMMDD or YYYY-MM-DD format
        zfs_prefix (PurePath): The ZFS filesystem path (e.g., "tank/data")
    """

    model_config = ConfigDict(frozen=True)

    date: Date
    zfs_prefix: PurePath

    def __eq__(self, other: Self):
        return self.zfs_handle == other.zfs_handle

    @field_validator("date", mode="before")
    @classmethod
    def convert_date(cls, value: str | Date) -> Date:
        if isinstance(value, str):
            return Date.fromisoformat(value)
        elif isinstance(value, Date):
            return value
        raise TypeError(f"Unable to coerce value = {value} to datetime.date")

    @field_validator("zfs_prefix", mode="before")
    @classmethod
    def format_zfs_prefix(cls, value: str | PurePath):
        value = str(value)
        if re.fullmatch(r"(\w)+(\w|\/)+", value):
            return PurePath(value)
        raise ValueError("root must not start with '/' or '.'")

    @computed_field()
    def zfs_handle(self) -> str:
        return f'{str(self.zfs_prefix)}@{self.date.strftime(r"%Y%m%d")}'


class SnapshotStream(BaseModel):
    """Represents a stream of snapshot data, either complete or incremental.

    A wrapper around snapshot data that includes metadata about the source snapshot
    and optionally an anchor snapshot for incremental streams.

    Attributes:
        ref (SnapshotRef): The snapshot being streamed
        iterable (Iterable[bytes]): The actual snapshot data stream
        anchor (Optional[SnapshotRef]): For incremental streams, the base snapshot
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    ref: SnapshotRef
    # the anchor snapshot when doing zfs send -i <anchor>..<snapshot> ...
    anchor: Optional[SnapshotRef] = Field(None)
    iterable: Iterable[bytes]

    @model_validator(mode="after")
    def validate_snapshot_refs(self):
        """Validates that the ref and anchor have the same zfs_root"""
        if (self.anchor is not None) and (self.anchor.zfs_prefix != self.ref.zfs_prefix):
            raise ValueError("ref and anchor must have matching zfs_root")
        return self

    @computed_field
    @property
    def filename(self) -> str:
        """Filename that the snapshot data should have"""
        if self.anchor is None:
            filename = f'{self.ref.date.strftime(r"%Y%m%d")}.zfs'
        else:
            filename = f'{self.anchor.date.strftime(r"%Y%m%d")}_{self.ref.date.strftime(r"%Y%m%d")}.zfs'
        return filename

    @computed_field
    @property
    def filepath(self) -> PurePath:
        """Filepath including the filename that the snapshot data should have
        relative to the backup directory root.
        """
        return self.ref.zfs_prefix.joinpath(self.filename)


class SnapshotStorageAdapter(ABC):
    """Abstract interface for storing and retrieving snapshot data.

    Defines the required operations for any storage backend that will be used
    to persist snapshot data. Implementations must handle both complete and
    incremental snapshots.
    """

    @abstractmethod
    def query(zfs_prefix: PurePath) -> list[PurePath]:
        """Query for stored snapshots matching the given ZFS prefix.

        Args:
            zfs_prefix: The ZFS filesystem path to query snapshots for

        Returns:
            List of paths to stored snapshot data files
        """
        pass

    @abstractmethod
    def destroy(filepaths: list[PurePath]) -> list[bool]:
        """Remove stored snapshot data files.

        Args:
            filepaths: List of paths to snapshot files to delete

        Returns:
            List of boolean success flags, one per input path
        """
        pass

    @abstractmethod
    def recv(stream: SnapshotStream) -> PurePath:
        """Store a snapshot data stream.

        Args:
            stream: The snapshot stream to store

        Returns:
            Path where the snapshot data was stored
        """
        pass

    @abstractmethod
    def send(filepaths: list[PurePath], blocksize: int = 4096) -> Iterable[bytes]:
        """Read stored snapshot data as a stream.

        Args:
            filepaths: Paths to snapshot files to read

        Returns:
            Iterator yielding the snapshot data in chunks
        """
        pass
