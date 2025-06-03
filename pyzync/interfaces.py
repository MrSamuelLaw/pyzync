"""Core interfaces and data models for managing ZFS snapshots.

This module defines the fundamental interfaces and data structures used throughout
the pyzync package for representing and managing ZFS snapshots and their data streams.
"""

import re
from abc import ABC, abstractmethod
from pathlib import PurePath
from datetime import datetime as Datetime
from typing import Self, Iterable, Optional, Literal

from pydantic import (
    BaseModel,
    Field,
    model_validator,
    field_validator,
    computed_field,
    ConfigDict,
)

DATETIME_FORMAT = r"%Y%m%dT%H%M%S"
DATETIME_REGEX = r'\d{8}T\d{6}'
DATETIME_STR_LENGTH = len('yyyymmddThhmmss')

DuplicateDetectedPolicy = Literal['error', 'ignore', 'overwrite']


class SnapshotRef(BaseModel):
    """Reference to a ZFS snapshot with datetime and filesystem path information.

    Attributes:
        datetime (Datetime): The date/time of the snapshot
        zfs_dataset_path (PurePath): The ZFS filesystem path (e.g., "tank/data").
    """

    model_config = ConfigDict(frozen=True)

    datetime: Datetime
    zfs_dataset_path: PurePath

    def __eq__(self, other: Self):
        return self.zfs_snapshot_id == other.zfs_snapshot_id
    
    def __str__(self):
        return str(self.zfs_snapshot_id)

    @field_validator("datetime", mode="before")
    @classmethod
    def convert_date(cls, value: str | Datetime) -> Datetime:
        if isinstance(value, str):
            return Datetime.fromisoformat(value)
        elif isinstance(value, Datetime):
            return value
        raise TypeError(f"Unable to coerce value = {value} to datetime.datetime")

    @field_validator("zfs_dataset_path", mode="before")
    @classmethod
    def format_zfs_dataset_path(cls, value: str | PurePath):
        value = str(value)
        if re.fullmatch(r"(\w)+(\w|\/)+", value):
            return PurePath(value)
        raise ValueError("root must not start with '/' or '.'")

    @computed_field()
    def zfs_snapshot_id(self) -> str:
        """Returns the ZFS snapshot identifier in the format <dataset>@<YYYYMMDDTHHmmss>."""
        return f'{str(self.zfs_dataset_path)}@{self.datetime.strftime(DATETIME_FORMAT)}'


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
    
    def __str__(self):
        return str(self.filepath)

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
            filename = f'{self.ref.datetime.strftime(DATETIME_FORMAT)}.zfs'
        else:
            filename = f'{self.base.datetime.strftime(DATETIME_FORMAT)}_{self.ref.datetime.strftime(DATETIME_FORMAT)}.zfs'
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
            dryrun: If true, do not actually delete the file.

        Returns:
            List of boolean success flags, one per input path.
        """
        pass

    @abstractmethod
    def recv(stream: SnapshotStream) -> PurePath:
        """Store a snapshot data stream.

        Args:
            stream: The snapshot stream to store.
            dryrun: don't actually receive the data,
            on_duplicate_detected: how to handle the stream if it would result in a duplicate file.

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


class LineageTableNode(BaseModel):
    """Represents a node in a snapshot lineage table.

    A node can represent either a complete snapshot or an incremental snapshot,
    identified by its filename and date.

    Attributes:
        filename (str): Name of the snapshot file.
        datetime (str): Datetime string of the snapshot.
        node_type (Literal['complete', 'incremental']): Node type.
    """

    model_config = ConfigDict(frozen=True)

    filename: str
    datetime: str
    node_type: Literal['complete', 'incremental']
    
    def __str__(self):
        return self.filename

    def __eq__(self, other):
        return bool(self.date == other.date)

    def __hash__(self):
        return hash(self.filename)


type LineageTableChain = tuple[Optional[LineageTableNode], ...]


class LineageTable(BaseModel):
    """Represents a table of snapshot lineages.

    The table is organized as a matrix where:
    - Columns represent different backup chains/lineages.
    - Rows represent different datetimes.
    - Each cell contains either None or a LineageTableNode.
    - Complete nodes start new columns.
    - Incremental nodes build upon previous nodes in the same column.

    Example structure:
        Date1: [Complete, None,    None   ]
        Date2: [Inc1,    Complete, None   ]
        Date3: [Inc2,    Inc1,    Complete]

    Attributes:
        zfs_dataset_path (PurePath): Path to the ZFS dataset.
        index (tuple[Datetime, ...]): Tuple of datetimes representing the row labels.
        data (tuple[LineageTableChain, ...]): Matrix of LineageTableNodes.
        orphaned_nodes (tuple[LineageTableNode, ...]): Nodes not belonging to any valid lineage.
    """

    model_config = ConfigDict(frozen=True)

    zfs_dataset_path: PurePath
    index: tuple[Datetime, ...]
    data: tuple[LineageTableChain, ...]
    orphaned_nodes: tuple[LineageTableNode, ...]