import re
from pathlib import PurePath
from itertools import chain
from collections import defaultdict
from abc import ABC, abstractmethod
from datetime import datetime, tzinfo
from typing import Self, Iterable, Optional, Literal, overload, Any

from pydantic import (BaseModel, Field, BeforeValidator, model_validator, field_validator,
                      computed_field, ConfigDict, GetCoreSchemaHandler, validate_call)
from pydantic_core import CoreSchema, core_schema


DATETIME_FORMAT = r"%Y%m%dT%H%M%S"
DATETIME_REGEX = r'\d{8}T\d{6}'
DATETIME_STR_LENGTH = len('yyyymmddThhmmss')

DuplicateDetectedPolicy = Literal['error', 'ignore', 'overwrite']


class Datetime(datetime):
    """
    Extended datetime class for ZFS snapshot naming and parsing.
    """

    @overload
    def __new__(cls, iso_string: str) -> Self:
        pass

    @overload
    def __new__(cls, dt: datetime) -> Self:
        pass

    @overload
    def __new__(cls,
                year: int,
                month: int,
                day: int,
                hour: int = 0,
                minute: int = 0,
                second: int = 0,
                microsecond: int = 0,
                tzinfo: Optional[tzinfo] = None,
                *,
                fold: Literal[0, 1] = 0):
        pass

    def __new__(cls, *args, **kwargs):
        """
        Create a new Datetime instance from string, datetime, or components.

        Raises:
            ValueError: If the string or components are invalid.
        """
        if (len(args) == 1):
            if (type(args[0]) == str):
                iso_string = args[0]
                return cls.fromisoformat(iso_string)
            elif (type(args[0]) == datetime):
                dt = args[0]
                return datetime.__new__(cls,
                                        dt.year,
                                        dt.month,
                                        dt.day,
                                        dt.hour,
                                        dt.minute,
                                        dt.second,
                                        tzinfo=dt.tzinfo,
                                        fold=dt.fold)
        else:
            return datetime.__new__(cls, *args, **kwargs)

    def __str__(self):
        """
        Return the string representation in ZFS datetime format.
        """
        return self.strftime(DATETIME_FORMAT)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """
        Pydantic core schema for Datetime.
        """
        return core_schema.union_schema([
            core_schema.no_info_after_validator_function(cls, handler(str)),
            core_schema.no_info_after_validator_function(cls, handler(datetime)),
            core_schema.is_instance_schema(cls)
        ])


class ZfsDatasetId(str):
    """
    String type for ZFS dataset IDs with validation.
    """
    
    _pattern = re.compile(r"(\w)+(\w|\/)+")

    def __new__(cls, path: str):
        """
        Validate and create a new ZfsDatasetId.

        Raises:
            ValueError: If the path is not a valid ZFS dataset ID.
        """
        if not cls._pattern.fullmatch(path):
            raise ValueError("ZfsDatasetPath must not start with '.' or '/'")
        return str.__new__(cls, path)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """
        Pydantic core schema for ZfsDatasetId.
        """
        return core_schema.no_info_after_validator_function(cls, handler(str))


class ZfsSnapshotId(str):
    """
    String type for ZFS snapshot IDs with validation.
    """
    
    _pattern = re.compile(r"(\w)+(\w|\/)+(@{inj})".format(inj=DATETIME_REGEX))

    def __new__(cls, snapshot_id):
        """
        Validate and create a new ZfsSnapshotId.

        Raises:
            ValueError: If the snapshot_id is not valid.
        """
        if not cls._pattern.fullmatch(snapshot_id):
            raise ValueError(f"ZfsSnapshotId = {snapshot_id} is not valid")
        return str.__new__(cls, snapshot_id)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """
        Pydantic core schema for ZfsSnapshotId.
        """
        return core_schema.no_info_after_validator_function(cls, handler(str))


class ZfsFilePath(str):
    """
    String type for ZFS file paths with validation.
    """
    
    _pattern = re.compile(r"(\w)+(\w|\/)+(({inj})|({inj}_{inj})).zfs".format(inj=DATETIME_REGEX))
    _dt_pattern = re.compile(DATETIME_REGEX)

    def __new__(cls, path):
        """
        Validate and create a new ZfsFilePath.

        Raises:
            ValueError: If the path is not a valid ZFS file path.
        """
        if not cls._pattern.fullmatch(path):
            raise ValueError(f"ZfsFilePath = {path} is not valid")

        dts = cls._dt_pattern.findall(path)
        dts = [Datetime(dt) for dt in dts]
        
        return str.__new__(cls, path)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """
        Pydantic core schema for ZfsFilePath.
        """
        return core_schema.no_info_after_validator_function(cls, handler(str))


SnapshotNodeType = Literal['complete', 'incremental']


class SnapshotNode(BaseModel):
    """
    Node representing a ZFS snapshot, complete or incremental.
    """

    model_config = ConfigDict(frozen=True)

    dt: Datetime
    parent_dt: Optional[Datetime] = None
    dataset_id: ZfsDatasetId
    
    @model_validator(mode='after')
    def validate_data(self):
        """
        Validate that parent_dt is less than dt if present.

        Raises:
            ValueError: If parent_dt is not less than dt.
        """
        if (self.parent_dt is not None) and (self.parent_dt >= self.dt):
            raise ValueError('SnapshotNode.parent_dt must be less than self.dt.' \
                             f'\n found older parent_dt on SnapshotNode = {self}.')
        return self

    @computed_field
    @property
    def node_type(self) -> SnapshotNodeType:
        """
        Return the type of snapshot node ('complete' or 'incremental').
        """
        return 'complete' if self.parent_dt is None else 'incremental'

    @computed_field
    @property
    def snapshot_id(self) -> ZfsSnapshotId:
        """
        Return the ZFS snapshot ID for this node.
        """
        return ZfsSnapshotId(f'{self.dataset_id}@{self.dt}')

    @computed_field
    @property
    def filepath(self) -> ZfsFilePath:
        """
        Return the ZFS file path for this node.
        """
        if self.parent_dt is not None:
            filepath = f'{self.dataset_id}/{self.parent_dt}_{self.dt}.zfs'
        else:
            filepath = f'{self.dataset_id}/{self.dt}.zfs'
        return ZfsFilePath(filepath)

    def __str__(self):
        """
        Return the string representation (file path) of the node.
        """
        return self.filepath
    
    def __hash__(self):
        """
        Return the hash of the node's file path.
        """
        return hash(self.filepath)

    @classmethod
    @validate_call
    def from_zfs_snapshot_id(cls, snapshot_id: ZfsSnapshotId):
        """
        Create a SnapshotNode from a ZFS snapshot ID.

        Raises:
            ValueError: If the snapshot_id is not valid.
        """
        dataset_id, dt = snapshot_id.split('@')
        return cls(dataset_id=dataset_id, dt=dt)
    
    @classmethod
    @validate_call
    def from_zfs_filepath(cls, filepath: ZfsFilePath):
        """
        Create a SnapshotNode from a ZFS file path.

        Raises:
            ValueError: If the filepath is not valid.
        """
        parts = filepath.split('/')
        dataset_id = '/'.join(parts[:-1])
        name = parts[-1]
        dates = filepath._dt_pattern.findall(name)
        node = cls(
            dataset_id = dataset_id,
            dt = dates[-1],
            parent_dt = dates[0] if len(dates) == 2 else None
        )
        return node
    

type SnapshotChain = list[Optional[SnapshotNode]] 


class SnapshotGraph(BaseModel):
    """
    Graph structure for managing ZFS snapshot nodes and their relationships.
    """

    model_config = ConfigDict(frozen=True)

    dataset_id: ZfsDatasetId
    _table: defaultdict[Datetime, set[SnapshotNode]] = defaultdict(set)

    def get_nodes(self):
        """
        Return all nodes in the graph.
        """
        return set(chain(*self._table.values()))

    def get_orphans(self):
        """
        Return orphaned nodes not found in any chain.
        """
        # get the chains as a flat set
        chains = self.get_chains()
        chained_nodes = set(chain(*chains))
        # get all the nodes as a flat set
        nodes = self.get_nodes()
        # orphaned nodes are all nodes not found in a chain
        orphans = nodes - chained_nodes
        return orphans
    
    def get_chains(self):
        """
        Return all chains of snapshots in the graph.
        """
        # pop the biggest group as the starting point, since that will be how many lineages there will be.
        nodes = self.get_nodes()
        _chains = [[n] for n in nodes if n.node_type == 'complete']
        nodes = {n for n in nodes if n .node_type == 'incremental'}
        i = 0
        chains: list[SnapshotChain] = []
        while i < 1024 and _chains:
            i += 1
            # pop the chain and find all the children of the tip node
            chain_ = _chains.pop()
            tip = chain_[-1]
            children = {n for n in nodes if n.parent_dt == tip.dt}
            # the chain will split if we have multiple children
            if children:
                for child in children:
                    _chains.append([c for c in chain_] + [child])
            else:
                chains.append(chain_)
        return chains
    
    def add(self, node: SnapshotNode) -> None:
        """
        Add a node to the graph.

        Raises:
            ValueError: If the node's dataset_id does not match or is duplicate.
        """
        if node.dataset_id != self.dataset_id:
            raise ValueError(f'Failed to add node = {node} to table, node & table have different dataset_ids.')
        group = self._table[node.dt]
        if node in group:
            raise ValueError(f'Cannot add duplicate node = {node}')
        group.add(node)

    def remove(self, node: SnapshotNode):
        """
        Remove a node from the graph.

        Raises:
            ValueError: If the node does not exist in the graph.
        """
        try:
            group = self._table[node.dt]
            group.remove(node)
            if not group:
                del self._table[node.dt]
        except KeyError:
            raise ValueError(f'Unable to remove node = {node}, it does not exist in the graph')


class SnapshotStream(BaseModel):
    """
    Represents a stream of snapshot data for transfer.
    """
    
    model_config = ConfigDict(frozen=True)

    node: SnapshotNode
    bytes_stream: Iterable[bytes]
    
    def __str__(self):
        """
        Return a string representation of the snapshot stream.
        """
        return f"SnapshotStream(node={self.node.snapshot_id}, filepath={self.node.filepath})"


class SnapshotStorageAdapter(ABC):
    """
    Abstract base class for snapshot storage adapters.
    """

    @abstractmethod
    def query(dataset_id: Optional[ZfsDatasetId]) -> list[SnapshotGraph]:
        """
        Query for snapshot graphs for a dataset.
        """
        pass

    @abstractmethod
    def destroy(node: SnapshotNode):
        """
        Destroy a snapshot node in storage.
        """
        pass

    @abstractmethod
    def send(filepath: ZfsFilePath, blocksize: int = 4096) -> 'SnapshotStream':
        """
        Send a snapshot file as a stream.
        """
        pass

    @abstractmethod
    def recv(stream: 'SnapshotStream'):
        """
        Receive a snapshot stream and store it.
        """
        pass