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
        return self.strftime(DATETIME_FORMAT)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.union_schema([
            core_schema.no_info_after_validator_function(cls, handler(str)),
            core_schema.no_info_after_validator_function(cls, handler(datetime)),
            core_schema.is_instance_schema(cls)
        ])


class ZfsDatasetId(str):
    
    _pattern = re.compile(r"(\w)+(\w|\/)+")

    def __new__(cls, path: str):
        if not cls._pattern.fullmatch(path):
            raise ValueError("ZfsDatasetPath must not start with '.' or '/'")
        return str.__new__(cls, path)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))


class ZfsSnapshotId(str):
    
    _pattern = re.compile(r"(\w)+(\w|\/)+(@{inj})".format(inj=DATETIME_REGEX))

    def __new__(cls, snapshot_id):
        if not cls._pattern.fullmatch(snapshot_id):
            raise ValueError(f"ZfsSnapshotId = {snapshot_id} is not valid")
        return str.__new__(cls, snapshot_id)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))


class ZfsFilePath(str):
    
    _pattern = re.compile(r"(\w)+(\w|\/)+(({inj})|({inj}_{inj})).zfs".format(inj=DATETIME_REGEX))
    _dt_pattern = re.compile(DATETIME_REGEX)

    def __new__(cls, path):
        if not cls._pattern.fullmatch(path):
            raise ValueError(f"ZfsFilePath = {path} is not valid")

        dts = cls._dt_pattern.findall(path)
        dts = [Datetime(dt) for dt in dts]
        
        return str.__new__(cls, path)

    @classmethod
    def __get_pydantic_core_schema__(cls, _: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))


SnapshotNodeType = Literal['complete', 'incremental']


class SnapshotNode(BaseModel):

    model_config = ConfigDict(frozen=True)

    dt: Datetime
    parent_dt: Optional[Datetime] = None
    dataset_id: ZfsDatasetId
    
    @model_validator(mode='after')
    def validate_data(self):
        if (self.parent_dt is not None) and (self.parent_dt >= self.dt):
            raise ValueError('SnapshotNode.parent_dt must be less than self.dt.' \
                             f'\n found older parent_dt on SnapshotNode = {self}.')
        return self

    @computed_field
    @property
    def node_type(self) -> SnapshotNodeType:
        return 'complete' if self.parent_dt is None else 'incremental'

    @computed_field
    @property
    def snapshot_id(self) -> ZfsSnapshotId:
        return ZfsSnapshotId(f'{self.dataset_id}@{self.dt}')

    @computed_field
    @property
    def filepath(self) -> ZfsFilePath:
        if self.parent_dt is not None:
            filepath = f'{self.dataset_id}/{self.parent_dt}_{self.dt}.zfs'
        else:
            filepath = f'{self.dataset_id}/{self.dt}.zfs'
        return ZfsFilePath(filepath)

    def __str__(self):
        return self.filepath
    
    def __hash__(self):
        return hash(self.filepath)

    @classmethod
    @validate_call
    def from_zfs_snapshot_id(cls, snapshot_id: ZfsSnapshotId):
        dataset_id, dt = snapshot_id.split('@')
        return cls(dataset_id=dataset_id, dt=dt)
    
    @classmethod
    @validate_call
    def from_zfs_filepath(cls, filepath: ZfsFilePath):
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

    model_config = ConfigDict(frozen=True)

    dataset_id: ZfsDatasetId
    _table: defaultdict[Datetime, set[SnapshotNode]] = defaultdict(set)

    def get_nodes(self):
        return set(chain(*self._table.values()))

    def get_orphans(self):
        # get the chains as a flat set
        chains = self.get_chains()
        chained_nodes = set(chain(*chains))
        # get all the nodes as a flat set
        nodes = self.get_nodes()
        # orphaned nodes are all nodes not found in a chain
        orphans = nodes - chained_nodes
        return orphans
    
    def get_chains(self):
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
        if node.dataset_id != self.dataset_id:
            raise ValueError(f'Failed to add node = {node} to table, node & table have different dataset_ids.')
        group = self._table[node.dt]
        if node in group:
            raise ValueError(f'Cannot add duplicate node = {node}')
        group.add(node)

    def remove(self, node: SnapshotNode):
        try:
            group = self._table[node.dt]
            group.remove(node)
            if not group:
                del self._table[node.dt]
        except KeyError:
            raise ValueError(f'Unable to remove node = {node}, it does not exist in the graph')


class SnapshotStream(BaseModel):
    
    model_config = ConfigDict(frozen=True)

    node: SnapshotNode
    bytes_stream: Iterable[bytes]
    
    def __str__(self):
        return f"SnapshotStream(node={self.node.snapshot_id}, filepath={self.node.filepath})"


class SnapshotStorageAdapter(ABC):

    @abstractmethod
    def query(dataset_id: Optional[ZfsDatasetId]) -> list[SnapshotGraph]:
        pass

    @abstractmethod
    def destroy(node: SnapshotNode):
        pass

    @abstractmethod
    def send(filepath: ZfsFilePath, blocksize: int = 4096) -> SnapshotStream:
        pass
    
    @abstractmethod
    def recv(stream: SnapshotStream):
        pass