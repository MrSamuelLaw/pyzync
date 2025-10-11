from abc import ABC, abstractmethod
from typing import TypeAlias

from pydantic import BaseModel, ConfigDict

from pyzync.interfaces import SnapshotNode, SnapshotGraph

Keep: TypeAlias = set[SnapshotNode]
Destroy: TypeAlias = set[SnapshotNode]


class RetentionPolicy(ABC, BaseModel):

    model_config = ConfigDict(frozen=True)

    @abstractmethod
    def split(self, graph: SnapshotGraph) -> tuple[Keep, Destroy]:
        pass
