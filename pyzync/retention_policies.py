"""
Policies for snapshot retention.
"""

import logging
from abc import ABC, abstractmethod
from typing import TypeAlias

from pydantic import BaseModel, Field, ConfigDict

from pyzync.interfaces import SnapshotNode, SnapshotGraph

logger = logging.getLogger(__name__)

Keep: TypeAlias = list[SnapshotNode]
Destroy: TypeAlias = list[SnapshotNode]


class RetentionPolicy(ABC, BaseModel):

    model_config = ConfigDict(frozen=True)

    @abstractmethod
    def split(self, graph: SnapshotGraph) -> tuple[Keep, Destroy]:
        """For a given set of snapshot refs, determines which ones to keep
        and which ones to destroy.
        """
        pass


class LastNSnapshotsPolicy(RetentionPolicy):

    n_snapshots: int = Field(ge=0, le=30)

    def split(self, graph: SnapshotGraph):
        nodes = sorted(list(graph.get_nodes()), key=lambda node: node.dt, reverse=True)
        destroy = {node for i, node in enumerate(nodes) if i >= self.n_snapshots}
        keep = {node for node in nodes if node not in destroy}
        return (keep, destroy)
