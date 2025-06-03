"""
Policies for snapshot retention.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime as Datetime
from typing import TypeAlias

from pydantic import BaseModel, Field, ConfigDict

from pyzync.interfaces import SnapshotRef

logger = logging.getLogger(__name__)

Keep: TypeAlias = list[SnapshotRef]
Destroy: TypeAlias = list[SnapshotRef]


class RetentionPolicy(ABC, BaseModel):

    model_config = ConfigDict(frozen=True)

    @abstractmethod
    def split(self, refs: list[SnapshotRef]) -> tuple[Keep, Destroy]:
        """For a given set of snapshot refs, determines which ones to keep
        and which ones to delete.
        """
        pass


class LastNSnapshotsPolicy(RetentionPolicy):

    n_snapshots: int = Field(ge=0, le=30)

    def split(self, refs: list[SnapshotRef]):
        refs = sorted(refs, key=lambda ref: ref.datetime, reverse=True)
        destroy = [ref for i, ref in enumerate(refs) if i >= self.n_snapshots]
        keep = [ref for ref in refs if ref not in destroy]
        return (keep, destroy)
