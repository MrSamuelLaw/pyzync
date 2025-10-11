from abc import ABC, abstractmethod
from typing import Optional, Iterable, Sequence
from itertools import groupby

import humanize
from pydantic import BaseModel, ConfigDict

from pyzync import logging
from pyzync.errors import DataIntegrityError
from pyzync.streaming import SnapshotStreamProducer, SnapshotStreamConsumer, BytesConsumer
from pyzync.interfaces import (DuplicateDetectedPolicy, ZfsDatasetId, ZfsFilePath, SnapshotNode,
                               SnapshotGraph)
from pyzync.otel import trace, with_tracer

logger = logging.get_logger(__name__)
tracer = trace.get_tracer(__name__)


class SnapshotStorageAdapter(ABC):

    @abstractmethod
    def query(self, dataset_id: Optional[ZfsDatasetId]) -> Sequence[ZfsFilePath]:
        pass

    @abstractmethod
    def destroy(self, node: SnapshotNode):
        pass

    @abstractmethod
    def get_producer(self, node: SnapshotNode, bufsize: int = 4096) -> Iterable[bytes]:
        pass

    @abstractmethod
    def get_consumer(self, node: SnapshotNode) -> BytesConsumer:
        pass


class RemoteSnapshotManager(BaseModel):

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    adapter: SnapshotStorageAdapter

    def __str__(self):
        return f"RemoteSnapshotManager(adapter={str(self.adapter)})"

    @with_tracer(tracer)
    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        lgr = logger.bind(dataset_id=dataset_id, adapter=self.adapter)
        lgr.debug("Querying snapshots")
        files = self.adapter.query(dataset_id)
        nodes = [SnapshotNode.from_zfs_filepath(f) for f in files]
        nodes = sorted(nodes, key=lambda n: n.dataset_id)  # sort so the groupby works
        graphs: list[SnapshotGraph] = []
        for dataset_id, group in groupby(nodes, key=lambda n: n.dataset_id):
            graph = SnapshotGraph(dataset_id=dataset_id)
            [graph.add(node) for node in group]
            graphs.append(graph)
        return graphs

    @with_tracer(tracer)
    def destroy(self,
                node: SnapshotNode,
                graph: SnapshotGraph,
                dryrun: bool = False,
                prune: bool = False,
                force: bool = False):
        lgr = logger.bind(node=node, dryrun=dryrun, prune=prune, force=force)
        lgr.info("Destroying snapshot")
        # if there is only one chain, prevent the user from deleting anything but the last link
        chains = graph.get_chains()
        if (len(chains) == 1) and (node != chains[0][-1]) and (not force):
            raise DataIntegrityError(
                f'Unable to delete node = {node}, must set force = True to delete node')
        # remove the node
        graph.remove(node)
        if not dryrun:
            self.adapter.destroy(node)
        # prune if needed
        if prune:
            orphans = graph.get_orphans()
            for node in orphans:
                graph.remove(node)
                if not dryrun:
                    lgr.debug('Destroying orphaned node', node=node)
                    self.adapter.destroy(node)

    @with_tracer(tracer)
    def get_producer(
            self,
            node: SnapshotNode,
            graph: SnapshotGraph,
            bufsize: int = 100 * (2**20),  # 100 MD
            dryrun: bool = False):
        lgr = logger.bind(node=node,
                          bufsize=humanize.naturalsize(bufsize),
                          dryrun=dryrun,
                          adapter=self.adapter)
        lgr.info("Getting producer")
        if node not in graph.get_nodes():
            raise ValueError(f'Node = {node} not part of graph = {graph}')
        if dryrun:
            producer = SnapshotStreamProducer(node=node, generator=(b'0',))
        else:
            producer = SnapshotStreamProducer(node=node,
                                              generator=self.adapter.get_producer(node, bufsize=bufsize))
        return producer

    @with_tracer(tracer)
    def get_consumer(
            self,
            node: SnapshotNode,
            graph: SnapshotGraph,
            dryrun: bool = False,
            duplicate_policy: DuplicateDetectedPolicy = 'error') -> Optional[SnapshotStreamConsumer]:
        lgr = logger.bind(node=node, dryrun=dryrun, duplicate_policy=duplicate_policy)
        lgr.info("Getting consumer")
        is_duplicate = node in graph.get_nodes()
        if not is_duplicate:
            graph.add(node)
        elif duplicate_policy == 'error':
            raise ValueError(
                f'Cannot get consumer for duplicate node = {node} for adapter = {self.adapter}')
        if dryrun:

            def _dryrun_consumer(node: SnapshotNode):
                while True:
                    chunk = yield
                    if chunk is None:
                        break

            consumer = SnapshotStreamConsumer(node=node, generator=_dryrun_consumer(node))
        else:
            consumer = SnapshotStreamConsumer(node=node, generator=self.adapter.get_consumer(node))
        return consumer
