import logging
from abc import ABC, abstractmethod
from typing import Optional, Iterable, Sequence
from itertools import groupby

from pydantic import BaseModel, ConfigDict

from pyzync.errors import DataIntegrityError
from pyzync.streaming import SnapshotStreamProducer, SnapshotStreamConsumer, BytesConsumer
from pyzync.interfaces import (DuplicateDetectedPolicy, ZfsDatasetId, ZfsFilePath, SnapshotNode,
                               SnapshotGraph)
from pyzync.otel import trace, with_tracer

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class SnapshotStorageAdapter(ABC):

    @abstractmethod
    def query(self, dataset_id: Optional[ZfsDatasetId]) -> Sequence[ZfsFilePath]:
        pass

    @abstractmethod
    def destroy(self, node: SnapshotNode):
        pass

    @abstractmethod
    def get_producer(self, node: SnapshotNode, blocksize: int = 4096) -> Iterable[bytes]:
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
        logger.info(
            "[RemoteStorageAdapter] query called for " \
            f"adapter={self.adapter} with dataset_id={dataset_id}"
        )
        files = self.adapter.query(dataset_id)
        nodes = [SnapshotNode.from_zfs_filepath(f) for f in files]
        nodes = sorted(nodes, key=lambda n: n.dataset_id)  # sort so the groupby works
        graphs: list[SnapshotGraph] = []
        for dataset_id, group in groupby(nodes, key=lambda n: n.dataset_id):
            graph = SnapshotGraph(dataset_id=dataset_id)
            [graph.add(node) for node in group]
            graphs.append(graph)
        logger.info(f"[RemoteStorageAdapter] query for adapter={self.adapter} " \
                    f"with dataset_id={dataset_id} returned {len(graphs)} graphs")
        return graphs

    @with_tracer(tracer)
    def destroy(self,
                node: SnapshotNode,
                graph: SnapshotGraph,
                dryrun: bool = False,
                prune: bool = False,
                force: bool = False):
        logger.info(f"[RemoteStorageAdapter] destroy called for adapter={self.adapter} with node={node}")
        # if there is only one chain, prevent the user from deleting anything but the last link
        chains = graph.get_chains()
        if (len(chains) == 1) and (node != chains[0][-1]) and (not force):
            raise DataIntegrityError(
                f'Unable to delete node = {node}, must set force = True to delete node')
        # remove the node
        graph.remove(node)
        if not dryrun:
            logger.debug(f'Destroying node = {node}')
            self.adapter.destroy(node)
        # prune if needed
        if prune:
            orphans = graph.get_orphans()
            for node in orphans:
                graph.remove(node)
                if not dryrun:
                    logger.debug(f'Destroying node = {node}')
                    self.adapter.destroy(node)
        logger.info(
            f"[RemoteStorageAdapter] Successfully destroyed called for adapter={self.adapter} with node={node}"
        )

    @with_tracer(tracer)
    def get_producer(self,
                     node: SnapshotNode,
                     graph: SnapshotGraph,
                     blocksize: int = 4096,
                     dryrun: bool = False):
        logger.info(f"[RemoteStorageAdapter] Send called for adapter={self.adapter} with node={node}")
        if node not in graph.get_nodes():
            raise ValueError(f'Node = {node} not part of graph = {graph}')
        if dryrun:
            stream = SnapshotStreamProducer(node=node, generator=(b'0',))
        else:
            stream = SnapshotStreamProducer(node=node,
                                            generator=self.adapter.get_producer(node,
                                                                                blocksize=blocksize))
        logger.info(
            f"[RemoteStorageAdapter] Stream successfully built for adapter={self.adapter} with node={node}"
        )
        return stream

    @with_tracer(tracer)
    def get_consumer(
            self,
            node: SnapshotNode,
            graph: SnapshotGraph,
            dryrun: bool = False,
            duplicate_policy: DuplicateDetectedPolicy = 'error') -> Optional[SnapshotStreamConsumer]:
        logger.info(
            f"[RemoteStorageAdapter] getting consumer for adapter={self.adapter} and node={node}")
        is_duplicate = node in graph.get_nodes()
        if not is_duplicate:
            graph.add(node)
        elif duplicate_policy == 'error':
            raise ValueError(
                f'Cannot get consumer for duplicate node = {node} for adapter = {self.adapter}')
        if not dryrun:
            return SnapshotStreamConsumer(node=node, generator=self.adapter.get_consumer(node))
        logger.info(
            f"[RemoteStorageAdapter] Successfully got consumer for adapter={self.adapter} and node={node}"
        )
