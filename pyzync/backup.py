import logging
import asyncio

from pydantic import BaseModel, ConfigDict

from pyzync.otel import trace, with_tracer
from pyzync.host import HostSnapshotManager
from pyzync.streaming import SnapshotStreamManager
from pyzync.retention.interfaces import RetentionPolicy
from pyzync.storage.interfaces import SnapshotStorageAdapter, RemoteSnapshotManager
from pyzync.interfaces import ZfsDatasetId, SnapshotGraph, DuplicateDetectedPolicy, Datetime

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class BackupJob(BaseModel):

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    retention_policy: RetentionPolicy
    buffer_length: int = 100 * (2**20)  # 100 MB is the default buffer length
    adapters: list[SnapshotStorageAdapter]

    @with_tracer(tracer)
    def rotate(self,
               dataset_id: ZfsDatasetId,
               host: HostSnapshotManager = HostSnapshotManager,
               dt: Datetime = Datetime.now(),
               dryrun: bool = False):
        # create a new snapshot and delete the old ones using the retention policy
        logger.info(f"Rotating snapshots for dataset {dataset_id}")
        try:
            graphs = host.query(dataset_id)
            graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
            _ = host.create(dt, graph, dryrun=dryrun)
            keep, destroy = self.retention_policy.split(graph)
            for node in destroy:
                host.destroy(node, graph, dryrun=dryrun)
            return (keep, destroy)
        except Exception:
            logger.exception(f"Exception during rotation for {dataset_id}")
            raise

    @with_tracer(tracer)
    async def sync(self,
                   dataset_id: ZfsDatasetId,
                   host: HostSnapshotManager = HostSnapshotManager,
                   force: bool = False,
                   dryrun: bool = False,
                   prune: bool = True,
                   duplicate_policy: DuplicateDetectedPolicy = 'ignore'):

        # get the host graph
        graphs = host.query(dataset_id)
        host_graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
        chain = sorted(list(host_graph.get_nodes()), key=lambda node: node.dt)

        remotes: list[tuple[RemoteSnapshotManager, SnapshotGraph]] = []
        for adapter in self.adapters:
            # get the graph for the remote
            manager = RemoteSnapshotManager(adapter=adapter)
            graphs = manager.query(dataset_id)
            remote_graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)
            remotes.append((manager, remote_graph))

        # send the streams to the remote if they are not already on the remote
        producers = [
            HostSnapshotManager.get_producer(chain[0].dt, host_graph, buffer_length=self.buffer_length)
        ]
        producers.extend([
            HostSnapshotManager.get_producer(node.dt, host_graph, parent.dt)
            for node, parent in zip(chain[1:], chain)
        ])

        # build the data streams
        stream_managers = []
        for producer in producers:
            consumers = []
            for manager, remote_graph in remotes:
                logger.info(f"Subscribing manager {manager} to producer {producer}")
                consumer = manager.get_consumer(producer.node,
                                                graph=remote_graph,
                                                dryrun=dryrun,
                                                duplicate_policy=duplicate_policy)
                consumers.append(consumer)
            stream_manager = SnapshotStreamManager(producer, consumers)
            stream_managers.append(stream_manager)

        # transmit the streams in parallel
        await asyncio.gather(*[sm.transmit() for sm in stream_managers])

        streamed_nodes = [producer.node for producer in producers]
        for manager, remote_graph in remotes:
            old_nodes = [node for node in remote_graph.get_nodes() if node not in streamed_nodes]
            old_nodes = sorted(old_nodes, key=lambda node: node.dt, reverse=True)
            for node in old_nodes:
                logger.info(f"Destroying old remote nodes: {node} for manager {manager}")
                manager.destroy(node, remote_graph, prune=prune, force=force, dryrun=dryrun)
