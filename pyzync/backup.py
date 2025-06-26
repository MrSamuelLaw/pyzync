"""This module performs backup related tasks, like rotating the host snapshots and syncing
them with remotes using the sync method
"""

import logging

from pydantic import BaseModel, ConfigDict

from pyzync.host import HostSnapshotManager
from pyzync.storage_adapters import FileSnapshotManager
from pyzync.retention_policies import RetentionPolicy
from pyzync.interfaces import ZfsDatasetId, SnapshotGraph, DuplicateDetectedPolicy, SnapshotStorageAdapter, Datetime

logger = logging.getLogger(__name__)


class BackupJob(BaseModel):
    """
    Represents a backup configuration for a single zfs dataset on the host os.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    retention_policy: RetentionPolicy
    adapters: list[SnapshotStorageAdapter]

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
            new_node = host.create(dt, graph, dryrun=dryrun)
            keep, destroy = self.retention_policy.split(graph)
            for node in destroy:
                host.destroy(node, graph, dryrun=dryrun)
            return (keep, destroy)
        except Exception as e:
            logger.exception(f"Exception during rotation for {dataset_id}")
            raise

    def sync(self,
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
        for adapter in self.adapters:
            # get the graph for the remote
            manager = FileSnapshotManager(adapter=adapter)
            graphs = manager.query(dataset_id)
            remote_graph = graphs[0] if graphs else SnapshotGraph(dataset_id=dataset_id)

            # send the streams to the remote if they are not already on the remote
            streams = [HostSnapshotManager.send(chain[0].dt, host_graph)]
            streams.extend([
                HostSnapshotManager.send(node.dt, host_graph, parent.dt)
                for node, parent in zip(chain[1:], chain)
            ])
            for stream in streams:
                logger.info(f"Sending stream {stream} to remote {manager}")
                manager.recv(stream, remote_graph, dryrun=dryrun, duplicate_policy=duplicate_policy)

            # destroy old nodes from tip to root by using reverse=True
            streamed_nodes = [stream.node for stream in streams]
            old_nodes = [node for node in remote_graph.get_nodes() if node not in streamed_nodes]
            old_nodes = sorted(old_nodes, key=lambda node: node.dt, reverse=True)
            for node in old_nodes:
                logger.info(f"Destroying old remote nodes: {node}")
                manager.destroy(node, remote_graph, prune=prune, force=force, dryrun=dryrun)
