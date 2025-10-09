from pydantic import Field

from pyzync.interfaces import SnapshotGraph
from pyzync.retention.interfaces import RetentionPolicy


class LastNSnapshotsPolicy(RetentionPolicy):

    n_snapshots: int = Field(ge=0, le=30)

    def split(self, graph: SnapshotGraph):
        nodes = sorted(list(graph.get_nodes()), key=lambda node: node.dt, reverse=True)
        destroy = {node for i, node in enumerate(nodes) if i >= self.n_snapshots}
        keep = {node for node in nodes if node not in destroy}
        return (keep, destroy)
