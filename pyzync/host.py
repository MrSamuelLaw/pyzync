import sys
import asyncio
import subprocess
import logging
from functools import partial
from textwrap import dedent
from itertools import groupby
from typing import Optional, Iterable, AsyncIterator

from pydantic import validate_call

from pyzync.errors import DataIntegrityError
from pyzync.interfaces import (ZfsDatasetId, SnapshotNode, SnapshotGraph, SnapshotStream, Datetime,
                               DATETIME_REGEX)

logger = logging.getLogger(__name__)


class HostSnapshotManager:
    """
    Manages ZFS snapshots on the host system.

    Provides functionality for creating, querying, destroying, and sending both complete
    and incremental snapshots using ZFS commands.
    """

    @staticmethod
    @validate_call
    def create(dt: Datetime, graph: SnapshotGraph, dryrun: bool = False):
        """
        Create a snapshot for the given dataset at the specified datetime.

        Args:
            dt (Datetime): The datetime for the snapshot.
            graph (SnapshotGraph): The snapshot graph to update.
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.

        Returns:
            SnapshotNode: The created snapshot node.

        Raises:
            subprocess.CalledProcessError: If the ZFS snapshot command fails.
            Exception: For other errors during snapshot creation.
        """
        logger.info(f"Creating snapshot for {graph.dataset_id} on {dt}.")
        try:
            node = SnapshotNode(dataset_id=graph.dataset_id, dt=dt)
            graph.add(node)
            if not dryrun:
                subprocess.run(
                    ["bash", "-c", f"zfs snapshot {node.snapshot_id}"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
            return node
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to create snapshot for {graph.dataset_id} on {dt}")
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    @validate_call
    def query(dataset_id: Optional[ZfsDatasetId] = None):
        """
        Query all snapshots for a given dataset or all datasets.

        Args:
            dataset_id (Optional[ZfsDatasetId], optional): The dataset to query. Defaults to None.

        Returns:
            list[SnapshotGraph]: List of snapshot graphs for each dataset.

        Raises:
            subprocess.CalledProcessError: If the ZFS list or grep command fails.
            Exception: For other errors during query.
        """
        logger.debug(f"Querying SnapshotNodes for {dataset_id}")

        try:
            if dataset_id is None:
                regex = r"(\w|\/)+@" + DATETIME_REGEX
            else:
                dataset_id = ZfsDatasetId(dataset_id)
                regex = str(dataset_id) + "@" + DATETIME_REGEX
            result = subprocess.run(
                ["bash", "-c", f'zfs list -t snapshot -o name | grep -P "{regex}"'],
                capture_output=True,
                text=True,
                check=True,
            )
            ids = result.stdout.splitlines()
            nodes = [SnapshotNode.from_zfs_snapshot_id(snapshot_id) for snapshot_id in ids]
            nodes = sorted(nodes, key=lambda node: node.dataset_id)
            graphs: list[SnapshotGraph] = []
            for dataset_id, group in groupby(nodes, key=lambda node: node.dataset_id):
                graph = SnapshotGraph(dataset_id=dataset_id)
                [graph.add(node) for node in group]
                graphs.append(graph)
            return graphs

        except subprocess.CalledProcessError as e:
            # returncode == -1 indicates grep didn't find any matches
            if (e.stderr.strip() == "no datasets available") or (e.returncode == 1):
                if dataset_id is None:
                    result: list[SnapshotGraph] = []
                else:
                    result = [SnapshotGraph(dataset_id=dataset_id)]
                return result
            logger.exception(f"Failed to query snapshots for {dataset_id}")
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    @validate_call
    def destroy(node: SnapshotNode, graph: SnapshotGraph, dryrun: bool = False):
        """
        Destroy a snapshot node from the graph and the ZFS system.

        Args:
            node (SnapshotNode): The snapshot node to destroy.
            graph (SnapshotGraph): The snapshot graph to update.
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.

        Raises:
            subprocess.CalledProcessError: If the ZFS destroy command fails.
            Exception: For other errors during destroy.
        """
        logger.info(f"Destroying snapshot for snapshot {node.snapshot_id} with dryrun = {dryrun}")
        try:
            graph.remove(node)
            if not dryrun:
                subprocess.run(
                    ["bash", "-c", f"zfs destroy -d {node.snapshot_id}"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to destroy snapshot {node.snapshot_id}")
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    @validate_call
    def send(
            dt: Datetime,
            graph: SnapshotGraph,
            parent_dt: Optional[Datetime] = None,
            zfs_args: list[str] = [],
            blocksize: int = 2**20,  # 1 MB
            dryrun: bool = False):
        """
        Send a snapshot or incremental snapshot as a stream.

        Args:
            dt (Datetime): The datetime of the snapshot to send.
            graph (SnapshotGraph): The snapshot graph.
            parent_dt (Optional[Datetime], optional): The parent snapshot datetime for incremental. Defaults to None.
            zfs_args (list[str], optional): Additional ZFS arguments. Defaults to [].
            blocksize (int, optional): Block size for streaming. Defaults to 4096.
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.

        Returns:
            SnapshotStream: The snapshot stream object.

        Raises:
            DataIntegrityError: If parent_dt is not less than dt.
            ValueError: If the graph does not contain the required nodes.
            subprocess.CalledProcessError: If the ZFS send command fails.
            Exception: For other errors during send.
        """
        if (parent_dt is not None) and (parent_dt >= dt):
            raise DataIntegrityError(
                dedent("""
                    Cannot create incremental snapshots that link a newer snapshot to an older snapshot
                    Incremental snapshots most link an older snapshot to a newer snapshot
                """))

        # check that the nodes exist in the graph
        nodes = graph.get_nodes()
        node = [n for n in nodes if n.dt == dt and n.parent_dt is None]
        if not node:
            raise ValueError(f'Graph does not contain complete node with dt = {dt}')
        node = node[0]
        if parent_dt is not None:
            if not any((n for n in nodes if n.dt == parent_dt and n.parent_dt is None)):
                raise ValueError(f'Graph does not contain complete node with dt = {parent_dt}')
            # update the node definition if the parent exists
            node = SnapshotNode(dt=node.dt, parent_dt=parent_dt, dataset_id=node.dataset_id)

        def _iterator():
            try:
                # build the command
                cmd = ["zfs", "send", *zfs_args]
                if node.parent_dt is not None:
                    # note we only pass the part after the @ to avoid mixing datasets by accident
                    cmd.extend(["-i", str(node.parent_dt)])
                cmd.append(node.snapshot_id)

                # using a context manager, build an unbuffered generator to stream the data
                with subprocess.Popen(cmd,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      bufsize=blocksize) as process:
                    # check to make sure the pipe is open
                    if process.stdout is None:
                        raise RuntimeError("Failed to open stdout for the subprocess")

                    # iterate over the binary data
                    for block in iter(partial(process.stdout.read, blocksize), b""):
                        yield block

                    # check for non-zero return code
                    returncode = process.wait(1)
                    if returncode:
                        error = process.stderr.read()
                        raise subprocess.CalledProcessError(returncode, cmd, stderr=error)
            except subprocess.CalledProcessError as e:
                logger.exception(f"Failed to send snapshot using cmd = {cmd}")
                print(e.stderr, file=sys.stderr)
                raise

        if dryrun:
            iterator = (bytes(b) for b in b'somebytes')
        else:
            iterator: Iterable[bytes] = _iterator()
        stream = SnapshotStream(
            node=node,
            bytes_stream=iterator,
        )
        return stream

    @staticmethod
    @validate_call
    def recv(stream: SnapshotStream,
             graph: SnapshotGraph,
             zfs_args: list[str] = [],
             dryrun: bool = False):
        """
        Receive a snapshot stream and add it to the graph and ZFS system.

        Args:
            stream (SnapshotStream): The snapshot stream to receive.
            graph (SnapshotGraph): The snapshot graph to update.
            zfs_args (list[str], optional): Additional ZFS arguments. Defaults to [].
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.

        Raises:
            subprocess.CalledProcessError: If the ZFS recv command fails.
            Exception: For other errors during receive.
        """
        logger.info(f"Receiving snapshot stream for {stream}")
        # make sure to recieve as a non-incremental node into the graph
        if stream.node.node_type == 'complete':
            graph.add(stream.node)
        else:
            data = stream.node.model_dump()
            del data['parent_dt']
            graph.add(SnapshotNode(**data))
        # Use only the dataset name for zfs recv target
        cmd = ["zfs", "recv", *zfs_args, stream.node.dataset_id]
        if not dryrun:
            try:
                with subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
                                      bufsize=0) as process:

                    # Check that the pipe is open
                    if process.stdin is None:
                        raise RuntimeError("Failed to open stdin for the subprocess")

                    # write the data
                    for chunk in stream.bytes_stream:
                        process.stdin.write(chunk)

                    # flush and close the buffer
                    process.stdin.flush()
                    process.stdin.close()

                    # wait for the process to complete
                    returncode = process.wait(1)
                    if returncode:
                        error = process.stderr.read()
                        raise subprocess.CalledProcessError(returncode, cmd, stderr=error)
            except subprocess.CalledProcessError as e:
                logger.exception(f"Failed to receive snapshot stream for {stream}")
                print(e.stderr, file=sys.stderr)
                raise
