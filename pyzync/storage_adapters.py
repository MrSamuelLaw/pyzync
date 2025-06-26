"""Storage adapter implementations for handling snapshot data persistence.

This module provides concrete implementations of the SnapshotStorageAdapter interface
for different storage backends. Currently supports local filesystem storage.
"""

import re
import logging
from pathlib import Path
from typing import Optional
from itertools import groupby

from pydantic import BaseModel, ConfigDict, field_validator

from pyzync.errors import DataIntegrityError
from pyzync.interfaces import (SnapshotStorageAdapter, SnapshotStream, DuplicateDetectedPolicy,
                               DATETIME_REGEX, ZfsDatasetId, ZfsFilePath, SnapshotNode, SnapshotGraph)

logger = logging.getLogger(__name__)


class FileSnapshotManager(BaseModel):

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    adapter: SnapshotStorageAdapter

    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        files = self.adapter.query(dataset_id)
        nodes = [SnapshotNode.from_zfs_filepath(f) for f in files]
        graphs: list[SnapshotGraph] = []
        for dataset_id, group in groupby(nodes, key=lambda n: n.dataset_id):
            graph = SnapshotGraph(dataset_id=dataset_id)
            [graph.add(node) for node in group]
            graphs.append(graph)
        return graphs

    def destroy(self,
                node: SnapshotNode,
                graph: SnapshotGraph,
                dryrun: bool = False,
                prune: bool = False,
                force: bool = False):

        logger.info(f'Destroying node = {node}')
        # if there is only one chain, prevent the user from deleting anything but the last link
        chains = graph.get_chains()
        if (len(chains) == 1) and (node != chains[0][-1]) and (not force):
            raise DataIntegrityError(
                f'Unable to delete node = {node}, must set force = True to delete node')
        # remove the node
        graph.remove(node)
        if not dryrun:
            self.adapter.destroy(node.filepath)
        # prune if needed
        if prune:
            orphans = graph.get_orphans()
            for o in orphans:
                logger.info(f'Destroying node = {o}')
                graph.remove(o)
                if not dryrun:
                    self.adapter.destroy(o.filepath)

    def send(self,
             node: SnapshotNode,
             graph: SnapshotGraph,
             blocksize: int = 4096,
             dryrun: bool = False):
        if node not in graph.get_nodes():
            raise ValueError(f'Node = {node} not part of graph = {graph}')
        if dryrun:
            stream = SnapshotStream(node=node, bytes_stream=(b'bytes_stream',))
        else:
            stream = SnapshotStream(node=node,
                                    bytes_stream=self.adapter.send(node.filepath, blocksize=blocksize))
        return stream

    def recv(self,
             stream: SnapshotStream,
             graph: SnapshotGraph,
             dryrun: bool = False,
             duplicate_policy: DuplicateDetectedPolicy = 'error'):
        logger.info(f'Receiveing stream for node = {stream.node}')
        is_duplicate = stream.node in graph.get_nodes()
        if (not is_duplicate) or (is_duplicate and duplicate_policy == 'error'):
            graph.add(stream.node)
        if (not dryrun) and ((not is_duplicate) or (is_duplicate and duplicate_policy == 'overwrite')):
            self.adapter.recv(stream)


class LocalFileStorageAdapter(SnapshotStorageAdapter, BaseModel):

    model_config = ConfigDict(frozen=True)

    directory: Path

    @field_validator('directory')
    @classmethod
    def validate_path(cls, directory: Path):
        if not directory.is_absolute():
            raise ValueError(f"directory must be absolute, instead received path = {directory}")
        return directory.resolve()

    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        # query all the .zfs files at the root and below
        logger.debug(f"Querying snapshot files in {self.directory} for {dataset_id}")
        glob_pattern = "*.zfs" if dataset_id is None else f"**/{dataset_id}/*.zfs"
        matches = [fp for fp in self.directory.rglob(glob_pattern)]

        def filt(results):
            matches = []
            for r in results:
                try:
                    matches.append(ZfsFilePath(str(r)))
                except ValueError:
                    pass
            return matches

        matches = [r.relative_to(self.directory) for r in matches]
        matches = filt(matches)
        return matches

    def destroy(self, filepath: ZfsFilePath):
        filepath = self.directory.joinpath(filepath)
        logger.info(f"Destroying file: {filepath}")
        try:
            filepath.unlink()
        except FileNotFoundError as e:
            logger.warning(f"File not found during destroy: {filepath}")

    def recv(self, stream: SnapshotStream):
        path = self.directory.joinpath(stream.node.filepath)
        logger.info(f"Receiving snapshot stream to {path}")
        # create the parent dirs if they don't exist
        if not path.parent.exists():
            path.parent.mkdir(parents=True)
        # open the file and write the stream in
        with open(path, 'wb') as handle:
            for chunk in stream.bytes_stream:
                handle.write(chunk)
        return path

    def send(self, filepath: Path, blocksize: int = 4096):
        filepath = self.directory.joinpath(filepath)
        logger.info(f"Sending snapshot file {filepath}")

        def _snapshot_stream():
            with open(filepath, 'rb', buffering=0) as handle:
                while True:
                    chunk = handle.read(blocksize)
                    if not chunk:
                        break
                    yield chunk

        return _snapshot_stream()
