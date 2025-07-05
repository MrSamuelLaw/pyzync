"""Storage adapter implementations for handling snapshot data persistence.

This module provides concrete implementations of the SnapshotStorageAdapter interface
for different storage backends. Currently supports local filesystem storage.
"""

import re
import dropbox
import logging
from io import BytesIO
from pathlib import Path, PurePath
from typing import Optional, Iterable
from itertools import groupby

from pydantic import BaseModel, ConfigDict, field_validator

from pyzync.errors import DataIntegrityError
from pyzync.interfaces import (SnapshotStorageAdapter, SnapshotStream, DuplicateDetectedPolicy,
                               ZfsDatasetId, ZfsFilePath, SnapshotNode, SnapshotGraph)

logger = logging.getLogger(__name__)


class RemoteSnapshotManager(BaseModel):
    """
    Manages snapshot graphs using a storage adapter backend.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    adapter: SnapshotStorageAdapter

    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        """
        Query all snapshots for a given dataset or all datasets from the adapter.

        Args:
            dataset_id (Optional[ZfsDatasetId], optional): The dataset to query. Defaults to None.

        Returns:
            list[SnapshotGraph]: List of snapshot graphs for each dataset.
        """
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
        """
        Destroy a snapshot node from the graph and storage backend.

        Args:
            node (SnapshotNode): The snapshot node to destroy.
            graph (SnapshotGraph): The snapshot graph to update.
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.
            prune (bool, optional): If True, prune orphaned nodes. Defaults to False.
            force (bool, optional): If True, force deletion. Defaults to False.
        """
        logger.info(f'Destroying node = {node}')
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
                logger.info(f'Destroying node = {node}')
                graph.remove(node)
                if not dryrun:
                    self.adapter.destroy(node)

    def send(self,
             node: SnapshotNode,
             graph: SnapshotGraph,
             blocksize: int = 4096,
             dryrun: bool = False):
        """
        Send a snapshot node as a stream using the adapter.

        Args:
            node (SnapshotNode): The snapshot node to send.
            graph (SnapshotGraph): The snapshot graph.
            blocksize (int, optional): Block size for streaming. Defaults to 4096.
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.

        Returns:
            SnapshotStream: The snapshot stream object.
        """
        if node not in graph.get_nodes():
            raise ValueError(f'Node = {node} not part of graph = {graph}')
        if dryrun:
            stream = SnapshotStream(node=node, bytes_stream=(b'bytes_stream',))
        else:
            stream = SnapshotStream(node=node, bytes_stream=self.adapter.send(node, blocksize=blocksize))
        return stream

    def recv(self,
             stream: SnapshotStream,
             graph: SnapshotGraph,
             dryrun: bool = False,
             duplicate_policy: DuplicateDetectedPolicy = 'error'):
        """
        Receive a snapshot stream and add it to the graph and storage backend.

        Args:
            stream (SnapshotStream): The snapshot stream to receive.
            graph (SnapshotGraph): The snapshot graph to update.
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.
            duplicate_policy (DuplicateDetectedPolicy, optional): Policy for handling duplicates. Defaults to 'error'.
        """
        logger.info(f'Receiveing stream for node = {stream.node}')
        is_duplicate = stream.node in graph.get_nodes()
        if (not is_duplicate) or (is_duplicate and duplicate_policy == 'error'):
            graph.add(stream.node)
        if (not dryrun) and ((not is_duplicate) or (is_duplicate and duplicate_policy == 'overwrite')):
            self.adapter.recv(stream)


class LocalFileStorageAdapter(SnapshotStorageAdapter, BaseModel):
    """
    Storage adapter for handling snapshot files on the local filesystem.
    """

    model_config = ConfigDict(frozen=True)

    directory: Path
    max_file_size: Optional[int] = None

    @field_validator('directory')
    @classmethod
    def validate_path(cls, directory: Path):
        """
        Validate that the directory is an absolute path.

        Args:
            directory (Path): The directory path to validate.

        Returns:
            Path: The resolved absolute path.
        """
        if not directory.is_absolute():
            raise ValueError(f"directory must be absolute, instead received path = {directory}")
        return directory.resolve()

    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        """
        Query all .zfs files in the directory for a given dataset or all datasets.

        Args:
            dataset_id (Optional[ZfsDatasetId], optional): The dataset to query. Defaults to None.

        Returns:
            list[ZfsFilePath]: List of matching ZFS file paths.
        """
        # query all the .zfs files at the root and below
        logger.debug(f"Querying snapshot files in {self.directory} for {dataset_id}")
        glob_pattern = "*.zfs" if dataset_id is None else f"**/{dataset_id}/*.zfs"
        matches = {fp for fp in self.directory.rglob(glob_pattern)}
        matches = {r.relative_to(self.directory) for r in matches}
        pattern = re.compile('_d+.zfs')

        def filt(results):
            matches = set()
            # pattern = ZfsDatasetId._pattern
            for r in results:
                try:
                    # if the stream was chunked, strip the _1 before the.zfs
                    matches.add(ZfsFilePath(str(r)))
                except ValueError:
                    pass
            return matches

        matches = filt(matches)
        return matches

    def destroy(self, node: SnapshotNode):
        """
        Destroy a snapshot file at the given file path.

        Args:
            node (SnapshotNode): The node to destroy.
        """
        filepath = node.filepath
        filepath = self.directory.joinpath(filepath)
        logger.info(f"Destroying file: {filepath}")
        try:
            filepath.unlink()
        except FileNotFoundError as e:
            logger.warning(f"File not found during destroy: {filepath}")

    def recv(self, stream: SnapshotStream):
        """
        Receive a snapshot stream and write it to the local filesystem, splitting into multiple files if max_file_size is set.

        Args:
            stream (SnapshotStream): The snapshot stream to receive.

        Raises:
            FileNotFoundError: If the parent directory cannot be created or file cannot be written.
            OSError: If there is an OS error during file operations.
        """
        path = self.directory.joinpath(stream.node.filepath)
        logger.info(f"Receiving snapshot stream to {path}")
        # create the parent dirs if they don't exist
        if not path.parent.exists():
            logger.debug(f"Creating parent directories {path}")
            path.parent.mkdir(parents=True)

        # define a helper function to open the file and write the stream in using context manager
        def write_bytes(path: PurePath,
                        bytes_stream: Iterable[bytes],
                        buffer: bytes = b'') -> Iterable[bytes]:
            file_size = 0
            with open(path, 'wb') as handle:
                for chunk in bytes_stream:
                    buffer += chunk
                    chunk_size = len(buffer)
                    if (self.max_file_size is not None) and ((file_size + chunk_size)
                                                             > self.max_file_size):
                        split_index = self.max_file_size - file_size
                        left, right = buffer[0:split_index], buffer[split_index:]
                        handle.write(left)
                        return right
                    file_size += len(buffer)
                    handle.write(buffer)

        file_index = 0
        remaining_bytes = write_bytes(path, stream.bytes_stream)
        while remaining_bytes is not None:
            file_index += 1
            path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
            remaining_bytes = write_bytes(path, stream.bytes_stream, remaining_bytes)

    def send(self, node: SnapshotNode, blocksize: int = 4096):
        """
        Send a snapshot file as a stream from the local filesystem.

        Args:
            node (SnapshotNode): The node to send.
            blocksize (int, optional): Block size for streaming. Defaults to 4096.

        Returns:
            generator: A generator yielding file chunks as bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
            OSError: If there is an OS error during file operations.
        """
        filepath = node.filepath
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


class DropboxStorageAdapter(SnapshotStorageAdapter, BaseModel):
    """
    Storage adapter for handling snapshot files on Dropbox.
    """

    model_config = ConfigDict(frozen=True)

    access_token: str
    directory: PurePath

    @field_validator('directory')
    @classmethod
    def validate_path(cls, directory: PurePath):
        """
        Validate that the directory is an absolute path.

        Args:
            directory (Path): The directory path to validate.

        Returns:
            Path: The resolved absolute path.
        """
        if not directory.is_absolute():
            raise ValueError(f"directory must be absolute, instead received path = {directory}")
        return directory

    def _client(self):
        return dropbox.Dropbox(self.access_token)

    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        """
        Query all .zfs files in Dropbox for a given dataset or all datasets.
        """
        dbx = self._client()
        # Build the path to list
        if dataset_id is None:
            path = self.directory
        else:
            path = self.directory.joinpath(dataset_id)
        # Recursively list all .zfs files
        def list_files(folder):
            try:
                res = dbx.files_list_folder(folder, recursive=True)
            except dropbox.exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
            return [e for e in entries if isinstance(e, dropbox.files.FileMetadata)]

        files = list_files(str(path))
        # Filter for .zfs files and convert to ZfsFilePath relative to root_path
        matches = []
        for f in files:
            if f.name.endswith('.zfs'):
                path = PurePath(f.path_display)
                path = path.relative_to(self.directory)
                try:
                    matches.append(ZfsFilePath(str(path)))
                except ValueError:
                    pass
        return matches

    def destroy(self, node: SnapshotNode):
        """
        Delete a snapshot file from Dropbox.
        """
        dbx = self._client()
        path = self.directory.joinpath(node.filepath)
        try:
            dbx.files_delete_v2(str(path))
        except dropbox.exceptions.ApiError as e:
            logger.warning(f"Dropbox file not found during destroy: {path}")

    def recv(self, stream: SnapshotStream):
        """
        Receive a snapshot stream and upload it to Dropbox.
        """
        dbx = self._client()
        path = str(self.directory.joinpath(stream.node.filepath))
        # Dropbox API requires bytes, so we need to join the stream
        buf = BytesIO()
        for chunk in stream.bytes_stream:
            buf.write(chunk)
        buf.seek(0)
        dbx.files_upload(buf.read(), path, mode=dropbox.files.WriteMode.overwrite)

    def send(self, node: SnapshotNode, blocksize: int = 4096):
        """
        Download a snapshot file from Dropbox as a stream.
        """
        dbx = self._client()
        path = str(self.directory.joinpath(node.filepath))
        # Download file content into memory (BytesIO)
        metadata, res = dbx.files_download(path)
        from io import BytesIO
        buf = BytesIO(res.content)

        def _snapshot_stream():
            while True:
                chunk = buf.read(blocksize)
                if not chunk:
                    break
                yield chunk

        return _snapshot_stream()
