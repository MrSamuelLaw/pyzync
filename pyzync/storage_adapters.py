"""Storage adapter implementations for handling snapshot data persistence.

This module provides concrete implementations of the SnapshotStorageAdapter interface
for different storage backends. Currently supports local filesystem storage.
"""

import os
import re
import time
import logging
from abc import ABC, abstractmethod
from pathlib import Path, PurePath
from typing import Optional, Iterable, cast, Sequence
from itertools import groupby

import dropbox
from dropbox import exceptions as dbx_exceptions
from dropbox import files as dbx_files
from pydantic import BaseModel, ConfigDict, field_validator, SecretStr
from pyzync.errors import DataIntegrityError
from pyzync.interfaces import (SnapshotStream, DuplicateDetectedPolicy, ZfsDatasetId, ZfsFilePath,
                               SnapshotNode, SnapshotGraph)
from pyzync.otel import trace, with_tracer

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class SnapshotStorageAdapter(ABC):
    """
    Abstract base class for snapshot storage adapters.
    """

    @abstractmethod
    def query(self, dataset_id: Optional[ZfsDatasetId]) -> Sequence[ZfsFilePath]:
        """
        Query for snapshot graphs for a dataset.
        """
        pass

    @abstractmethod
    def destroy(self, node: SnapshotNode):
        """
        Destroy a snapshot node in storage.
        """
        pass

    @abstractmethod
    def send(self, node: SnapshotNode, blocksize: int = 4096) -> Iterable[bytes]:
        """
        Send a snapshot file as a stream.
        """
        pass

    @abstractmethod
    def subscribe(self, stream: SnapshotStream):
        """
        Registers a generator with the stream for iteration of each chunk
        """
        pass


class RemoteSnapshotManager(BaseModel):
    """
    Manages snapshot graphs using a storage adapter backend.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    adapter: SnapshotStorageAdapter

    def __str__(self):
        return f"RemoteSnapshotManager(adapter={str(self.adapter)})"

    @with_tracer(tracer)
    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        """
        Query all snapshots for a given dataset or all datasets from the adapter.

        Args:
            dataset_id (Optional[ZfsDatasetId], optional): The dataset to query. Defaults to None.

        Returns:
            list[SnapshotGraph]: List of snapshot graphs for each dataset.
        """
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
        """Destroys a snapshot node from the graph and storage backend.

        Args:
            node (SnapshotNode): The snapshot node to destroy.
            graph (SnapshotGraph): The snapshot graph to update.
            dryrun (bool, optional): If True, do not perform actual operations. Defaults to False.
            prune (bool, optional): If True, prune orphaned nodes. Defaults to False.
            force (bool, optional): If True, force deletion. Defaults to False.

        Raises:
            DataIntegrityError: If attempting to delete a non-terminal node in a single chain
                without force=True.

        Notes:
            - Updates both the in-memory graph and storage backend
            - Can optionally prune orphaned nodes after deletion
            - Prevents accidental chain breakage unless forced
        """
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
    def send(self,
             node: SnapshotNode,
             graph: SnapshotGraph,
             blocksize: int = 4096,
             dryrun: bool = False):
        """Sends a snapshot node as a stream using the adapter.

        Args:
            node (SnapshotNode): The snapshot node to send.
            graph (SnapshotGraph): The snapshot graph containing the node.
            blocksize (int, optional): Block size for streaming. Defaults to 4096.
            dryrun (bool, optional): If True, returns dummy stream. Defaults to False.

        Returns:
            SnapshotStream: A stream object containing the snapshot data.

        Raises:
            ValueError: If the node is not part of the provided graph.

        Notes:
            - Verifies node exists in graph before sending
            - Returns dummy stream in dryrun mode
            - Uses adapter's send method for actual data transfer
        """
        logger.info(f"[RemoteStorageAdapter] Send called for adapter={self.adapter} with node={node}")
        if node not in graph.get_nodes():
            raise ValueError(f'Node = {node} not part of graph = {graph}')
        if dryrun:
            stream = SnapshotStream(node=node, bytes_stream=(b'bytes_stream',))
        else:
            stream = SnapshotStream(node=node, bytes_stream=self.adapter.send(node, blocksize=blocksize))
        logger.info(
            f"[RemoteStorageAdapter] Stream successfully built for adapter={self.adapter} with node={node}"
        )
        return stream

    @with_tracer(tracer)
    def subscribe(self,
                  stream: SnapshotStream,
                  graph: SnapshotGraph,
                  dryrun: bool = False,
                  duplicate_policy: DuplicateDetectedPolicy = 'error'):
        """Subscribes to a snapshot stream for receiving data incrementally.

        Similar to recv() but designed for incremental data reception through
        a publish/subscribe pattern.

        Args:
            stream (SnapshotStream): The snapshot stream to subscribe to.
            graph (SnapshotGraph): The snapshot graph to update.
            dryrun (bool, optional): If True, only updates graph. Defaults to False.
            duplicate_policy (DuplicateDetectedPolicy, optional): How to handle duplicates.
                Defaults to 'error'.

        Raises:
            ValueError: If duplicate node is detected and policy is 'error'.

        Notes:
            - Updates graph immediately but receives data incrementally
            - Checks for duplicate nodes before subscribing
            - Uses adapter's subscribe method for data handling
            - Suitable for real-time or streaming data reception
        """
        logger.info(
            f"[RemoteStorageAdapter] subscribing to stream for adapter={self.adapter} and node={stream.node}"
        )
        is_duplicate = stream.node in graph.get_nodes()
        if not is_duplicate:
            graph.add(stream.node)
        elif duplicate_policy == 'error':
            raise ValueError(
                f'Cannot subscribe to stream = {stream} duplicate node = {stream.node} for adapter = {self.adapter}'
            )
        if not dryrun:
            self.adapter.subscribe(stream)
        logger.info(
            f"[RemoteStorageAdapter] Successfully subscribed to stream for adapter={self.adapter} and node={stream.node}"
        )


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

    def __str__(self):
        return f"LocalFileStorageAdapter(directory={self.directory}, max_file_size={self.max_file_size})"

    @with_tracer(tracer)
    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        """
        Query all .zfs files in the directory for a given dataset or all datasets.

        Args:
            dataset_id (Optional[ZfsDatasetId], optional): The dataset to query. Defaults to None.

        Returns:
            list[ZfsFilePath]: List of matching ZFS file paths.
        """
        logger.debug(f"[LocalFileStorageAdapter] query called with dataset_id={dataset_id}")
        # query all the .zfs files at the root and below
        logger.debug(f"Querying snapshot files in {self.directory} for {dataset_id}")
        glob_pattern = "*.zfs" if dataset_id is None else f"**/{dataset_id}/*.zfs"
        matches = {fp for fp in self.directory.rglob(glob_pattern)}
        matches = {r.relative_to(self.directory) for r in matches}

        def filt(results):
            matches: set[ZfsFilePath] = set()
            for r in results:
                try:
                    matches.add(ZfsFilePath(str(r)))
                except ValueError:
                    pass
            return matches

        matches = list(filt(matches))
        return matches

    @with_tracer(tracer)
    def destroy(self, node: SnapshotNode):
        """
        Destroy a snapshot file at the given file path.

        Args:
            node (SnapshotNode): The node to destroy.
        """
        logger.info(f"[LocalFileStorageAdapter] destroy called for node={node}")
        filepath = node.filepath
        base_path = self.directory.joinpath(filepath)
        base_path = base_path.resolve()
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix
        logger.info(f"Destroying snapshot file(s) for {base_path}")

        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")
        filepaths = [f for f in directory.glob(f"{stem}_*{suffix}")]
        filepaths = [f for f in filepaths if pattern.fullmatch(f.name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        filepaths.insert(0, base_path)
        for fp in filepaths:
            fp.unlink()
        logger.info(f"[LocalFileStorageAdapter] destroy called for node={node}")

    @with_tracer(tracer)
    def subscribe(self, stream: SnapshotStream):
        logger.debug(f"[LocalFileStorageAdapter] subscribe called for stream.node={stream.node}")
        # build a function that can handle a single chunck
        path = self.directory.joinpath(stream.node.filepath)
        # create the parent dirs if they don't exist
        if not path.parent.exists():
            logger.debug(f"Creating parent directories {path}")
            path.parent.mkdir(parents=True)

        # define a helper function to open the file and write the stream in using context manager
        @with_tracer(tracer)
        def consume_bytes(path: PurePath):
            """Consumes bytes from a stream and writes them to files, splitting across multiple files if needed.

            Args:
                path (PurePath): The base path where the file(s) will be written.

            Yields:
                None: Generator function that yields None but accepts bytes through send().

            Notes:
                - Creates new files with incremental suffixes when max_file_size is reached
                - Handles file creation, writing, and cleanup in case of errors
            """
            file_size = 0
            file_index = 0
            cur_path = path
            handle = None

            def _logged_write(handle, bytes):
                start = time.time
                nbytes = len(bytes)
                handle.write(bytes)
                end = time.time
                logger.debug(
                    f"[LocalFileStorageAdapter] wrote {nbytes} bytes to {cur_path} using adapter {self}."
                )

            try:
                while True:
                    chunk = yield
                    if chunk is None:
                        break
                    elif handle is None:
                        handle = open(cur_path, 'wb')

                    # write out until out of data or at max file size, then start a new file
                    chunk_size = len(chunk)
                    if (self.max_file_size is not None) and ((file_size + chunk_size)
                                                             > self.max_file_size):
                        split_index = self.max_file_size - file_size
                        left, right = chunk[0:split_index], chunk[split_index:]
                        _logged_write(handle, left)
                        handle.close()
                        file_index += 1
                        cur_path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
                        handle = open(cur_path, 'wb')
                        file_size = len(right)
                        _logged_write(handle, right)
                    file_size += len(chunk)
                    _logged_write(handle, chunk)
            except IOError as e:
                logger.exception(f"Error writing to {cur_path}: {e}")
                raise
            finally:
                if handle:
                    handle.close()

        consumer = consume_bytes(path=path)
        next(consumer)
        stream.register(consumer)

    @with_tracer(tracer)
    def send(self, node: SnapshotNode, blocksize: int = 2**20):  # default blocksize = 1MB
        """
        Send a snapshot file as a stream from the local filesystem, including chunked files in order.

        Args:
            node (SnapshotNode): The node to send.
            blocksize (int, optional): Block size for streaming. Defaults to 4096.

        Returns:
            generator: A generator yielding file chunks as bytes.

        Raises:
            FileNotFoundError: If the file does not exist.
            OSError: If there is an OS error during file operations.
        """
        logger.debug(f"[LocalFileStorageAdapter] send called for node={node}, blocksize={blocksize}")
        filepath = node.filepath
        base_path = self.directory.joinpath(filepath)
        base_path = base_path.resolve()
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix
        logger.info(f"Sending snapshot file(s) for {base_path}")

        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")
        filepaths = [f for f in directory.glob(f"{stem}_*{suffix}")]
        filepaths = [f for f in filepaths if pattern.fullmatch(f.name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        filepaths.insert(0, base_path)

        @with_tracer(tracer)
        def _snapshot_stream():
            """Generates a stream of bytes from a series of snapshot files.

            Yields:
                bytes: Chunks of data read from the snapshot files in sequence

            Notes:
                - Reads files in order of their numerical suffixes
                - Uses buffered reading for memory efficiency
                - Closes files automatically using context manager
            """
            for fp in filepaths:
                with open(fp, 'rb', buffering=0) as handle:
                    logger.debug(f"Streaming node = {node} from filepath = {fp}")
                    while True:
                        chunk = handle.read(blocksize)
                        if not chunk:
                            break
                        yield chunk
            logger.info(f"Successfully sent snapshot file(s) for {base_path}")

        return _snapshot_stream()


# Configure Dropbox SDK logging to only show warnings when level = INFO as info is closer to debug.
LOG_LEVEL = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO'))
if LOG_LEVEL == logging.INFO:
    dropbox_logger = logging.getLogger('dropbox')
    dropbox_logger.setLevel(logging.WARNING)


class DropboxStorageAdapter(SnapshotStorageAdapter, BaseModel):
    """
    Storage adapter for handling snapshot files on Dropbox.
    """

    model_config = ConfigDict(frozen=True)

    directory: PurePath
    key: Optional[SecretStr] = None
    secret: Optional[SecretStr] = None
    refresh_token: Optional[SecretStr] = None
    access_token: Optional[SecretStr] = None
    max_file_size: int = 350 * (2**30)  # 350 GB

    def __str__(self):
        return f"DropboxStorageAdapter(directory={self.directory}, max_file_size={self.max_file_size})"

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
        """Creates and returns an authenticated Dropbox client.

        Returns:
            dropbox.Dropbox: An authenticated Dropbox client instance

        Raises:
            RuntimeError: If required Dropbox credentials are not found in environment variables

        Notes:
            - Prioritizes OAuth2 refresh token authentication
            - Falls back to access token if refresh token not available
            - Requires either DROPBOX_REFRESH_TOKEN + DROPBOX_KEY + DROPBOX_SECRET
              or DROPBOX_TOKEN environment variables
        """
        # Prefer refresh token if available, else fall back to access token
        refresh_token = os.environ.get("DROPBOX_REFRESH_TOKEN")
        app_key = os.environ.get("DROPBOX_KEY")
        app_secret = os.environ.get("DROPBOX_SECRET")
        access_token = os.environ.get("DROPBOX_TOKEN")
        if refresh_token and app_key and app_secret:
            return dropbox.Dropbox(oauth2_refresh_token=refresh_token,
                                   app_key=app_key,
                                   app_secret=app_secret)
        elif access_token:
            return dropbox.Dropbox(access_token)
        else:
            raise RuntimeError(
                "Dropbox credentials not found. Set DROPBOX_REFRESH_TOKEN, DROPBOX_KEY, DROPBOX_SECRET or DROPBOX_TOKEN in your environment."
            )

    @with_tracer(tracer)
    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        """
        Query all .zfs files in Dropbox for a given dataset or all datasets.
        """
        logger.debug(f"[DropboxStorageAdapter] query called with dataset_id={dataset_id}")
        dbx = self._client()
        # Build the path to list
        if dataset_id is None:
            path = self.directory
        else:
            path = self.directory.joinpath(dataset_id)
        # Recursively list all .zfs files
        def list_files(folder: str):
            """Lists all files in a Dropbox folder recursively.

            Args:
                folder (str): The Dropbox folder path to list

            Returns:
                list[dbx_files.FileMetadata]: List of file metadata objects

            Notes:
                - Handles pagination for large directories
                - Returns only file entries, not folders
                - Returns empty list if folder doesn't exist
            """
            try:
                res = dbx.files_list_folder(folder, recursive=True)
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
            return [e for e in entries if isinstance(e, dbx_files.FileMetadata)]

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

    @with_tracer(tracer)
    def destroy(self, node: SnapshotNode):
        """
        Delete a snapshot file and all chunked files from Dropbox.
        """
        logger.debug(f"[DropboxStorageAdapter] destroy called for node={node}")
        dbx = self._client()
        base_path = self.directory.joinpath(node.filepath)
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix

        # List all files in the directory on Dropbox
        def list_files(folder):
            try:
                res = dbx.files_list_folder(str(folder))
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = cast(dbx_files.ListFolderResult, dbx.files_list_folder_continue(res.cursor))
                entries.extend(res.entries)
            return [PurePath(e.path_display) for e in entries if isinstance(e, dbx_files.FileMetadata)]

        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")
        filepaths = list_files(directory)
        filepaths = [f for f in filepaths if pattern.fullmatch(f.name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        filepaths.insert(0, base_path)
        for f in filepaths:
            try:
                dbx.files_delete_v2(str(f))
            except dbx_exceptions.ApiError as e:
                logger.warning(f"Dropbox file could not be destroyed: {f}")
        logger.info(f"Successfully destroyed all files for {base_path}")

    @with_tracer(tracer)
    def subscribe(self, stream: SnapshotStream):
        """Subscribe to a snapshot stream and handle incoming chunks for Dropbox upload.
        
        Args:
            stream (SnapshotStream): The snapshot stream to subscribe to.
        """
        path = self.directory.joinpath(stream.node.filepath)
        logger.debug(f"[DropboxStorageAdapter] subscribe called for stream.node={stream.node}")

        MODULO = 4 * (2**20)  # 4MB
        MAX_SIZE = 144 * (2**20)  # 144 MB

        @with_tracer(tracer)
        def consume_bytes(path: PurePath):
            """Consumes bytes from a stream and uploads them to Dropbox, splitting into chunks if needed.

            Args:
                path (PurePath): The base path where the file will be stored in Dropbox

            Yields:
                None: Generator function that yields None but accepts bytes through send()

            Notes:
                - Creates new upload sessions when max_file_size is reached
                - Buffers data to match Dropbox's WINDOW_LENGTH requirement
                - Handles upload session management and cleanup
            """
            file_index = 0
            file_size = 0
            cur_path = path
            session_id = None
            buffer = b''
            dbx = None
            try:
                while True:
                    # once the iterator is primed it starts here
                    chunk = yield
                    if chunk is None:
                        logger.info(f'[DropboxStorageAdapter] stopping consumer for {self}')
                        break
                    if session_id is None:
                        dbx = self._client()
                        result = dbx.files_upload_session_start(b'')
                        session_id = result.session_id
                        file_size = 0

                    buffer += chunk
                    if (self.max_file_size is not None) and ((file_size + len(buffer))
                                                             > self.max_file_size):
                        split_index = self.max_file_size - file_size
                        left, right = buffer[0:split_index], buffer[split_index:]
                        dbx.files_upload_session_finish(
                            left, dbx_files.UploadSessionCursor(session_id, file_size),
                            dbx_files.CommitInfo(str(cur_path), mode=dbx_files.WriteMode.overwrite))
                        buffer = right
                        file_index += 1
                        cur_path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
                        result = dbx.files_upload_session_start(b'')
                        session_id = result.session_id
                        file_size = 0
                    else:
                        while len(buffer) >= MODULO:
                            # Calculate largest slice that's divisible by MODULO and less than MAX_SIZE
                            slice_size = min(len(buffer) - (len(buffer) % MODULO), MAX_SIZE)
                            data, buffer = buffer[:slice_size], buffer[slice_size:]
                            # perform the upload
                            dbx.files_upload_session_append_v2(
                                data, dbx_files.UploadSessionCursor(session_id, file_size))
                            file_size += len(data)
                if buffer and session_id and dbx:
                    while len(buffer) >= MODULO:
                        # Calculate largest slice that's divisible by MODULO and less than MAX_SIZE
                        slice_size = min(len(buffer) - (len(buffer) % MODULO), MAX_SIZE)
                        data, buffer = buffer[:slice_size], buffer[slice_size:]
                        # perform the upload
                        dbx.files_upload_session_append_v2(
                            data, dbx_files.UploadSessionCursor(session_id, file_size))
                        file_size += len(data)
                    dbx.files_upload_session_finish(
                        buffer, dbx_files.UploadSessionCursor(session_id, file_size),
                        dbx_files.CommitInfo(str(cur_path), mode=dbx_files.WriteMode.overwrite))
            except:
                logger.exception('Failed to consume bytes for dropbox remote')
                raise

        consumer = consume_bytes(path=path)
        next(consumer)  # Prime the generator
        stream.register(consumer)

    @with_tracer(tracer)
    def send(self, node: SnapshotNode, blocksize: int = 2**20):
        """
        Download a snapshot file and all chunked files from Dropbox as a stream.
        """
        logger.debug(f"[DropboxStorageAdapter] send called for node={node}, blocksize={blocksize}")
        dbx = self._client()
        base_path = self.directory.joinpath(node.filepath)
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix
        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")

        def list_files(folder):
            try:
                res = dbx.files_list_folder(folder)
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
            return [e for e in entries if isinstance(e, dbx_files.FileMetadata)]

        files = list_files(str(directory))
        files = list_files(str(directory))
        filepaths = [f for f in files if pattern.fullmatch(PurePath(f.name).name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        # Insert the base file at the start
        filepaths.insert(0, base_path)

        def _snapshot_stream():
            """Generates a stream of bytes from a series of snapshot files stored in Dropbox.

            Yields:
                bytes: Chunks of data downloaded from the snapshot files in sequence

            Notes:
                - Downloads files in order of their numerical suffixes
                - Handles both FileMetadata and PurePath inputs
                - Uses buffered reading for memory efficiency
            """
            for f in filepaths:
                # f may be a Dropbox FileMetadata or a PurePath, handle both
                if hasattr(f, 'path_display'):
                    path_str = f.path_display
                else:
                    path_str = str(f)
                metadata, res = dbx.files_download(path_str)
                from io import BytesIO
                buf = BytesIO(res.content)
                while True:
                    chunk = buf.read(blocksize)
                    if not chunk:
                        break
                    yield chunk
            logger.info(f"Successfully sent snapshot file(s) for {base_path}")

        return _snapshot_stream()
