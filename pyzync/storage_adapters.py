"""Storage adapter implementations for handling snapshot data persistence.

This module provides concrete implementations of the SnapshotStorageAdapter interface
for different storage backends. Currently supports local filesystem storage.
"""

import os
import re
import dropbox
import logging
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
        """Receives a snapshot stream and adds it to the graph and storage backend.

        Args:
            stream (SnapshotStream): The snapshot stream to receive.
            graph (SnapshotGraph): The snapshot graph to update.
            dryrun (bool, optional): If True, only updates graph. Defaults to False.
            duplicate_policy (DuplicateDetectedPolicy, optional): How to handle duplicates.
                Defaults to 'error'.

        Raises:
            ValueError: If duplicate node is detected and policy is 'error'.

        Notes:
            - Updates both graph and storage backend
            - Checks for duplicate nodes before receiving
            - Supports different duplicate handling policies
            - Log messages track operation progress
        """
        logger.info(f'Receiving stream for node = {stream.node}')
        is_duplicate = stream.node in graph.get_nodes()
        if not is_duplicate:
            graph.add(stream.node)
        elif duplicate_policy == 'error':
            raise ValueError(
                f'Cannot recv stream = {stream} duplicate node = {stream.node} for adapter = {self.adapter}'
            )
        self.adapter.recv(stream)

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
        logger.info(f'Subscribing to stream for node = {stream.node}')
        is_duplicate = stream.node in graph.get_nodes()
        if not is_duplicate:
            graph.add(stream.node)
        elif duplicate_policy == 'error':
            raise ValueError(
                f'Cannot subscribe to stream = {stream} duplicate node = {stream.node} for adapter = {self.adapter}'
            )
        self.adapter.subscribe(stream)


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

        def filt(results):
            matches = set()
            for r in results:
                try:
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
            try:
                fp.unlink()
            except FileNotFoundError as e:
                logger.warning(f"File not found during destroy: {fp}")

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
                        buffer: bytes = b'') -> Iterable[bytes] | None:
            """Writes a byte stream to a file, handling file size limits.

            Args:
                path (PurePath): The path where the file will be written
                bytes_stream (Iterable[bytes]): Iterator yielding bytes to write
                buffer (bytes, optional): Initial buffer of bytes to write. Defaults to b''.

            Returns:
                Iterable[bytes] | None: Remaining bytes if max_file_size was reached, None if all bytes were written

            Notes:
                - Creates new files when max_file_size is reached
                - Uses context manager for safe file handling
            """
            file_size = len(buffer)
            with open(path, 'wb') as handle:
                # write out the data from the last iteration if passed in
                if buffer:
                    handle.write(buffer)
                # write out until out of data or at max file size
                for chunk in bytes_stream:
                    chunk_size = len(chunk)
                    if (self.max_file_size is not None) and ((file_size + chunk_size)
                                                             > self.max_file_size):
                        split_index = self.max_file_size - file_size
                        left, right = chunk[0:split_index], chunk[split_index:]
                        handle.write(left)
                        return right
                    file_size += len(chunk)
                    handle.write(chunk)

        # use the write_bytes method to write out data to each file
        file_index = 0
        remaining_bytes = write_bytes(path, stream.bytes_stream)
        while remaining_bytes is not None:
            file_index += 1
            next_path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
            remaining_bytes = write_bytes(next_path, stream.bytes_stream, remaining_bytes)

    def subscribe(self, stream: SnapshotStream):
        # build a function that can handle a single chunck
        path = self.directory.joinpath(stream.node.filepath)
        logger.debug(f"Subscribing for stream = {stream} to path = {path}")
        # create the parent dirs if they don't exist
        if not path.parent.exists():
            logger.debug(f"Creating parent directories {path}")
            path.parent.mkdir(parents=True)

        # define a helper function to open the file and write the stream in using context manager
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
                        handle.write(left)
                        handle.close()
                        file_index += 1
                        cur_path = path.with_name(f"{path.stem}_{file_index}{cur_path.suffix}")
                        handle = open(cur_path, 'wb')
                        file_size = len(right)
                        handle.write(right)
                    file_size += len(chunk)
                    handle.write(chunk)
            except IOError as e:
                logger.exception(f"Error writing to {cur_path}: {e}")
            finally:
                if handle:
                    handle.close()

        consumer = consume_bytes(path=path)
        next(consumer)
        stream.register(consumer)

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

        return _snapshot_stream()


class DropboxStorageAdapter(SnapshotStorageAdapter, BaseModel):
    """
    Storage adapter for handling snapshot files on Dropbox.
    """

    model_config = ConfigDict(frozen=True)

    directory: PurePath
    key: Optional[str] = None
    secret: Optional[str] = None
    refresh_token: Optional[str] = None
    access_token: Optional[str] = None
    max_file_size: int = 350 * (2**30)  # 350 GB

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
            """Lists all files in a Dropbox folder recursively.

            Args:
                folder (str): The Dropbox folder path to list

            Returns:
                list[dropbox.files.FileMetadata]: List of file metadata objects

            Notes:
                - Handles pagination for large directories
                - Returns only file entries, not folders
                - Returns empty list if folder doesn't exist
            """
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
        Delete a snapshot file and all chunked files from Dropbox.
        """
        dbx = self._client()
        base_path = self.directory.joinpath(node.filepath)
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix
        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")

        # List all files in the directory on Dropbox
        def list_files(folder):
            try:
                res = dbx.files_list_folder(folder)
            except dropbox.exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
            return [e for e in entries if isinstance(e, dropbox.files.FileMetadata)]

        files = list_files(str(directory))
        filepaths = [PurePath(f) for f in files if pattern.fullmatch(PurePath(f.name).name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        filepaths.insert(0, base_path)
        for f in filepaths:
            try:
                dbx.files_delete_v2(str(f))
            except dropbox.exceptions.ApiError as e:
                logger.warning(f"Dropbox file could not be destroyed: {f}")

    def recv(self, stream: SnapshotStream):
        """
        Receive a snapshot stream and upload it to Dropbox, splitting into multiple files if max_file_size is set.
        Uses Dropbox upload sessions to support streaming large files.
        """
        dbx = self._client()
        path = self.directory.joinpath(stream.node.filepath)
        logger.info(f"Receiving snapshot stream to Dropbox at {path}")

        # calls to dbx.files_upload_session_append_v2 must be a multiple of this many bytes
        # except for the last call with UploadSessionStartArg.close to True.
        WINDOW_LENGTH = 4 * (2**20)  # 4MB

        def write_bytes(path: PurePath,
                        bytes_stream: Iterable[bytes],
                        buffer: bytes = b'') -> Iterable[bytes] | None:
            """Writes a byte stream to Dropbox using upload sessions.

            Args:
                path (PurePath): The path where the file will be stored in Dropbox
                bytes_stream (Iterable[bytes]): Iterator yielding bytes to upload
                buffer (bytes, optional): Initial buffer of bytes to upload. Defaults to b''.

            Returns:
                Iterable[bytes] | None: Remaining bytes if max_file_size was reached, None if all bytes were uploaded

            Notes:
                - Uses Dropbox upload sessions for large file support
                - Splits files when they exceed max_file_size
                - Maintains WINDOW_LENGTH chunks for optimal upload performance
            """
            # start the file upload session
            file_size = len(buffer)
            result = dbx.files_upload_session_start(buffer)
            session_id = result.session_id
            buffer = b''  # clear the buffer after starting session
            # pipe chunks into the session using WINDOW_LENGTH for each push
            for chunk in bytes_stream:
                buffer += chunk
                if (self.max_file_size is not None) and ((file_size + len(buffer)) > self.max_file_size):
                    split_index = self.max_file_size - file_size
                    left, right = buffer[0:split_index], buffer[split_index:]
                    dbx.files_upload_session_finish(
                        left, dropbox.files.UploadSessionCursor(session_id, file_size),
                        dropbox.files.CommitInfo(str(path), mode=dropbox.files.WriteMode.overwrite))
                    return right
                else:
                    while len(buffer) >= WINDOW_LENGTH:
                        # grab a chunk of the buffer
                        data, buffer = buffer[:WINDOW_LENGTH], buffer[WINDOW_LENGTH:]
                        # perform the upload
                        dbx.files_upload_session_append_v2(
                            data, dropbox.files.UploadSessionCursor(session_id, file_size))
                        file_size += len(data)
            # if the iteration ended and the buffer still has data, end the session
            dbx.files_upload_session_finish(
                buffer, dropbox.files.UploadSessionCursor(session_id, file_size),
                dropbox.files.CommitInfo(str(path), mode=dropbox.files.WriteMode.overwrite))

        # use the write_bytes method to write out data to each file
        file_index = 0
        remaining_bytes = write_bytes(path, stream.bytes_stream)
        while remaining_bytes is not None:
            file_index += 1
            next_path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
            remaining_bytes = write_bytes(next_path, stream.bytes_stream, remaining_bytes)

    def subscribe(self, stream: SnapshotStream):
        """Subscribe to a snapshot stream and handle incoming chunks for Dropbox upload.
        
        Args:
            stream (SnapshotStream): The snapshot stream to subscribe to.
        """
        path = self.directory.joinpath(stream.node.filepath)
        logger.debug(f"Subscribing for stream = {stream} to path = {path}")

        WINDOW_LENGTH = 4 * (2**20)  # 4MB

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
            cur_path = path
            session_id = None
            buffer = b''
            try:
                while True:
                    # once the iterator is primed it starts here
                    chunk = yield
                    if chunk is None:
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
                            left, dropbox.files.UploadSessionCursor(session_id, file_size),
                            dropbox.files.CommitInfo(str(path), mode=dropbox.files.WriteMode.overwrite))
                        buffer = right
                        file_index += 1
                        cur_path = path.with_name(f"{path.stem}_{file_index}{cur_path.suffix}")
                        result = dbx.files_upload_session_start(b'')
                        session_id = result.session_id
                        file_size = 0
                    else:
                        while len(buffer) >= WINDOW_LENGTH:
                            # grab a chunk of the buffer
                            data, buffer = buffer[:WINDOW_LENGTH], buffer[WINDOW_LENGTH:]
                            # perform the upload
                            dbx.files_upload_session_append_v2(
                                data, dropbox.files.UploadSessionCursor(session_id, file_size))
                            file_size += len(data)
                if buffer and session_id:
                    dbx.files_upload_session_finish(
                        buffer, dropbox.files.UploadSessionCursor(session_id, file_size),
                        dropbox.files.CommitInfo(str(path), mode=dropbox.files.WriteMode.overwrite))
            except:
                logger.exception('Failed to consume bytes for dropbox remote')

        consumer = consume_bytes(path=path)
        next(consumer)  # Prime the generator
        stream.register(consumer)

    def send(self, node: SnapshotNode, blocksize: int = 2**20):
        """
        Download a snapshot file and all chunked files from Dropbox as a stream.
        """
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
            except dropbox.exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
            return [e for e in entries if isinstance(e, dropbox.files.FileMetadata)]

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

        return _snapshot_stream()
