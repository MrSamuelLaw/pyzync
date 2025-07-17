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
        logger.info(f'Receiving stream for node = {stream.node}')
        is_duplicate = stream.node in graph.get_nodes()
        if (not is_duplicate) or (is_duplicate and duplicate_policy == 'error'):
            graph.add(stream.node)
        if (not dryrun) and ((not is_duplicate) or (is_duplicate and duplicate_policy == 'overwrite')):
            self.adapter.recv(stream)

    def subscribe(self, stream: SnapshotStream, graph: SnapshotGraph, dryrun: bool = False):
        logger.info(f'Subscribing to stream for node = {stream.node}')
        is_duplicate = stream.node in graph.get_nodes()
        if is_duplicate:
            raise ValueError(f'Cannot subscribe to a stream for duplicate node = {stream.node}')
        graph.add(stream.node)
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
        logger.info(f"Sending snapshot file(s) for {base_path}")

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
            file_size = 0
            with open(path, 'wb') as handle:
                # write out the data from the last iteration if passed in
                if buffer:
                    handle.write(buffer)
                    file_size += len(buffer)
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
            file_size = 0
            file_index = 0
            cur_path = path
            try:
                handle = open(cur_path, 'wb')
                while True:
                    chunk = yield
                    if chunk is None:
                        break

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
    max_file_size: Optional[int] = None

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
        filepaths = [f for f in files if pattern.fullmatch(PurePath(f.name).name)]

        def get_stem(f):
            if hasattr(f, 'name'):
                return PurePath(f.name).stem
            return PurePath(f).stem

        filepaths = sorted(filepaths,
                           key=lambda f: int(get_stem(f).split('_')[-1]) if '_' in get_stem(f) else -1)
        filepaths.insert(0, base_path)
        for f in filepaths:
            try:
                dbx.files_delete_v2(str(f) if not hasattr(f, 'path_display') else f.path_display)
            except dropbox.exceptions.ApiError as e:
                logger.warning(f"Dropbox file not found during destroy: {f}")

    def recv(self, stream: SnapshotStream):
        """
        Receive a snapshot stream and upload it to Dropbox, splitting into multiple files if max_file_size is set.
        Uses Dropbox upload sessions to support streaming large files.
        """
        dbx = self._client()
        path = self.directory.joinpath(stream.node.filepath)
        logger.info(f"Receiving snapshot stream to Dropbox at {path}")

        def upload_stream_to_dropbox(path, bytes_stream):
            CHUNK_SIZE = 4 * 1024 * 1024  # 4MB, Dropbox minimum for session
            session_id = None
            offset = 0
            buffer = b''
            for chunk in bytes_stream:
                buffer += chunk
                while len(buffer) >= CHUNK_SIZE:
                    data, buffer = buffer[:CHUNK_SIZE], buffer[CHUNK_SIZE:]
                    if session_id is None:
                        result = dbx.files_upload_session_start(data)
                        session_id = result.session_id
                        offset += len(data)
                    else:
                        dbx.files_upload_session_append_v2(
                            data, dropbox.files.UploadSessionCursor(session_id, offset))
                        offset += len(data)
            # upload any remaining data and finish
            if session_id is None:
                dbx.files_upload(buffer, str(path), mode=dropbox.files.WriteMode.overwrite)
            else:
                dbx.files_upload_session_finish(
                    buffer, dropbox.files.UploadSessionCursor(session_id, offset),
                    dropbox.files.CommitInfo(str(path), mode=dropbox.files.WriteMode.overwrite))

        file_index = 0
        remaining_bytes = None

        def chunked_stream(bytes_stream, buffer):
            if buffer:
                yield buffer
            yield from bytes_stream

        # chunking logic for multi-file support
        while True:
            if file_index == 0:
                target_path = path
            else:
                target_path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
            file_size = 0

            def limited_stream():
                nonlocal file_size, remaining_bytes
                for chunk in chunked_stream(stream.bytes_stream if file_index == 0 else [],
                                            remaining_bytes):
                    if self.max_file_size is not None and file_size + len(chunk) > self.max_file_size:
                        split_index = self.max_file_size - file_size
                        left, right = chunk[:split_index], chunk[split_index:]
                        file_size += len(left)
                        remaining_bytes = right
                        yield left
                        return
                    file_size += len(chunk)
                    yield chunk
                remaining_bytes = None

            upload_stream_to_dropbox(target_path, limited_stream())
            if not remaining_bytes:
                break
            file_index += 1

    def subscribe(self, stream: SnapshotStream):
        """Subscribe to a snapshot stream and handle incoming chunks for Dropbox upload.
        
        Args:
            stream (SnapshotStream): The snapshot stream to subscribe to.
        """
        path = self.directory.joinpath(stream.node.filepath)
        logger.debug(f"Subscribing for stream = {stream} to path = {path}")

        def consume_bytes(path: PurePath):
            dbx = self._client()
            file_size = 0
            file_index = 0
            cur_path = path
            session_id = None
            offset = 0
            buffer = b''
            MIN_CHUNK_SIZE = 4 * 1024 * 1024  # 4MB Dropbox minimum for upload sessions

            try:
                while True:
                    chunk = yield
                    if chunk is None:
                        break

                    buffer += chunk
                    while len(buffer) >= MIN_CHUNK_SIZE:
                        # Calculate how much data we can write in this iteration
                        max_chunk = min(
                            len(buffer),  # Don't take more than we have
                            MIN_CHUNK_SIZE,  # Don't exceed Dropbox's minimum chunk size
                            float('inf') if self.max_file_size is None else  # No max file size limit
                            self.max_file_size - file_size  # Space remaining in current file
                        )

                        # If we hit the file size limit, start a new file
                        if max_chunk < MIN_CHUNK_SIZE and len(buffer) >= MIN_CHUNK_SIZE:
                            # Upload whatever we have in the current session
                            if session_id is None:
                                dbx.files_upload(buffer[:max_chunk],
                                                 str(cur_path),
                                                 mode=dropbox.files.WriteMode.overwrite)
                            else:
                                dbx.files_upload_session_finish(
                                    buffer[:max_chunk],
                                    dropbox.files.UploadSessionCursor(session_id, offset),
                                    dropbox.files.CommitInfo(str(cur_path),
                                                             mode=dropbox.files.WriteMode.overwrite))

                            # Start new file
                            file_index += 1
                            cur_path = path.with_name(f"{path.stem}_{file_index}{cur_path.suffix}")
                            session_id = None
                            offset = 0
                            file_size = 0
                            continue

                        # Split buffer and process the chunk
                        data, buffer = buffer[:max_chunk], buffer[max_chunk:]

                        # Normal upload progress
                        if session_id is None:
                            if len(buffer) >= MIN_CHUNK_SIZE or chunk is None:
                                # Start new session
                                result = dbx.files_upload_session_start(data)
                                session_id = result.session_id
                                offset = len(data)
                            else:
                                # Small file, direct upload
                                dbx.files_upload(data,
                                                 str(cur_path),
                                                 mode=dropbox.files.WriteMode.overwrite)
                        else:
                            # Continue existing session
                            dbx.files_upload_session_append_v2(
                                data, dropbox.files.UploadSessionCursor(session_id, offset))
                            offset += len(data)
                        file_size += len(data)

                # Upload any remaining data
                if buffer:
                    if session_id is None:
                        dbx.files_upload(buffer, str(cur_path), mode=dropbox.files.WriteMode.overwrite)
                    else:
                        dbx.files_upload_session_finish(
                            buffer, dropbox.files.UploadSessionCursor(session_id, offset),
                            dropbox.files.CommitInfo(str(cur_path),
                                                     mode=dropbox.files.WriteMode.overwrite))

            except dropbox.exceptions.ApiError as e:
                logger.exception(f"Dropbox API error while uploading to {cur_path}: {e}")
                raise
            except Exception as e:
                logger.exception(f"Error while uploading to {cur_path}: {e}")
                raise

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
