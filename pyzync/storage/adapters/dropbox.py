import os
import re
import logging
from io import BytesIO
from pathlib import PurePath
from typing import Optional, cast

from dropbox import dropbox_client
from dropbox import exceptions as dbx_exceptions
from dropbox import files as dbx_files
from requests.models import Response
from pydantic import BaseModel, field_validator, SecretStr, ConfigDict

from pyzync.storage.interfaces import SnapshotStorageAdapter
from pyzync.otel import trace, with_tracer
from pyzync.interfaces import ZfsDatasetId, ZfsFilePath, SnapshotNode

# Configure Dropbox SDK logging to only show warnings when level = INFO as info is closer to debug.
logger = logging.getLogger('dropbox')
LOG_LEVEL = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO'))
if LOG_LEVEL == logging.INFO:
    logger.setLevel(logging.WARNING)

tracer = trace.get_tracer('dropbox')


class DropboxStorageAdapter(SnapshotStorageAdapter, BaseModel):

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
            return dropbox_client.Dropbox(oauth2_refresh_token=refresh_token,
                                          app_key=app_key,
                                          app_secret=app_secret)
        elif access_token:
            return dropbox_client.Dropbox(access_token)
        else:
            raise RuntimeError(
                "Dropbox credentials not found. Set DROPBOX_REFRESH_TOKEN, DROPBOX_KEY, DROPBOX_SECRET or DROPBOX_TOKEN in your environment."
            )

    @with_tracer(tracer)
    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        logger.debug(f"[DropboxStorageAdapter] query called with dataset_id={dataset_id}")
        dbx = self._client()
        # Build the path to list
        if dataset_id is None:
            path = self.directory
        else:
            path = self.directory.joinpath(dataset_id)
        # Recursively list all .zfs files
        def list_files(folder: str):
            try:
                res = cast(dbx_files.ListFolderResult, dbx.files_list_folder(folder, recursive=True))
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = cast(dbx_files.ListFolderResult, dbx.files_list_folder_continue(res.cursor))
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
        logger.debug(f"[DropboxStorageAdapter] destroy called for node={node}")
        dbx = self._client()
        base_path = self.directory.joinpath(node.filepath)
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix

        # List all files in the directory on Dropbox
        def list_files(folder):
            try:
                res = cast(dbx_files.ListFolderResult, dbx.files_list_folder(str(folder)))
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
            except dbx_exceptions.ApiError:
                logger.warning(f"Dropbox file could not be destroyed: {f}")
        logger.info(f"Successfully destroyed all files for {base_path}")

    @with_tracer(tracer)
    def get_consumer(self, node: SnapshotNode):
        path = self.directory.joinpath(node.filepath)
        logger.debug(f"[DropboxStorageAdapter] subscribe called for stream.node={node}")

        MODULO = 4 * (2**20)  # 4MB
        MAX_SIZE = 144 * (2**20)  # 144 MB

        @with_tracer(tracer)
        def consume_bytes(path: PurePath):
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
                    if session_id is None or dbx is None:
                        dbx = self._client()
                        result = cast(dbx_files.UploadSessionStartResult,
                                      dbx.files_upload_session_start(b''))
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
                        result = cast(dbx_files.UploadSessionStartResult,
                                      dbx.files_upload_session_start(b''))
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
        return consumer

    @with_tracer(tracer)
    def get_producer(self, node: SnapshotNode, blocksize: int = 2**20):
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
                res = cast(dbx_files.ListFolderResult, dbx.files_list_folder(folder))
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = cast(dbx_files.ListFolderResult, dbx.files_list_folder_continue(res.cursor))
                entries.extend(res.entries)
            return [e for e in entries if isinstance(e, dbx_files.FileMetadata)]

        files = list_files(str(directory))
        files = list_files(str(directory))
        filepaths = [PurePath(f.name) for f in files if pattern.fullmatch(PurePath(f.name).name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        # Insert the base file at the start
        filepaths.insert(0, base_path)

        def _producer():
            for f in filepaths:
                # f may be a Dropbox FileMetadata or a PurePath, handle both
                path_str = str(f)
                _, res = cast(tuple[dbx_files.FileMetadata, Response], dbx.files_download(path_str))
                buf = BytesIO(res.content)
                while True:
                    chunk = buf.read(blocksize)
                    if not chunk:
                        break
                    yield chunk
            logger.info(f"Successfully sent snapshot file(s) for {base_path}")

        return _producer()
