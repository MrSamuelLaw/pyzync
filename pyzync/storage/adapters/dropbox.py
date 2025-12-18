import os
import re
import time
import random
from io import BytesIO
from pathlib import PurePath
from typing import Callable, Any, Optional, cast

import humanize
from dropbox import dropbox_client
from dropbox import files as dbx_files
from dropbox import exceptions as dbx_exceptions
from requests.models import Response
from requests.exceptions import RequestException
from pydantic import field_validator, SecretStr, ConfigDict

from pyzync import logging
from pyzync.storage.interfaces import SnapshotStorageAdapter
from pyzync.otel import trace, with_tracer
from pyzync.interfaces import ZfsDatasetId, ZfsFilePath, SnapshotNode

logger = logging.get_logger(__name__)
tracer = trace.get_tracer(__name__)


class DropboxStorageAdapter(SnapshotStorageAdapter):

    model_config = ConfigDict(frozen=True)

    directory: PurePath
    key: Optional[SecretStr] = None
    secret: Optional[SecretStr] = None
    refresh_token: Optional[SecretStr] = None
    access_token: Optional[SecretStr] = None
    max_file_size: int = 350 * (2**30)  # 350 GB

    # Retry/backoff configuration to tolerate transient network/timeouts
    max_retries: int = 5
    backoff_base: float = 0.5  # base seconds for exponential backoff
    backoff_max: float = 30.0  # max seconds to sleep between retries
    backoff_jitter: float = 0.1  # relative jitter (10%) added/subtracted

    def __str__(self):
        return f"DropboxStorageAdapter(directory={self.directory})"

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
            raise RuntimeError("Dropbox credentials not found. Set DROPBOX_REFRESH_TOKEN, DROPBOX_KEY,"
                               "DROPBOX_SECRET or DROPBOX_TOKEN in your environment.")

    def _retry_call(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        """Retry a Dropbox SDK call with exponential backoff and jitter for transient errors.

        Treats network-related exceptions and Dropbox Api/Http errors as retriable.
        """
        attempt = 0
        while True:
            try:
                return fn(*args, **kwargs)
            except Exception as e:  # noqa: BLE001 - broad catch for retry policy
                attempt += 1
                # Consider these exceptions transient/retriable
                is_transient = isinstance(e, (
                    dbx_exceptions.ApiError,
                    dbx_exceptions.HttpError,
                    RequestException,
                ))
                # If we've exhausted retries or the error is clearly not transient, re-raise
                if attempt >= (self.max_retries or 1) or not is_transient:
                    logger.exception("Dropbox API call failed and will not be retried")
                    raise
                # Exponential backoff with jitter
                sleep = min(self.backoff_max, self.backoff_base * (2**(attempt - 1)))
                jitter = sleep * self.backoff_jitter
                sleep = max(0.0, sleep + random.uniform(-jitter, jitter))
                logger.exception(
                    f"Transient Dropbox error (attempt {attempt}/{self.max_retries}), retrying in {sleep:.1f}s.",
                )
                time.sleep(sleep)

    @with_tracer(tracer)
    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        lgr = logger.bind(dataset_id=dataset_id, directory=self.directory)
        lgr.debug("Querying snapshots")
        dbx = self._client()
        # Build the path to list
        if dataset_id is None:
            path = self.directory
        else:
            path = self.directory.joinpath(dataset_id)
        # Recursively list all .zfs files
        def list_files(folder: str):
            try:
                res = cast(dbx_files.ListFolderResult,
                           self._retry_call(dbx.files_list_folder, folder, recursive=True))
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = cast(dbx_files.ListFolderResult,
                           self._retry_call(dbx.files_list_folder_continue, res.cursor))
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
        lgr = logger.bind(node=node, directory=self.directory)
        lgr.info("Destroying snapshot")
        dbx = self._client()
        base_path = self.directory.joinpath(node.filepath)
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix

        # List all files in the directory on Dropbox
        def list_files(folder):
            try:
                res = cast(dbx_files.ListFolderResult,
                           self._retry_call(dbx.files_list_folder, str(folder)))
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = cast(dbx_files.ListFolderResult,
                           self._retry_call(dbx.files_list_folder_continue, res.cursor))
                entries.extend(res.entries)
            return [PurePath(e.path_display) for e in entries if isinstance(e, dbx_files.FileMetadata)]

        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")
        filepaths = list_files(directory)
        filepaths = [f for f in filepaths if pattern.fullmatch(f.name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        filepaths.insert(0, base_path)
        for fp in filepaths:
            try:
                self._retry_call(dbx.files_delete_v2, str(fp))
            except dbx_exceptions.ApiError:
                lgr.warning("Dropbox file could not be destroyed", path=fp)

    @with_tracer(tracer)
    def get_consumer(self, node: SnapshotNode):
        lgr = logger.bind(node=node)
        lgr.debug("Getting consumer")

        path = self.directory.joinpath(node.filepath)

        MODULO = 4 * (2**20)  # 4MB
        MAX_BUFFER_SIZE = 144 * (2**20)  # 144 MB

        @with_tracer(tracer)
        def consume_bytes(path: PurePath):
            file_index = 0
            file_size = 0
            cur_path = path
            session_id = None
            buffer = b''
            dbx = None
            try:
                nonlocal lgr
                lgr = lgr.bind(path=path)
                lgr.debug("Starting dropbox consumer generator")
                while True:
                    # once the iterator is primed it starts here
                    chunk = yield
                    if chunk is None:
                        lgr.debug('Shutting down consumer generator')
                        break
                    if session_id is None or dbx is None:
                        dbx = self._client()
                        result = cast(dbx_files.UploadSessionStartResult,
                                      self._retry_call(dbx.files_upload_session_start, b''))
                        session_id = result.session_id
                        file_size = 0

                    buffer += chunk
                    if (self.max_file_size is not None) and ((file_size + len(buffer))
                                                             > self.max_file_size):
                        split_index = self.max_file_size - file_size
                        left, right = buffer[0:split_index], buffer[split_index:]
                        self._retry_call(
                            dbx.files_upload_session_finish, left,
                            dbx_files.UploadSessionCursor(session_id, file_size),
                            dbx_files.CommitInfo(str(cur_path), mode=dbx_files.WriteMode.overwrite))
                        buffer = right
                        file_index += 1
                        cur_path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
                        lgr = lgr.bind(path=cur_path)
                        lgr.debug("Sharding to next file")
                        result = cast(dbx_files.UploadSessionStartResult,
                                      self._retry_call(dbx.files_upload_session_start, b''))
                        session_id = result.session_id
                        file_size = 0
                    else:
                        while len(buffer) >= MODULO:
                            # Calculate largest slice that's divisible by MODULO and less than MAX_SIZE
                            slice_size = min(len(buffer) - (len(buffer) % MODULO), MAX_BUFFER_SIZE)
                            data, buffer = buffer[:slice_size], buffer[slice_size:]
                            # perform the upload
                            self._retry_call(dbx.files_upload_session_append_v2, data,
                                             dbx_files.UploadSessionCursor(session_id, file_size))
                            file_size += len(data)
                if buffer and session_id and dbx:
                    while len(buffer) >= MODULO:
                        # Calculate largest slice that's divisible by MODULO and less than MAX_SIZE
                        slice_size = min(len(buffer) - (len(buffer) % MODULO), MAX_BUFFER_SIZE)
                        data, buffer = buffer[:slice_size], buffer[slice_size:]
                        # perform the upload
                        self._retry_call(dbx.files_upload_session_append_v2, data,
                                         dbx_files.UploadSessionCursor(session_id, file_size))
                        file_size += len(data)
                    self._retry_call(
                        dbx.files_upload_session_finish, buffer,
                        dbx_files.UploadSessionCursor(session_id, file_size),
                        dbx_files.CommitInfo(str(cur_path), mode=dbx_files.WriteMode.overwrite))
            except:
                lgr.exception('Error consuming bytes')
                raise

        consumer = consume_bytes(path=path)
        return consumer

    @with_tracer(tracer)
    def get_producer(self, node: SnapshotNode, bufsize: int = 2**20):  # default 1 MB
        lgr = logger.bind(node=node, bufsize=humanize.naturalsize(bufsize))
        lgr.debug("Getting producer")
        dbx = self._client()
        base_path = self.directory.joinpath(node.filepath)
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix
        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")

        def list_files(folder):
            try:
                res = cast(dbx_files.ListFolderResult, self._retry_call(dbx.files_list_folder, folder))
            except dbx_exceptions.ApiError:
                return []
            entries = res.entries
            while res.has_more:
                res = cast(dbx_files.ListFolderResult,
                           self._retry_call(dbx.files_list_folder_continue, res.cursor))
                entries.extend(res.entries)
            return [e for e in entries if isinstance(e, dbx_files.FileMetadata)]

        files = list_files(str(directory))
        filepaths = [PurePath(f.path_lower) for f in files if pattern.fullmatch(PurePath(f.name).name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        # Insert the base file at the start
        filepaths.insert(0, base_path)

        def _producer():
            for fp in filepaths:
                # f may be a Dropbox FileMetadata or a PurePath, handle both
                path_str = str(fp)
                lgr.debug("Streaming data from file", path=fp)
                _, res = cast(tuple[dbx_files.FileMetadata, Response],
                              self._retry_call(dbx.files_download, path_str))
                buf = BytesIO(res.content)
                while True:
                    chunk = buf.read(bufsize)
                    if not chunk:
                        break
                    yield chunk

        return _producer()
