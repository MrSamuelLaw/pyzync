import re
from pathlib import Path, PurePath
from typing import Optional

import humanize
from pydantic import BaseModel, ConfigDict, field_validator

from pyzync import logging
from pyzync.otel import with_tracer, trace
from pyzync.interfaces import ZfsDatasetId, ZfsFilePath, SnapshotNode
from pyzync.storage.interfaces import SnapshotStorageAdapter

logger = logging.get_logger(__name__)
tracer = trace.get_tracer(__name__)


class LocalFileStorageAdapter(SnapshotStorageAdapter, BaseModel):

    model_config = ConfigDict(frozen=True)

    directory: Path
    max_file_size: Optional[int] = None

    @field_validator('directory')
    @classmethod
    def validate_path(cls, directory: Path):
        if not directory.is_absolute():
            raise ValueError(f"directory must be absolute, instead received path = {directory}")
        return directory.resolve()

    def __str__(self):
        return f"LocalFileStorageAdapter(directory={self.directory}, max_file_size={self.max_file_size})"

    @with_tracer(tracer)
    def query(self, dataset_id: Optional[ZfsDatasetId] = None):
        lgr = logger.bind(dataset_id=dataset_id, directory=self.directory)
        lgr.debug("Querying snapshots")
        # query all the .zfs files at the root and below
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
        lgr = logger.bind(node=node, directory=self.directory)
        lgr.info("Detroying snapshot")
        filepath = node.filepath
        base_path = self.directory.joinpath(filepath)
        base_path = base_path.resolve()
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix

        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")
        filepaths = [f for f in directory.glob(f"{stem}_*{suffix}")]
        filepaths = [f for f in filepaths if pattern.fullmatch(f.name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        filepaths.insert(0, base_path)
        for fp in filepaths:
            fp.unlink()

    @with_tracer(tracer)
    def get_consumer(self, node: SnapshotNode):
        lgr = logger.bind(node=node, directory=self.directory)
        lgr.debug("Getting consumer")
        # build a function that can handle a single chunck
        path = self.directory.joinpath(node.filepath)
        # create the parent dirs if they don't exist
        if not path.parent.exists():
            lgr.debug("Creating parent directories", path=path)
            path.parent.mkdir(parents=True)

        # define a helper function to open the file and write the stream in using context manager
        @with_tracer(tracer)
        def consume_bytes(path: PurePath):
            file_size = 0
            file_index = 0
            cur_path = path
            handle = None
            try:

                nonlocal lgr
                lgr = lgr.bind(path=path)
                lgr.debug("Starting file consumer generator",
                          max_file_size=humanize.naturalsize(self.max_file_size)
                          if self.max_file_size is not None else None)
                while True:
                    chunk = yield
                    if chunk is None:
                        lgr.debug("Shutting down consumer generator")
                        break
                    elif handle is None:
                        handle = open(cur_path, 'wb')

                    # write out until out of data or at max file size, then start a new file
                    chunk_size = len(chunk)
                    if (self.max_file_size is not None) and ((file_size + chunk_size)
                                                             > self.max_file_size):
                        # fill up the file to the max size with the left chunk
                        split_index = self.max_file_size - file_size
                        left, right = chunk[0:split_index], chunk[split_index:]
                        handle.write(left)
                        # close the current file and start open the next one
                        handle.close()
                        file_index += 1
                        cur_path = path.with_name(f"{path.stem}_{file_index}{path.suffix}")
                        handle = open(cur_path, 'wb')
                        lgr = lgr.bind(path=cur_path)
                        lgr.debug("Sharding to next file")
                        # put the rest of the chunk at the start of the next file
                        file_size = len(right)
                        handle.write(right)
                    else:
                        file_size += len(chunk)
                        handle.write(chunk)
            except IOError:
                lgr.exception("Error consuming bytes")
                raise
            except Exception:
                lgr.error("Error consuming bytes")
                raise
            finally:
                if handle:
                    handle.close()

        consumer = consume_bytes(path=path)
        return consumer

    @with_tracer(tracer)
    def get_producer(self, node: SnapshotNode, bufsize: int = 2**20):  # default blocksize = 1MB
        lgr = logger.bind(node=node, bufsize=humanize.naturalsize(bufsize), directory=self.directory)
        lgr.debug("Getting producer")
        filepath = node.filepath
        base_path = self.directory.joinpath(filepath)
        base_path = base_path.resolve()
        directory = base_path.parent
        stem = base_path.stem
        suffix = base_path.suffix

        # Find all chunked files: base, _1, _2, ...
        pattern = re.compile(rf"{re.escape(stem)}_\d+{re.escape(suffix)}")
        filepaths = [f for f in directory.glob(f"{stem}_*{suffix}")]
        filepaths = [f for f in filepaths if pattern.fullmatch(f.name)]
        filepaths = sorted(filepaths, key=lambda f: int(f.stem.split('_')[-1]))
        filepaths.insert(0, base_path)

        @with_tracer(tracer)
        def _snapshot_stream():
            for fp in filepaths:
                with open(fp, 'rb', buffering=bufsize) as handle:
                    lgr.debug("Streaming data from file", path=fp)
                    while True:
                        chunk = handle.read(bufsize)
                        if not chunk:
                            break
                        yield chunk

        return _snapshot_stream()
