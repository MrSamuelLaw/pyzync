"""Storage adapter implementations for handling snapshot data persistence.

This module provides concrete implementations of the SnapshotStorageAdapter interface
for different storage backends. Currently supports local filesystem storage.
"""

import re
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator

from pyzync.interfaces import SnapshotStorageAdapter, SnapshotStream


class LocalFileSnapshotDataAdapter(SnapshotStorageAdapter, BaseModel):
    """Handles storage and retrieval of snapshot data in the local filesystem.

    Implements operations for storing, retrieving, and managing
    snapshot data as files on the local system's storage.
    """

    model_config = ConfigDict(frozen=True)

    directory: Path

    @field_validator('directory')
    @classmethod
    def validate_path(cls, directory: Path):
        """Ensure the directory is an absolute path."""
        if not directory.is_absolute():
            raise ValueError(f"directory must be absolute, instead received path = {directory}")
        return directory.resolve()

    def query(self, zfs_dataset_path: Path = None):
        """Search for snapshot files in the specified directory.

        Args:
            zfs_dataset_path (Path, optional): Optional prefix to filter snapshot files.

        Returns:
            list[Path]: List of relative paths to snapshot files, sorted by date.
        """

        # query all the .zfs files at the root and below
        glob_pattern = "*.zfs" if zfs_dataset_path is None else f"**/{zfs_dataset_path}/*.zfs"
        match_pattern = re.compile(r"(\d{8}.zfs)|(\d{8}_\d{8}.zfs)")
        matches = [r for r in self.directory.rglob(glob_pattern)]
        matches = [r for r in self.directory.rglob(glob_pattern) if match_pattern.fullmatch(r.name)]
        matches = [r.relative_to(self.directory) for r in matches]
        return matches

    def destroy(self, filepaths: list[Path]):
        """Delete the specified snapshot files from storage.

        Args:
            filepaths (list[Path]): List of relative paths to snapshot files.

        Returns:
            list[bool]: List of success flags for each deletion attempt.
        """
        filepaths = [self.directory.joinpath(fp) for fp in filepaths]
        success_flags = []
        for fp in filepaths:
            try:
                fp.unlink()
                success_flags.append(True)
            except FileNotFoundError as e:
                success_flags.append(False)
        return success_flags

    def recv(self, stream: SnapshotStream):
        """Store a snapshot stream to a file in the specified directory.

        Args:
            stream (SnapshotStream): Stream containing the snapshot data.

        Returns:
            Path: Absolute path to the created snapshot file.
        """
        path = self.directory.joinpath(stream.filepath)

        # create the parent dirs if they don't exist
        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        # open the file and write the stream in
        with open(path, 'wb') as handle:
            for chunk in stream.snapshot_stream:
                handle.write(chunk)
        return path

    def send(self, filepath: Path, blocksize: int = 4096):
        """Read a snapshot file and yield its contents as a stream.

        Args:
            filepath (Path): Snapshot file to read, relative to the root directory.
            blocksize (int, optional): Size of chunks to read. Defaults to 4096.

        Returns:
            Iterable[bytes]: Iterable yielding chunks of snapshot data.
        """
        filepath = self.directory.joinpath(filepath)

        def _snapshot_stream():
            with open(filepath, 'rb', buffering=0) as handle:
                while True:
                    chunk = handle.read(blocksize)
                    if not chunk:
                        break
                    yield chunk

        return _snapshot_stream()
