"""Storage adapter implementations for handling snapshot data persistence.

This module provides concrete implementations of the SnapshotDataAdapter interface
for different storage backends. Currently supports local filesystem storage.
"""

import re
from pathlib import Path

from pyzync.interfaces import SnapshotStorageAdapter, SnapshotStream


class LocalFileSnapshotDataAdapter(SnapshotStorageAdapter):
    """Handles storage and retrieval of snapshot data in the local filesystem.

    This adapter implements operations for storing, retrieving, and managing
    snapshot data as files on the local system's storage. It ensures paths
    are absolute and handles file operations safely.
    """

    def __init__(self, directory: str | Path):
        directory = Path(directory)
        self.directory = self.validate_path(directory).resolve()

    @staticmethod
    def validate_path(directory: Path):
        if not directory.is_absolute():
            raise ValueError(f"directory must be absolute, instead received path = {directory}")
        return directory

    def query(self, zfs_prefix: Path = None):
        """Search for snapshot files in the specified directory.

        Args:
            directory (Path): The root directory to search in
            zfs_prefix (Path, optional): Optional prefix to filter snapshot files

        Returns:
            list[Path]: List of relative paths to snapshot files, sorted by date

        Raises:
            ValueError: If the provided directory path is not absolute
        """

        # query all the .zfs files at the root and below
        glob_pattern = "*.zfs" if zfs_prefix is None else f"**/{zfs_prefix}/*.zfs"
        match_pattern = re.compile(r"(\d{8}.zfs)|(\d{8}_\d{8}.zfs)")
        matches = [r for r in self.directory.rglob(glob_pattern)]
        matches = [r for r in self.directory.rglob(glob_pattern) if match_pattern.fullmatch(r.name)]
        matches = [r.relative_to(self.directory) for r in matches]
        return matches

    def destroy(self, filepaths: list[Path]):
        """Delete the specified snapshot files from storage.

        Args:
            filepaths (list[Path]): List of relative paths to snapshot files

        Returns:
            list[bool]: List of success flags for each deletion attempt

        Raises:
            ValueError: If any of the provided paths do not exist
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
            directory (Path): Directory where the snapshot file will be stored
            stream (SnapshotStream): Stream containing the snapshot data

        Returns:
            Path: Absolute path to the created snapshot file

        Raises:
            ValueError: If the directory path is not absolute
        """
        path = self.directory.joinpath(stream.filepath)

        # create the parent dirs if they don't exist
        if not path.parent.exists():
            path.parent.mkdir(parents=True)

        # open the file and write the stream in
        with open(path, 'wb') as handle:
            for chunk in stream.iterable:
                handle.write(chunk)
        return path

    def send(self, filepaths: list[Path], blocksize: int = 4096):
        """Read snapshot files and yield their contents as a stream.

        Args:
            filepaths (list[Path]): List of snapshot files to read relative to the root directory
            blocksize (int, optional): Size of chunks to read. Defaults to 4096

        Yields:
            bytes: Chunks of snapshot data
        """
        filepaths = [self.directory.joinpath(fp) for fp in filepaths]

        def _iterable():
            for fp in filepaths:
                with open(fp, 'rb', buffering=0) as handle:
                    yield handle.read(blocksize)

        return _iterable()
