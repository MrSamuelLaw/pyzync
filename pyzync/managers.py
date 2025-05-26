"""
Manages ZFS snapshots and their data through host system operations and data storage.

This module provides two main manager classes for handling ZFS snapshots:
- HostSnapshotManager: Manages direct ZFS operations on the host system.
- FileSnapshotManager: Manages snapshot data storage and lineage tracking.
"""

import re
import sys
import warnings
import subprocess
from datetime import date as Date
from pathlib import PurePath
from functools import partial
from textwrap import dedent
from itertools import groupby
from typing import Optional, Literal, Iterable, Callable

from pydantic import BaseModel, ConfigDict

from pyzync.errors import DataCorruptionError, DataIntegrityError
from pyzync.interfaces import SnapshotRef, SnapshotStream, SnapshotStorageAdapter


class HostSnapshotManager:
    """Manages ZFS snapshots on the host system.

    Provides functionality for creating, querying, destroying, and sending both complete
    and incremental snapshots using ZFS commands.
    """

    @staticmethod
    def create(zfs_dataset_path: PurePath, date: Date):
        """
        Create a new ZFS snapshot on the host system.

        Args:
            zfs_dataset_path (PurePath): Path to the ZFS dataset.
            date (Date): Date to be used for the snapshot name.

        Returns:
            SnapshotRef: Reference to the created snapshot.

        Raises:
            subprocess.CalledProcessError: If snapshot creation fails.
        """
        try:
            snapshot = SnapshotRef(zfs_dataset_path=zfs_dataset_path, date=date)
            subprocess.run(
                ["bash", "-c", f"zfs snapshot {snapshot.zfs_snapshot_id}"],
                capture_output=True,
                text=True,
                check=True,
            )
            return snapshot
        except subprocess.CalledProcessError as e:
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    def query(zfs_dataset_path: Optional[PurePath] = None):
        """
        Query existing ZFS snapshots on the host system.

        Args:
            zfs_dataset_path (Optional[PurePath]): Optional path to filter snapshots by dataset.

        Returns:
            list[SnapshotRef]: List of snapshot references matching the query.

        Raises:
            subprocess.CalledProcessError: If snapshot query fails.
        """

        def _from_zfs_snapshot_id(zfs_snapshot_id):
            zfs_dataset_path, date = zfs_snapshot_id.split("@")
            return SnapshotRef(date=date, zfs_dataset_path=zfs_dataset_path)

        try:
            if zfs_dataset_path is None:
                regex = r"(\w|\/)+@\d{8}"
            else:
                regex = str(zfs_dataset_path) + r"@\d{8}"
            result = subprocess.run(
                ["bash", "-c", f'zfs list -t snapshot -o name | grep -P "{regex}"'],
                capture_output=True,
                text=True,
                check=True,
            )
            snapshots = result.stdout.splitlines()
            snapshots = [_from_zfs_snapshot_id(n) for n in snapshots]
            return snapshots

        except subprocess.CalledProcessError as e:
            if e.stderr.strip() == "no datasets available":
                result: list[SnapshotRef] = []
                return result
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    def destroy(snapshot_ref: SnapshotRef):
        """
        Destroy a ZFS snapshot on the host system.

        Args:
            snapshot_ref (SnapshotRef): Reference to the snapshot to destroy.

        Returns:
            SnapshotRef: Reference to the destroyed snapshot.

        Raises:
            subprocess.CalledProcessError: If snapshot destruction fails.
        """
        try:
            subprocess.run(
                ["bash", "-c", f"zfs destroy -d {snapshot_ref.zfs_snapshot_id}"],
                capture_output=True,
                text=True,
                check=True,
            )
            return snapshot_ref

        except subprocess.CalledProcessError as e:
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    def send(ref: SnapshotRef,
             base: Optional[SnapshotRef] = None,
             zfs_flags: list[str] = ["-R"],
             blocksize: int = 4096):
        """
        Stream a ZFS snapshot, optionally as an increment from an base.

        Args:
            ref (SnapshotRef): Reference to the snapshot to send.
            base (Optional[SnapshotRef]): Optional reference to base the incremental snapshot on.
            zfs_flags (list[str]): Additional ZFS send flags.
            blocksize (int): Size of data blocks to yield.

        Returns:
            list[SnapshotStream]: List containing a SnapshotStream iterator over the snapshot data.

        Raises:
            DataIntegrityError: If base is newer than ref.
            subprocess.CalledProcessError: If snapshot sending fails.
            RuntimeError: If subprocess stdout cannot be opened.
        """

        if (base is not None) and (base.date >= ref.date):
            raise DataIntegrityError(
                dedent("""
                    Cannot create incremental snapshots that link a newer snapshot to an older snapshot
                    Incremental snapshots most link an older snapshot to a new snapshot
                """))

        def _iterator():
            try:
                # build the command
                cmd = ["zfs", "send", *zfs_flags]
                if base is not None:
                    cmd.extend(["-i", base.zfs_snapshot_id.split("@")[1]])
                cmd.append(ref.zfs_snapshot_id)

                # using a context manager, build an unbuffered generator to stream the data
                with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      bufsize=0) as process:
                    # check to make sure the pipe is open
                    if process.stdout is None:
                        raise RuntimeError("Failed to open stdout for the subprocess")

                    # iterate over the binary data
                    for block in iter(partial(process.stdout.read, blocksize), b""):
                        yield block

                    # check for non-zero return code
                    returncode = process.wait(1)
                    if returncode:
                        error = process.stderr.read()
                        raise subprocess.CalledProcessError(returncode, cmd, stderr=error)
            except subprocess.CalledProcessError as e:
                print(e.stderr, file=sys.stderr)
                raise

        iterator: Iterable[bytes] = _iterator()
        stream = SnapshotStream(
            ref=ref,
            base=base,
            snapshot_stream=iterator,
        )
        return [stream]

    @staticmethod
    def recv(stream: SnapshotStream):
        """
        Receive and restore a ZFS snapshot stream.

        Args:
            stream (SnapshotStream): SnapshotStream containing the snapshot data to restore.

        Raises:
            subprocess.CalledProcessError: If snapshot receive operation fails.
            RuntimeError: If subprocess stdin cannot be opened.
        """
        try:
            # Build the command for receiving the snapshot
            cmd = ["zfs", "recv", "-F", stream.ref.zfs_dataset_path]
            with subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE,
                                  bufsize=0) as process:

                # Check that the pipe is open
                if process.stdin is None:
                    raise RuntimeError("Failed to open stdin for the subprocess")

                # write the data
                for block in stream.snapshot_stream:
                    process.stdin.write(block)

                # flush and close the buffer
                process.stdin.flush()
                process.stdin.close()

                # wait for the process to complete
                returncode = process.wait(1)
                if returncode:
                    error = process.stderr.read()
                    raise subprocess.CalledProcessError(returncode, cmd, stderr=error)
        except subprocess.CalledProcessError as e:
            print(e.stderr, file=sys.stderr)
            raise


class LineageTableNode(BaseModel):
    """Represents a node in a snapshot lineage table.

    A node can represent either a complete snapshot or an incremental snapshot,
    identified by its filename and date.

    Attributes:
        filename (str): Name of the snapshot file.
        date (str): Date string of the snapshot.
        node_type (Literal['complete', 'incremental']): Node type.
    """

    model_config = ConfigDict(frozen=True)

    filename: str
    date: str
    node_type: Literal['complete', 'incremental']

    def __eq__(self, other):
        return bool(self.date == other.date)

    def __hash__(self):
        return hash(self.filename)


type LineageTableChain = tuple[Optional[LineageTableNode], ...]


class LineageTable(BaseModel):
    """Represents a table of snapshot lineages.

    The table is organized as a matrix where:
    - Columns represent different backup chains/lineages.
    - Rows represent different dates.
    - Each cell contains either None or a LineageTableNode.
    - Complete nodes start new columns.
    - Incremental nodes build upon previous nodes in the same column.

    Example structure:
        Date1: [Complete, None,    None   ]
        Date2: [Inc1,    Complete, None   ]
        Date3: [Inc2,    Inc1,    Complete]

    Attributes:
        zfs_dataset_path (PurePath): Path to the ZFS dataset.
        index (tuple[Date, ...]): Tuple of dates representing the row labels.
        data (tuple[LineageTableChain, ...]): Matrix of LineageTableNodes.
        orphaned_nodes (tuple[LineageTableNode, ...]): Nodes not belonging to any valid lineage.
    """

    model_config = ConfigDict(frozen=True)

    zfs_dataset_path: PurePath
    index: tuple[Date, ...]
    data: tuple[LineageTableChain, ...]
    orphaned_nodes: tuple[LineageTableNode, ...]


class FileSnapshotManager:
    """
    Manages data generated from snapshots, handling both complete and 
    incremental snapshots. Complete snapshots contain all the data needed 
    for a full restore, while incremental snapshots only contain changes 
    since a previous snapshot.
    """

    def __init__(self, adapter: SnapshotStorageAdapter):
        """
        Initialize the FileSnapshotManager with a storage adapter.

        Args:
            adapter (SnapshotStorageAdapter): Adapter for accessing snapshot data.
        """
        self.__adapter = adapter

    @staticmethod
    def _compute_lineage_tables(filepaths: list[PurePath]):
        """
        Compute lineage tables from a list of snapshot file paths.

        Args:
            filepaths (list[PurePath]): List of paths to snapshot files.

        Returns:
            list[LineageTable]: List of computed lineage tables.

        Raises:
            DataCorruptionError: If invalid incremental snapshots are found.
        """

        # predefine the patterns used to seperate out files
        complete_pattern = re.compile(r"\d{8}.zfs")
        incremental_pattern = re.compile(r"\d{8}_\d{8}.zfs")

        # build out lineages using the tips
        lineage_tables: list[LineageTable] = []

        # sorting the files_paths allows for a single forward pass
        filepaths = sorted(filepaths)
        for zfs_dataset_path, group_files in groupby(filepaths, key=lambda p: p.parent):
            zfs_dataset_path = SnapshotRef.format_zfs_dataset_path(zfs_dataset_path)

            # for each group, get the file name
            group_files = [r.name for r in group_files]

            # split the results into complete and incremental files
            complete_files = [n for n in group_files if complete_pattern.fullmatch(n)]
            incremental_files = [n for n in group_files if incremental_pattern.fullmatch(n)]

            # for the incrementals, cache if they've been used or not so we can identify orphans later
            incremental_files = [{"name": n, "orphaned": True} for n in incremental_files]

            # build out the lineages for each complete snapshot
            table_index: set[Date] = set()
            table_data: list[list[Optional[LineageTableNode]]] = []
            for filename in complete_files:
                # start with a complete node
                lineage = [LineageTableNode(filename=filename, date=filename[:8], node_type='complete')]
                table_index.add(lineage[-1].date)
                # join incremental nodes in order
                for incremental_file in incremental_files:
                    filename = incremental_file["name"]
                    if filename[0:8] >= filename[9:17]:
                        raise DataCorruptionError(
                            f'Incremental snapshot that increments back in time found with filename = {filename}'
                        )
                    if lineage[-1].date == filename[:8]:
                        incremental_file["orphaned"] = False
                        lineage.append(
                            LineageTableNode(filename=filename,
                                             date=filename[9:17],
                                             node_type='incremental'))
                        table_index.add(lineage[-1].date)
                table_data.append(lineage)

            # using the index created in the previous loops build out a table structure
            table_index = sorted(list(table_index))
            for i, lineage in enumerate(table_data):
                column = [None] * len(table_index)
                lineage_map = {node.date: node for node in lineage}
                for j, date in enumerate(table_index):
                    column[j] = lineage_map.get(date)
                table_data[i] = column

            # create orphaned nodes
            orphaned_nodes = [
                LineageTableNode(filename=o['name'], date=o['name'][9:17], node_type='incremental')
                for o in incremental_files
                if o["orphaned"]
            ]
            if orphaned_nodes:
                warnings.warn(f'The following orphaned LineageNodes were found, {orphaned_nodes}')

            table_index = [Date.fromisoformat(d) for d in table_index]
            table = LineageTable(zfs_dataset_path=zfs_dataset_path,
                                 index=table_index,
                                 data=table_data,
                                 orphaned_nodes=orphaned_nodes)

            # add the table to the master list
            lineage_tables.append(table)

        return lineage_tables

    @staticmethod
    def _compute_refs_from_lineage_tables(lineage_tables: list[LineageTable]):
        """
        Extract snapshot references from lineage tables.

        Args:
            lineage_tables (list[LineageTable]): List of lineage tables to process.

        Returns:
            list[SnapshotRef]: List of snapshot references.
        """
        refs: list[LineageTableNode] = []
        for table in lineage_tables:
            for date in table.index:
                refs.append(SnapshotRef(date=date, zfs_dataset_path=table.zfs_dataset_path))
        return refs

    def query(self, zfs_dataset_path: Optional[PurePath] = None):
        """
        Query snapshots using the configured adapter.

        Args:
            zfs_dataset_path (Optional[PurePath]): Optional path to filter snapshots by dataset.

        Returns:
            list[SnapshotRef]: List of snapshot references.
        """
        if zfs_dataset_path is not None:
            zfs_dataset_path = SnapshotRef.format_zfs_dataset_path(zfs_dataset_path)
        matches = self.__adapter.query(zfs_dataset_path)
        lineage_tables = self._compute_lineage_tables(matches)
        refs = self._compute_refs_from_lineage_tables(lineage_tables)
        return refs

    @staticmethod
    def _compute_redundant_nodes(table: LineageTable):
        """
        Find all nodes that are redundant in the table such that
        no lineages will be broken and no incremental snapshots will be orphaned.
        In the event that multiple paths through the table exist, the nodes
        that, when removed, minimize the total number of complete nodes will be selected.

        Args:
            table (LineageTable): The lineage table to analyze.

        Returns:
            list[LineageTableNode]: List of redundant nodes.
        """
        # convert to row major format and begin searching
        rows = tuple(map(tuple, zip(*table.data)))

        # we can always start at 1 because row zero will always have only a single comlete node in it
        redundant_nodes: list[LineageTableNode] = []
        for j in range(1, len(table.index)):
            i = j - 1
            nodes: list[LineageTableNode] = [n for n in rows[j] if n is not None]
            if len(nodes) > 1:
                # any complete nodes can be safely pruned if on a row with an incremental node
                complete_nodes = [n for n in nodes if n.node_type == 'complete']
                redundant_nodes.extend(complete_nodes)
                # for the incremental nodes, we preference nodes with a non-null previous index
                incremental_nodes = [(x, n) for x, n in enumerate(nodes) if n.node_type == 'incremental']
                if len(incremental_nodes) > 1:
                    for t in incremental_nodes:
                        if rows[i][t[0]] is None:
                            redundant_nodes.append(t[1])
        return redundant_nodes

    @staticmethod
    def _is_ref_deletable(table: LineageTable, ref: SnapshotRef):
        """
        Determine if a snapshot reference is deletable without breaking lineage.

        Args:
            table (LineageTable): The lineage table to check.
            ref (SnapshotRef): The snapshot reference to check.

        Returns:
            bool: True if the reference is deletable, False otherwise.

        Raises:
            ValueError: If zfs_dataset_path does not match.
        """
        if table.zfs_dataset_path != ref.zfs_dataset_path:
            raise ValueError('zfs_dataset_path for LineageTable must match zfs_dataset_path for SnapshotRef')

        if ref.date == table.index[-1]:
            return True
        elif ref.date == table.index[0] and len(table.index) > 1:
            # there must be a complete node in row 1
            return bool(
                [c[1] for c in table.data if (c[1] is not None) and (c[1].node_type == 'complete')])
        return False

    def destroy(self, ref: SnapshotRef):
        """
        Destroy snapshot files related to the given reference, ensuring lineage is not broken.

        Args:
            ref (SnapshotRef): The snapshot reference to destroy.

        Returns:
            tuple[list[PurePath], list[bool]]: Tuple of filepaths and success flags.

        Raises:
            ValueError: If multiple lineage tables are found.
            DataIntegrityError: If the reference is not deletable.
        """
        # query the files related to the ref
        matches = self.__adapter.query(ref.zfs_dataset_path)
        tables = self._compute_lineage_tables(matches)

        # if multiple lineages were found, something went wrong
        if len(tables) > 1:
            raise ValueError(f'Multiple lineage tables found when attempting to destroy ref {ref}. '
                             'Expected exactly one table.')

        # ensure ref is deletable
        table = tables[0]
        if not self._is_ref_deletable(table, ref):
            raise DataIntegrityError(
                f'Cannot delete snapshot {ref} as it would break snapshot lineage. '
                'Only the most recent snapshot or oldest snapshot (with complete backup) '
                'can be deleted.')
        row_idx = table.index.index(ref.date)
        row = [col[row_idx] for col in table.data if col[row_idx] is not None]
        filepaths = [table.zfs_dataset_path.joinpath(v.filename) for v in row]
        success_flags = self.__adapter.destroy(filepaths)
        return (filepaths, success_flags)

    def prune(self, zfs_dataset_path: PurePath):
        """
        Prune orphaned and redundant snapshot files for a given ZFS prefix.

        Args:
            zfs_dataset_path (PurePath): The ZFS dataset prefix.

        Returns:
            tuple[list[PurePath], list[bool]]: Tuple of filepaths and success flags.
        """
        zfs_dataset_path = SnapshotRef.format_zfs_dataset_path(zfs_dataset_path)
        paths = self.__adapter.query(zfs_dataset_path)
        tables = self._compute_lineage_tables(paths)
        filepaths, success_flags = ([], [])
        for table in tables:
            filepaths.extend([table.zfs_dataset_path.joinpath(n.filename) for n in table.orphaned_nodes])
            redundant_nodes = self._compute_redundant_nodes(table)
            filepaths.extend([table.zfs_dataset_path.joinpath(n.filename) for n in redundant_nodes])
            success_flags.extend(self.__adapter.destroy(filepaths))
        return (filepaths, success_flags)

    def recv(self, stream: SnapshotStream):
        """
        Receive and store a snapshot stream using the adapter.

        Args:
            stream (SnapshotStream): The snapshot stream to receive.

        Returns:
            Any: Result of the adapter's recv operation.

        Raises:
            DataIntegrityError: If the file already exists.
        """
        # validate that we won't be overwriting an existing file
        paths = self.__adapter.query(stream.ref.zfs_dataset_path)
        filenames = [p.name for p in paths]
        if stream.filename in filenames:
            raise DataIntegrityError(f'Cannot overwrite existing file = {stream.filename}')

        # write it out using the adapter
        return self.__adapter.recv(stream)

    def send(self, ref: SnapshotRef, chain_filter: Optional[Callable[[list[LineageTableChain]], LineageTableChain]] = None):
        """
        Generate SnapshotStreams for the given snapshot reference using the adapter.

        Args:
            ref (SnapshotRef): The snapshot reference to send.
            chain_filter (Optional[Callable]): Optional function to select a chain if multiple exist.

        Returns:
            list[SnapshotStream]: List of SnapshotStreams for the reference.

        Raises:
            ValueError: If multiple lineage tables are found or data for the reference is missing.
        """
        # get the lineage table
        filepaths = self.__adapter.query(ref.zfs_dataset_path)
        tables = self._compute_lineage_tables(filepaths)
        if len(tables) > 1:
            raise ValueError(f'Multiple lineage tables found when attempting to send ref {ref}. '
                             'Expected exactly one table')
        table = tables[0]

        # use the table to find the lineage with the requisite ref dates
        if ref.date not in table.index:
            raise ValueError(f'Cannot find data for SnapshotRef = {ref}')

        try:
            row_idx = table.index.index(ref.date)
        except ValueError as e:
            raise ValueError(f'Unable to generate SnapshotStream for ref = {ref}')

        # get all the nodes that could make the ref, capturing which column the node came from
        chains = [c for c in table.data if c[row_idx] is not None]

        chain = chain_filter(chains) if len(chains) > 1 else chains[0]

        # build the snapshot stream from the node
        streams = []
        for node in (n for n in chain[:row_idx + 1] if n is not None):
            filepath = table.zfs_dataset_path.joinpath(node.filename)
            snapshot_stream = self.__adapter.send(filepath)
            ref = SnapshotRef(date=node.date, zfs_dataset_path=table.zfs_dataset_path)
            base = streams[-1].ref if node.node_type == 'incremental' else None
            streams.append(SnapshotStream(ref=ref, base=base, snapshot_stream=snapshot_stream))
        return streams
