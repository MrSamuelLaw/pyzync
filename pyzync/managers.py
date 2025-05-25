"""
Manages ZFS snapshots and their data through host system operations and data storage.
This module provides two main manager classes for handling ZFS snapshots:
- HostSnapshotManager: Manages direct ZFS operations on the host system
- SnapshotDataManager: Manages snapshot data storage and lineage tracking
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
from typing import Optional, Literal

from pydantic import BaseModel, ConfigDict

from pyzync.errors import DataCorruptionError, DataIntegrityError
from pyzync.interfaces import SnapshotRef, SnapshotStream, SnapshotStorageAdapter


class HostSnapshotManager:
    """Manages ZFS snapshots on the host system.
    Provides functionality for creating, querying, and sending both complete
    and incremental snapshots."""

    @staticmethod
    def create(zfs_prefix: PurePath, date: Date):
        """Create a new ZFS snapshot on the host system.

        Args:
            zfs_prefix: Path to the ZFS dataset
            date: Date to be used for the snapshot name

        Returns:
            SnapshotRef: Reference to the created snapshot

        Raises:
            subprocess.CalledProcessError: If snapshot creation fails
        """
        try:
            snapshot = SnapshotRef(zfs_prefix=zfs_prefix, date=date)
            subprocess.run(
                ["bash", "-c", f"zfs snapshot {snapshot.zfs_handle}"],
                capture_output=True,
                text=True,
                check=True,
            )
            return snapshot
        except subprocess.CalledProcessError as e:
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    def query(zfs_prefix: Optional[PurePath] = None):
        """Query existing ZFS snapshots on the host system.

        Args:
            zfs_prefix: Optional path to filter snapshots by dataset

        Returns:
            list[SnapshotRef]: List of snapshot references matching the query

        Raises:
            subprocess.CalledProcessError: If snapshot query fails
        """

        def _from_zfs_handle(zfs_handle):
            zfs_prefix, date = zfs_handle.split("@")
            return SnapshotRef(date=date, zfs_prefix=zfs_prefix)

        try:
            if zfs_prefix is None:
                regex = r"(\w|\/)+@\d{8}"
            else:
                regex = str(zfs_prefix) + r"@\d{8}"
            result = subprocess.run(
                ["bash", "-c", f'zfs list -t snapshot -o name | grep -P "{regex}"'],
                capture_output=True,
                text=True,
                check=True,
            )
            snapshots = result.stdout.splitlines()
            snapshots = [_from_zfs_handle(n) for n in snapshots]
            return snapshots

        except subprocess.CalledProcessError as e:
            if e.stderr.strip() == "no datasets available":
                result: list[SnapshotRef] = []
                return result
            print(e.stderr, file=sys.stderr)
            raise

    @staticmethod
    def destroy(snapshot_ref: SnapshotRef):
        """Destroy a ZFS snapshot on the host system.

        Args:
            snapshot_ref: Reference to the snapshot to destroy

        Returns:
            SnapshotRef: Reference to the destroyed snapshot

        Raises:
            subprocess.CalledProcessError: If snapshot destruction fails
        """
        try:
            subprocess.run(
                ["bash", "-c", f"zfs destroy -d {snapshot_ref.zfs_handle}"],
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
             anchor: Optional[SnapshotRef] = None,
             zfs_flags: list[str] = ["-R"],
             blocksize: int = 4096):
        """Stream a ZFS snapshot, optionally as an increment from an anchor.

        Args:
            ref: Reference to the snapshot to send
            anchor: Optional reference to base the incremental snapshot on
            zfs_flags: Additional ZFS send flags
            blocksize: Size of data blocks to yield

        Returns:
            SnapshotStream: Iterator over the snapshot data

        Raises:
            DataIntegrityError: If anchor is newer than ref
            subprocess.CalledProcessError: If snapshot sending fails
            RuntimeError: If subprocess stdout cannot be opened
        """

        if (anchor is not None) and (anchor.date >= ref.date):
            raise DataIntegrityError(
                dedent("""
                    Cannot create incremental snapshots that link a newer snapshot to an older snapshot
                    Incremental snapshots most link an older snapshot to a new snapshot
                """))

        def _iterator():
            try:
                # build the command
                cmd = ["zfs", "send", *zfs_flags]
                if anchor is not None:
                    cmd.extend(["-i", anchor.zfs_handle.split("@")[1]])
                cmd.append(ref.zfs_handle)

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

        iterator = _iterator()
        stream = SnapshotStream(
            ref=ref,
            anchor=anchor,
            iterable=iterator,
        )
        return stream


class LineageTableNode(BaseModel):
    """Represents a node in a snapshot lineage table.
    
    A node can represent either a complete snapshot or an incremental snapshot,
    identified by its filename and date.

    Attributes:
        filename: Name of the snapshot file
        date: Date string of the snapshot
        node_type: Either 'complete' or 'incremental'
    """

    model_config = ConfigDict(frozen=True)

    filename: str
    date: str
    node_type: Literal['complete', 'incremental']

    def __eq__(self, other):
        return bool(self.date == other.date)

    def __hash__(self):
        return hash(self.filename)


class LineageTable(BaseModel):
    """Represents a table of snapshot lineages.
    
    The table is organized as a matrix where:
    - Columns represent different backup chains/lineages
    - Rows represent different dates
    - Each cell contains either None or a LineageTableNode
    - Complete nodes start new columns
    - Incremental nodes build upon previous nodes in the same column

    Example structure:
        Date1: [Complete, None,    None   ]
        Date2: [Inc1,    Complete, None   ]
        Date3: [Inc2,    Inc1,    Complete]

    Attributes:
        zfs_prefix: Path to the ZFS dataset
        index: Tuple of dates representing the row labels
        data: Matrix of LineageTableNodes organized by lineage and date
        orphaned_nodes: Nodes that don't belong to any valid lineage
    """

    model_config = ConfigDict(frozen=True)

    zfs_prefix: PurePath
    index: tuple[Date, ...]
    data: tuple[tuple[Optional[LineageTableNode], ...], ...]
    orphaned_nodes: tuple[LineageTableNode, ...]


class FileSnapshotManager:
    """Manages data generated from snapshots, handling both complete and 
    incremental snapshots. Complete snapshots contain all the data needed 
    for a full restore, while incremental snapshots only contain changes 
    since a previous snapshot."""

    @staticmethod
    def _compute_lineage_tables(filepaths: list[PurePath]):
        """Compute lineage tables from a list of snapshot file paths.

        Args:
            filepaths: List of paths to snapshot files

        Returns:
            list[LineageTable]: List of computed lineage tables

        Raises:
            DataCorruptionError: If invalid incremental snapshots are found
        """

        # predefine the patterns used to seperate out files
        complete_pattern = re.compile(r"\d{8}.zfs")
        incremental_pattern = re.compile(r"\d{8}_\d{8}.zfs")

        # build out lineages using the tips
        lineage_tables: list[LineageTable] = []

        # sorting the files_paths allows for a single forward pass
        filepaths = sorted(filepaths)
        for zfs_prefix, group_files in groupby(filepaths, key=lambda p: p.parent):
            zfs_prefix = SnapshotRef.format_zfs_prefix(zfs_prefix)

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
            table = LineageTable(zfs_prefix=zfs_prefix,
                                 index=table_index,
                                 data=table_data,
                                 orphaned_nodes=orphaned_nodes)

            # add the table to the master list
            lineage_tables.append(table)

        return lineage_tables

    @staticmethod
    def _compute_refs_from_lineage_tables(lineage_tables: list[LineageTable]):
        """Extract snapshot references from lineage tables.

        Args:
            lineage_tables: List of lineage tables to process

        Returns:
            list[SnapshotRef]: List of snapshot references
        """
        refs: list[LineageTableNode] = []
        for table in lineage_tables:
            for date in table.index:
                refs.append(SnapshotRef(date=date, zfs_prefix=table.zfs_prefix))
        return refs

    @classmethod
    def query(cls, adapter: SnapshotStorageAdapter, zfs_prefix: Optional[PurePath] = None):
        """Query snapshots using the provided adapter.

        Args:
            adapter: Adapter for accessing snapshot data
            zfs_prefix: Optional path to filter snapshots by dataset

        Returns:
            list[SnapshotRef]: List of snapshot references
        """
        if zfs_prefix is not None:
            zfs_prefix = SnapshotRef.format_zfs_prefix(zfs_prefix)
        matches = adapter.query(zfs_prefix)
        lineage_tables = cls._compute_lineage_tables(matches)
        refs = cls._compute_refs_from_lineage_tables(lineage_tables)
        return refs

    @staticmethod
    def _compute_redundent_nodes(table: LineageTable):
        """Finds all nodes that are redundent  in the table such that
        No lineages will be broken and no incremental snapshots will be orphaned.
        In the event that multiple paths through the table exist, the nodes
        that when removed minimizes the total number of complete nodes will be selected.
        """
        # convert to row major format and begin searching
        rows = tuple(map(tuple, zip(*table.data)))

        # we can always start at 1 because row zero will always have only a single comlete node in it
        redundent_nodes: list[LineageTableNode] = []
        for j in range(1, len(table.index)):
            i = j - 1
            nodes: list[LineageTableNode] = [n for n in rows[j] if n is not None]
            if len(nodes) > 1:
                # any complete nodes can be safely pruned if on a row with an incremental node
                complete_nodes = [n for n in nodes if n.node_type == 'complete']
                redundent_nodes.extend(complete_nodes)
                # for the incremental nodes, we preference nodes with a non-null previous index
                incremental_nodes = [(x, n) for x, n in enumerate(nodes) if n.node_type == 'incremental']
                if len(incremental_nodes) > 1:
                    for t in incremental_nodes:
                        if rows[i][t[0]] is None:
                            redundent_nodes.append(t[1])
        return redundent_nodes

    @staticmethod
    def _is_ref_deletable(table: LineageTable, ref: SnapshotRef):
        """Returns true if the ref is indeed deletable"""
        if table.zfs_prefix != ref.zfs_prefix:
            raise ValueError('zfs_prefix for LineageTable must match zfs_prefix for SnapshotRef')

        if ref.date == table.index[-1]:
            return True
        elif ref.date == table.index[0] and len(table.index) > 1:
            # there must be a complete node in row 1
            return bool(
                [c[1] for c in table.data if (c[1] is not None) and (c[1].node_type == 'complete')])
        return False

    @classmethod
    def destroy(cls, adapter: SnapshotStorageAdapter, ref: SnapshotRef):
        # query the files related to the ref
        matches = adapter.query(ref.zfs_prefix)
        tables = cls._compute_lineage_tables(matches)

        # if multiple lineages were found, something went wrong
        if len(tables) > 1:
            raise ValueError(f'Multiple lineage tables found when attempting to destroy ref {ref}. '
                             'Expected exactly one table.')

        # ensure ref is deletable
        table = tables[0]
        if not cls._is_ref_deletable(table, ref):
            raise DataIntegrityError(
                f'Cannot delete snapshot {ref} as it would break snapshot lineage. '
                'Only the most recent snapshot or oldest snapshot (with complete backup) '
                'can be deleted.')
        row_idx = table.index.index(ref.date)
        row = [col[row_idx] for col in table.data if col[row_idx] is not None]
        filepaths = [table.zfs_prefix.joinpath(v.filename) for v in row]
        success_flags = adapter.destroy(filepaths)
        return (filepaths, success_flags)

    @classmethod
    def prune(cls, adapter: SnapshotStorageAdapter, zfs_prefix: PurePath):
        zfs_prefix = SnapshotRef.format_zfs_prefix(zfs_prefix)
        paths = adapter.query(zfs_prefix)
        tables = cls._compute_lineage_tables(paths)
        filepaths, success_flags = ([], [])
        for table in tables:
            filepaths.extend([table.zfs_prefix.joinpath(n.filename) for n in table.orphaned_nodes])
            redundent_nodes = cls._compute_redundent_nodes(table)
            filepaths.extend([table.zfs_prefix.joinpath(n.filename) for n in redundent_nodes])
            success_flags.extend(adapter.destroy(filepaths))
        return (filepaths, success_flags)

    @staticmethod
    def recv(adapter: SnapshotStorageAdapter, stream: SnapshotStream):
        # validate that we won't be overwriting an existing file
        paths = adapter.query(stream.ref.zfs_prefix)
        filenames = [p.name for p in paths]
        if stream.filename in filenames:
            raise DataIntegrityError(f'Cannot overwrite existing file = {stream.filename}')

        # write it out using the adapter
        return adapter.recv(stream)

    @classmethod
    def send(cls,
             adapter: SnapshotStorageAdapter,
             ref: SnapshotRef,
             anchor: Optional[SnapshotRef] = None):
        """Generates SnapshotStream using the adapter"""
        # get the lineage table
        filepaths = adapter.query(ref.zfs_prefix)
        tables = cls._compute_lineage_tables(filepaths)
        if len(tables) > 1:
            raise ValueError(f'Multiple lineage tables found when attempting to send ref {ref}. '
                             'Expected exactly one table')
        table = tables[0]

        # use the table to find the lineage with the requisite ref dates
        if ref.date not in table.index:
            raise ValueError(f'Cannot find data for SnapshotRef = {ref}')
        elif (anchor is not None) and (anchor.date not in table.index):
            raise ValueError(f'Cannot find data for anchor SnapshotRef = {ref}')

        ref_idx = table.index.index(ref.date)
        anchor_idx = table.index.index(anchor.date) + 1 if anchor is not None else 0
        for col in table.data:
            # lineage is valid if root is not none and row is not None
            if (col[anchor_idx] is not None) and (col[ref_idx] is not None):
                filepaths = [
                    table.zfs_prefix.joinpath(n.filename)
                    for n in col[anchor_idx:ref_idx]
                    if n is not None
                ]
                iterable = adapter.send(filepaths)
                stream = SnapshotStream(ref=ref, anchor=anchor, iterable=iterable)
                return stream
            # if iteration completes without finding a match raise an exception
            raise ValueError(f'Unable to generate SnapshotStream for ref = {ref} and anchor = {anchor}')
