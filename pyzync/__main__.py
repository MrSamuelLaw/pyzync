import sys
import logging

from pyzync.backup import BackupJob, BackupConfig
from pyzync.retention_policies import LastNSnapshotsPolicy
from pyzync.storage_adapters import LocalFileSnapshotDataAdapter

if __name__ == '__main__':
    # setup the default logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Stdout handler set to INFO and above
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))
    root_logger.addHandler(stdout_handler)

    # Stderr handler set to ERROR
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))
    root_logger.addHandler(stderr_handler)

    # define the backup config for each job
    job = BackupJob(
        backup_configs = [
            BackupConfig(
                zfs_dataset_path='tank0/bar',
                retention_policy=LastNSnapshotsPolicy(n_snapshots=5),
                adapters=[
                    LocalFileSnapshotDataAdapter(
                        directory='/home/samuel/documents/pyzync/tests/test_backup_files'
                    )
                ]
            )
        ]
    )
    job.backup(on_duplicate_detected='error', force=False, dryrun=False)
