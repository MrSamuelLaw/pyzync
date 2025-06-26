import sys
import logging

from pyzync.backup import BackupJob, BackupJob
from pyzync.retention_policies import LastNSnapshotsPolicy
from pyzync.storage_adapters import LocalFileStorageAdapter

if __name__ == '__main__':
    # setup the default logging
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

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
    config = {
        'tank0/foo': BackupJob(
            retention_policy=LastNSnapshotsPolicy(n_snapshots=5),
            adapters=[LocalFileStorageAdapter(directory='/home/samuel/documents/pyzync/tests/test_backup_files')]
        )
    }
    
    # perform a rotate and run each job
    for dataset_id, job in config.items():
        job.rotate(dataset_id)
        job.sync(dataset_id)