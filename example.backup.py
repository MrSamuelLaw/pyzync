import sys
import dotenv
import logging
from os import environ

from pyzync.backup import BackupJob, BackupJob
from pyzync.retention_policies import LastNSnapshotsPolicy
from pyzync.storage_adapters import LocalFileStorageAdapter, DropboxStorageAdapter

# setup the default logging
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Stdout handler set to INFO and above
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(name)s:%(message)s'))
root_logger.addHandler(stdout_handler)

# Stderr handler set to ERROR
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:%(name)s:%(message)s'))
root_logger.addHandler(stderr_handler)

# loads enviroment variables from .env file
dotenv.load_dotenv()

# define the backup config for each job
backup_config = {
    'tank1/foo':
        BackupJob(
            retention_policy=LastNSnapshotsPolicy(n_snapshots=5),
            adapters=[
                LocalFileStorageAdapter(directory=environ['BACKUP_DIR']),
                  DropboxStorageAdapter(directory=environ['DROPBOX_DIR'],
                                        access_token=environ['DROPBOX_TOKEN'])
            ])
}

# perform a rotate and run each job
for dataset_id, job in backup_config.items():
    job.rotate(dataset_id)
    job.sync(dataset_id)