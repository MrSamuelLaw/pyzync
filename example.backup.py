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
        BackupJob(retention_policy=LastNSnapshotsPolicy(n_snapshots=5),
                  adapters=[
                      LocalFileStorageAdapter(directory='/home/path/to/your/backup/dir'),
                      DropboxStorageAdapter(directory='/dropbox/directory',
                                            access_token='your_access_token')
                  ]),
    'tank1/bar':
        BackupJob(retention_policy=LastNSnapshotsPolicy(n_snapshots=30),
                  adapters=[
                      DropboxStorageAdapter(directory='/dropbox/directory',
                                            access_token='your_access_token')
                  ]),
}
