from pyzync.backup import BackupConfig, backup
from pyzync.retention_polcies import RollingNDaysPolicy
from pyzync.storage_adapters import LocalFileSnapshotDataAdapter

if __name__ == '__main__':
    backup_configs = [
        BackupConfig(zfs_dataset_path='tank0/foo',
                     retention_policy=RollingNDaysPolicy(n_days=5),
                     storage_adapters=[
                         LocalFileSnapshotDataAdapter(
                             directory='/home/samuel/documents/pyzync/tests/test_sync_files')
                     ]),
        BackupConfig(zfs_dataset_path='tank0/bar',
                     retention_policy=RollingNDaysPolicy(n_days=2),
                     storage_adapters=[
                         LocalFileSnapshotDataAdapter(
                             directory='/home/samuel/documents/pyzync/tests/test_sync_files')
                     ])
    ]

    backup(backup_configs)
