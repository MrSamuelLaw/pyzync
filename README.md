# Pyzync

Pyzync is a Python-based tool for managing ZFS snapshot backups with pluggable retention policies and storage adapters. It is designed to help automate the creation, rotation, and synchronization of ZFS snapshots between a host and one or more backup locations (local or remote).

## Features
- Automated ZFS snapshot creation and rotation
- Pluggable retention policies (e.g., keep last N snapshots)
- Local file storage adapter (with support for custom adapters)
- Sync snapshots between host and backup storage
- Extensible and testable codebase

## Requirements
- Python 3.10+
- ZFS installed and available on the host system
- [pydantic](https://pydantic.dev/) (for data validation)

## Installation
Clone the repository and install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### 1. Configure a Backup Job
Edit or create a Python script (or use `pyzync/pyzync/__main__.py` as a template):

```python
from pyzync.backup import BackupJob
from pyzync.retention_policies import LastNSnapshotsPolicy
from pyzync.storage_adapters import LocalFileStorageAdapter

config = {
    'tank0/foo': BackupJob(
        retention_policy=LastNSnapshotsPolicy(n_snapshots=5),
        adapters=[LocalFileStorageAdapter(directory='/path/to/backup/directory')]
    )
}

for dataset_id, job in config.items():
    job.rotate(dataset_id)
    job.sync(dataset_id)
```

- Replace `'tank0/foo'` with your ZFS dataset name.
- Set the backup directory to a location with sufficient space.

### 2. Run the Backup
You can run the backup job from the command line:

```bash
python -m pyzync
```

Or run your custom script as needed.

### 3. Scheduling Backups with Cron

To automate your ZFS snapshot backups, you can schedule your backup script to run at regular intervals using cron.

1. Open your crontab for editing:

```bash
crontab -e
```

2. Add a line to schedule your backup. For example, to run the backup every day at 2:00 AM:

```
0 2 * * * /usr/bin/python3 /path/to/your/backup_script.py >> /var/log/pyzync_backup.log 2>&1
```

- Adjust the schedule and paths as needed.
- Make sure the script has the correct permissions and environment (e.g., ZFS and Python available).
- Redirect output to a log file for troubleshooting.

For more information on cron syntax, see [crontab.guru](https://crontab.guru/).

## Setting Up an Ubuntu VM for Testing

To test Pyzync in a safe environment, you can use an Ubuntu virtual machine. Below are the steps to set up ZFS and allow non-root users to run ZFS commands:

### 1. Create and Start an Ubuntu VM
- Use your preferred virtualization tool (e.g., VirtualBox, VMware, or KVM) to create a new Ubuntu VM.
- Allocate at least 2GB RAM and 20GB disk space.

### 2. Install ZFS
```bash
sudo apt update
sudo apt install -y zfsutils-linux
```

### 3. Add Your User to the `sudo` and `zfs` Groups
```bash
sudo usermod -aG sudo $USER
sudo groupadd zfs
sudo usermod -aG zfs $USER
```
Log out and log back in for group changes to take effect.

### 4. Allow Non-root Users to Run ZFS Commands
Set the sticky bit on the ZFS binaries so users in the `zfs` group can run them without sudo:
```bash
sudo chgrp zfs /sbin/zfs /sbin/zpool
sudo chmod 750 /sbin/zfs /sbin/zpool
sudo chmod g+s /sbin/zfs /sbin/zpool
```

### 5. (Optional) Create a Test ZFS Pool
You can create a ZFS pool using a loopback file for safe testing:

```bash
fallocate -l 2G /tmp/zfs_test.img
sudo zpool create testpool /tmp/zfs_test.img
```

**Explanation:**
- `fallocate -l 2G /tmp/zfs_test.img` creates a 2GB file on your disk. This file will act as a virtual disk for ZFS, so you don't need to use a real disk or partition.
- `sudo zpool create testpool /tmp/zfs_test.img` creates a new ZFS pool named `testpool` using the file as its storage device. This is useful for testing and development, as it is non-destructive and can be easily removed.
- You can safely experiment with ZFS commands and Pyzync on this pool without affecting your real data or disks.

### 6. Verify Permissions
Switch to your user and run:
```bash
zfs list
zpool status
```
You should be able to run these commands without `sudo`.

## Testing
Run the test suite with:

```bash
python -m unittest discover tests
```

## Project Structure
- `pyzync/` — Main library code
- `tests/` — Unit tests and test data
- `dev/` — Diagrams and development notes

## Extending
You can add new retention policies or storage adapters by subclassing the appropriate base classes in `pyzync/retention_policies.py` and `pyzync/storage_adapters.py`.

## License
MIT License
