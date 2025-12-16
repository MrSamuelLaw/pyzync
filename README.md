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
- uv install and available on the host system

## Installation
Clone the repository and install dependencies:

```bash
uv sync
```

## Usage

### 1. Configure a Backup Job
Edit or create a Python script (or use `pyzync/example_backup.py` as a template):

- Replace `'tank1/foo' and 'tank1/bar` with your ZFS dataset name.
- Set the backup directory to a location with sufficient space.

### 2. Run the Backup
You can run backup.py from the command line:

```bash
run run -m backup
```

### 3. Scheduling Backups with Cron

To automate your ZFS snapshot backups, you can schedule your backup script to run at regular intervals using cron.

1. Open your crontab for editing:

```bash
crontab -e
```

2. Add a line to schedule your backup. For example, to run the backup every day at 2:00 AM:

```
0 2 * * * /usr/bin/python3 /path/to/your/backup.py >> /path/to/your/backup.log 2>&1
```

- Adjust the schedule and paths as needed.
- Make sure the script has the correct permissions and environment (e.g., ZFS and Python available).
- Redirect output to a log file for troubleshooting.

For more information on cron syntax, see [crontab.guru](https://crontab.guru/).

### 4. Restoring from Backups with example_restore.py

The `example_restore.py` script provides an interactive CLI for restoring ZFS snapshots from remote backups. It guides you through the entire restore process step by step.

**Workflow:**
1. **Select Adapter** — Choose where your backups are stored (e.g., Dropbox or local files)
2. **Query Backups** — Retrieve all available snapshot graphs from the backup location
3. **Select Dataset** — Pick which ZFS dataset to restore
4. **Select Snapshot Chain** — Choose from available backup chains for that dataset
5. **Select Restore Target** — Pick a specific snapshot to restore up to (allows partial restore)
6. **Confirm** — Review the restore summary and confirm the operation
7. **Execute** — Stream data from the remote backup and restore snapshots to your system

**Running the restore:**

```bash
uv run -m example_restore
```

The script will prompt you interactively for each step. It displays snapshots as either `complete` (full backup) or `incremental` (delta since previous snapshot). You can restore up to any snapshot in the chain—all required snapshots will be restored in order.

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

### 3. Allow Non-root Users to Run ZFS Commands
Set the sticky bit on the ZFS binaries so users can run them without sudo:
```bash
sudo chmod u+s /sbin/zfs /sbin/zpool
```

### 4. Create a Test ZFS Pool
You can create a ZFS pool using a loopback file for safe testing:

```bash
fallocate -l 2G /tmp/zfs_test.img
sudo zpool create tank0 /tmp/zfs_test.img
```

**Explanation:**
- `fallocate -l 2G /tmp/zfs_test.img` creates a 2GB file on your disk. This file will act as a virtual disk for ZFS, so you don't need to use a real disk or partition.
- `sudo zpool create tank0 /tmp/zfs_test.img` creates a new ZFS pool named `tank0` using the file as its storage device. This is useful for testing and development, as it is non-destructive and can be easily removed.
- You can safely experiment with ZFS commands and Pyzync on this pool without affecting your real data or disks.  

Create the zfs filesystems needed for testing

```bash
sudo zfs create tank0/foo
sudo zfs create tank0/bar
```

### 5. Allow Your User to Create Files in the Dataset

By default, ZFS datasets are owned by root and only writable by the owner. To allow your user to create files, set your user as the owner and the group as needed on the mountpoint.

```bash
sudo chown -R $USER /tank0
```

- `chown -R $USER` sets your user as the owner and `zfs` as the group.
- Adjust the group as needed for your use case.

If you want all users to be able to create files, use `chmod 777` (less secure).

### 6. Verify Permissions
Switch to your user and run:
```bash
zfs list
zpool status
zfs snapshot tank0@test
touch tank0/foo/test.txt
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

## Extending
You can add new retention policies or storage adapters by subclassing the appropriate base classes in `pyzync/retention/interfaces.py` and `pyzync/storage/interfaces.py`.

## License
MIT License
