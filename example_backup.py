#!./.venv/bin/python
import asyncio
from os import environ

import dotenv
import humanize
from tabulate import tabulate

from pyzync.otel import trace, with_tracer
from pyzync.backup import BackupJob, BackupJob
from pyzync.retention.policies.rolling import RollingNSnapshotsPolicy
from pyzync.storage.adapters.file import LocalFileStorageAdapter
from pyzync.storage.adapters.dropbox import DropboxStorageAdapter

tracer = trace.get_tracer(__name__)

# loads enviroment variables from .env file
dotenv.load_dotenv()

# define the adapter config
GB = 2**30  # 1 GB
file_adapter = LocalFileStorageAdapter(directory=environ['FILE_BACKUP_DIR'])
dbx_adapter = DropboxStorageAdapter(directory=environ['DROPBOX_DIR'],
                                    access_token=environ['DROPBOX_TOKEN'])



backup_config = {
    'tank1/foo':
        BackupJob(retention_policy=RollingNSnapshotsPolicy(n_snapshots=5),
                  adapters=[file_adapter, dbx_adapter])
}


# perform a rotate and run each job
@with_tracer(tracer)
async def main():
    rows = []
    for dataset_id, job in backup_config.items():
        job.rotate(dataset_id)
        stream_managers = await job.sync(dataset_id)
        headers = [
            'dataset', 'timestamp', 'producer sts', 'producer dur', 
            'consumer', 'consumers sts', 'consumers dur'
        ]
        for sm in stream_managers:
            rows.append([
                sm.producer.node.dataset_id, sm.producer.node.dt, sm.producer.exit_status,
                humanize.naturaldelta(sm.producer.total_seconds), 
                '\n'.join((c.generator.gi_code.co_qualname.split(".")[0] for c in sm.consumers)), 
                '\n'.join((c.exit_status for c in sm.consumers)), 
                '\n'.join((humanize.naturaldelta(c.total_seconds) for c in sm.consumers))
            ])
    print(tabulate(rows, headers=headers, tablefmt="grid"))


if __name__ == '__main__':
    asyncio.run(main())
