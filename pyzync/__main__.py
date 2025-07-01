import sys
import logging
import argparse
import importlib.util


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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = 'Pyzync',
        description = 'Makes zfs backup storage easy!',
        epilog = 'The code is always right, if the docs are wrong, consult the code.'
    )
    parser.add_argument(
        'filepath',
        type=str,
        help='Filepath to the job.py script'
    )
    args = parser.parse_args()
    script_path = args.filepath

    # Dynamically import backup_config from the given script_path
    spec = importlib.util.spec_from_file_location('job_module', script_path)
    job_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job_module)
    backup_config = getattr(job_module, 'backup_config')

    # perform a rotate and run each job
    for dataset_id, job in backup_config.items():
        job.rotate(dataset_id)
        job.sync(dataset_id)