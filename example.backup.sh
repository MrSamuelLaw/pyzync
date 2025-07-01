cd /into/folder/where/pyzync/is/installed
/path/to/venv/python/binary -m pyzync >> backup.log 2>&1;
tail -n 300 backup.log > backup.tmp;
mv backup.tmp backup.log;