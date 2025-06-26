cd /home/samuel/documents/pyzync;
/home/samuel/documents/.venv/bin/python -m pyzync >> backup.log 2>&1;
tail -n 300 backup.log > backup.tmp;
mv backup.tmp backup.log;