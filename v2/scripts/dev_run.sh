#! /bin/bash

rake debug

kill_port() {
    port=$1
    pid=$(lsof -t -sTCP:LISTEN  -i:${port})
    if [ -n "${pid}" ]; then
        kill ${pid}
    fi
}

rm -rf *.log
cd debug

mkdir -p log_data

kill_port 2111
./logdb_v2 --dir log_data -p 2111 --max_val 1024 --loglevel 0 --logfile logdb_v2.log --pidfile pid >> logdb_v2.log 2>&1  &


cd ../scripts
python run_tests.py