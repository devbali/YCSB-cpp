#!/bin/bash

NUM_TABLES=4
rm -rf /mnt/rocksdb-disk/ycsb-rocksdb-data



do_load () {
        
        for ((i=0; i<5; i++)); do test+="----"; done
        ./ycsb -load -db rocksdb
}

./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES
./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf2
./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf3
./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf4
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf5
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf6
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf7
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf8
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf9
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf10
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf11
# ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p recordcount=30000 -p fieldcount=16 -p fieldlength=1024 -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data -s -p rocksdb.num_cf=$NUM_TABLES -p table=cf12

CACHE_SIZE=10000
./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
        -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data \
        -p requestdistribution=uniform \
        -s -p operationcount=10000 \
        -p rocksdb.num_cf=$NUM_TABLES \
        -p recordcount=1000 \
        -threads $NUM_TABLES \
        -target_rates "1000,1000,1000,1000" \
        -p read_rate_limits="1000,1000,1000,1000,1000" \
        -p io_read_capacity_kbps=$((420 * 1024)) \
            | tee status_thread.txt &
    
    ycsb_pid=$!
    echo "YCSB pid: ${ycsb_pid}"
    # Wait for the ycsb process to finish
    wait $ycsb_pid
