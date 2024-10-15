
import os

RECORD_SIZE = 100000
def do_load (tablename, num_tables):
    os.system(f"""
./ycsb \
    -load \
    -db rocksdb \
    -P workloads/workloada \
    -P rocksdb/rocksdb.properties \
    -p recordcount={RECORD_SIZE} \
    -p fieldcount=16 \
    -p fieldlength=10 \
    -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data \
    -p rocksdb.num_cf={num_tables} \
    -p table={tablename} \
        -p rocksdb.compression_per_level="{':'.join(['kSnappyCompression']* num_tables)}" \
        -p read_rate_limits="{','.join(['1000'] * num_tables)}" \
        -p io_read_capacity_kbps={420*1024} \
        -p rocksdb.num_levels="{':'.join(['4'] * num_tables)}" \
        -p rocksdb.max_write_buffer_number="{':'.join(['2'] * num_tables)}" \
        -p rocksdb.min_write_buffer_number_to_merge="{':'.join(['1'] * num_tables)}" \
        -p rocksdb.cache_size="{':'.join(['0'] * num_tables)}" \
        -p rocksdb.write_buffer_size="{':'.join(['67108864'] * num_tables)}" \
    -s >/dev/null
""")

import tempfile
def do_run (num_tables):
    os.system(f"""
./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
        -p rocksdb.dbname=/mnt/rocksdb-disk/ycsb-rocksdb-data \
        -p requestdistribution=uniform \
        -s -p operationcount=10000 \
        -p rocksdb.num_cf={num_tables} \
        -p recordcount={RECORD_SIZE} \
        -threads {num_tables} \
        -target_rates "{','.join(['1000'] * num_tables)}" \
        -p read_rate_limits="{','.join(['1000'] * num_tables)}" \
        -p rocksdb.num_levels="{':'.join(['4'] * num_tables)}" \
        -p rocksdb.max_write_buffer_number="{':'.join(['2'] * num_tables)}" \
        -p rocksdb.min_write_buffer_number_to_merge="{':'.join(['1'] * num_tables)}" \
        -p rocksdb.cache_size="{':'.join(['0'] * num_tables)}" \
        -p rocksdb.write_buffer_size="{':'.join(['67108864'] * num_tables)}" >/tmp/out.txt
""")
    
    with open("/tmp/out.txt", "r") as output:
        compaction_times = []
        for line in output.readlines():
            if "Synchronized manual compaction time elapsed:" in line:
                compaction_times.append(int(line.split("[BALI LOG] Synchronized manual compaction time elapsed: ")[-1].strip()))
    return compaction_times

def do (n):
    os.system("rm -rf /mnt/rocksdb-disk/ycsb-rocksdb-data")
    tables = ["default"]
    for i in range (2, n+1):
        tables.append(f"cf{i}")
    
    for table in tables:
        do_load(table, n)
    
    return do_run(n)

for i in range (2,10):
    print(i, do(i))
