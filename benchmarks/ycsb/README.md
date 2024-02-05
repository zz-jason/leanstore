# YCSB Benchmark

## Run

```sh
CPUPROFILE=ycsb.cpu.profile CPUPROFILESIGNAL=12 \
HEAPPROFILE=ycsb.heap.profile HEAPPROFILESIGNAL=13 \
./build/release/benchmarks/ycsb/ycsb \
  --worker_threads=8 \
  --ycsb_record_count=100000 \
  --ycsb_workload=c \
  --ycsb_run_for_seconds=600 \
  --ycsb_target=leanstore
```

## Profile

Get the cpu profile:

```sh
kill -12 <ycsb_pid> # start sampling
sleep 30
kill -12 <ycsb_pid> # stop sampling
```

Get the heap profile:

```sh
kill -13 <ycsb_pid> # get the heap profiling
```

View the profile result:

```sh
pprof -http 0.0.0.0:10080 </path/to/ycsb> </path/to/profile>
```

Then open the browser at **127.0.0.1:10080** to view the cpu/heap profile.

## Run With RocksDB

Compiling rocksdb takes is too slow, benchmark on rocksdb is not supported by
default. To enable it, you need to add a `rocksdb` dependency on `vcpkg.json`
and use `YcsbRocksDb` in `Ycsb.cpp`, and recompiling the project.