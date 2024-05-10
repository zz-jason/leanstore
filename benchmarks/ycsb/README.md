# YCSB Benchmark

## Build With RocksDB

Add a `rocksdb` dependency on `vcpkg.json` and recompile the project:

```sh
cmake --preset=performance_profile
cmake --build build/release -j `nproc`
```

## Run

```sh
./build/release/benchmarks/ycsb/ycsb \
  --ycsb_threads=8 \
  --ycsb_record_count=100000 \
  --ycsb_workload=c \
  --ycsb_run_for_seconds=600 \
  --ycsb_target=basickv
```

## Profile

Get the cpu profile:

```sh
curl -G "127.0.0.1:8080/profile" > cpu.prof
```

View the profile result:

```sh
pprof -http 0.0.0.0:4000 build/release/benchmarks/ycsb/ycsb cpu.prof
```

Then open the browser at **127.0.0.1:4000** to view the cpu profile.
