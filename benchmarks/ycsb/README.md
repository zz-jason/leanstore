# YCSB Benchmark

## Build With RocksDB

1. Add `rocksdb` to `$CMAKE_SOURCE_DIR/vcpkg.json`

2. Build the project:

```sh
cmake --preset=performance
cmake --build build/performance -j `nproc`
```

## Run YCSB

For convenience, a `ycsb-config.flags` file is provided to configure the YCSB
benchmark. You can modify the parameters in this file or override them with
command line arguments according to your needs

```sh
# load data
./build/performance/benchmarks/ycsb/ycsb -flagfile=benchmarks/ycsb/ycsb-config.flags -ycsb_cmd=load

# run benchmark
./build/performance/benchmarks/ycsb/ycsb -flagfile=benchmarks/ycsb/ycsb-config.flags -ycsb_cmd=run
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
