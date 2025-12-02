# YCSB Benchmark

## Build && install

```sh
export LEANSTORE_HOME=`pwd`
cmake --preset=release_coro
cmake --build build/release_coro -j `nproc`
cmake --install build/release_coro
```

## Run YCSB

```sh
export LD_LIBRARY_PATH=$LEANSTORE_HOME/dist/release_coro/lib:$LD_LIBRARY_PATH

# load data
$LEANSTORE_HOME/dist/release_coro/bin/ycsb \
    --data_dir=/tmp/leanstore/ycsb \
    --key_size=16 \
    --val_size=200 \
    --record_count=1000000 \
    --mem_gb=1 \
    --run_for_seconds=360000 \
    --target=basickv \
    --workload=c \
    --threads=4 \
    --clients=4 \
    --cmd=load

# run benchmark
$LEANSTORE_HOME/dist/release_coro/bin/ycsb \
    --data_dir=/tmp/leanstore/ycsb \
    --key_size=16 \
    --val_size=200 \
    --record_count=1000000 \
    --mem_gb=1 \
    --run_for_seconds=360000 \
    --target=basickv \
    --workload=c \
    --threads=4 \
    --clients=4 \
    --cmd=run
```

## Profile

Get the cpu profile:

```sh
curl -G "127.0.0.1:8080/profile" > cpu.prof
```

View the profile result:

```sh
pprof -http 0.0.0.0:4000 $LEANSTORE_HOME/dist/release_coro/bin/ycsb cpu.prof
```

Then open the browser at **127.0.0.1:4000** to view the cpu profile.
