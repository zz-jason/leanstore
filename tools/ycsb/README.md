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
    --dir=/tmp/leanstore/ycsb \
    --key_size=16 \
    --val_size=200 \
    --rows=1000000 \
    --dram=1 \
    --duration=360000 \
    --backend=basickv \
    --workload=c \
    --workers=4 \
    --clients=4 \
    --action=load

# run benchmark
$LEANSTORE_HOME/dist/release_coro/bin/ycsb \
    --dir=/tmp/leanstore/ycsb \
    --key_size=16 \
    --val_size=200 \
    --rows=1000000 \
    --dram=1 \
    --duration=360000 \
    --backend=basickv \
    --workload=c \
    --workers=4 \
    --clients=4 \
    --action=run
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
