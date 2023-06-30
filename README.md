# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a
high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs.
Our goal is to achieve performance comparable to in-memory systems when the data
set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe
SSDs for large data sets. While LeanStore is currently a research prototype, we
hope to make it usable in production in the future.

## Build and Run

Ubuntu 12.04 or newer is required. Install the following packages:

```sh
sudo apt-get update
sudo apt-get install -y libaio-dev libtbb-dev
```

Build the project with CMake:

```sh
cmake -DCMAKE_BUILD_TYPE=Debug -B build -S .
cmake --build build -j `nproc`
```

## TPC-C Example

```sh
touch ssd_block_device_or_file
build/frontend/tpcc \
    --ssd_path=./ssd_block_device_or_file \
    --worker_threads=4 \
    --pp_threads=2 \
    --dram_gib=4 \
    --tpcc_warehouse_count=1 \
    --notpcc_warehouse_affinity \
    --csv_path=./log \
    --free_pct=1 \
    --contention_split \
    --xmerge \
    --print_tx_console \
    --run_for_seconds=30 \
    --isolation_level=si
```

Get help with `build/frontend/tpcc --help`.