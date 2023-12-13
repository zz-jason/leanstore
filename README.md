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
sudo apt-get install -y libaio-dev
```

Build the project with CMake:

```sh
cmake -DCMAKE_BUILD_TYPE=Debug -B build -S .
cmake --build build -j `nproc`
ctest --test-dir build -j `nproc`
```
