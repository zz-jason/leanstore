# LeanStore

![Architecture](./docs/images/Architecture.jpg)

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a
high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs.
Our goal is to achieve performance comparable to in-memory systems when the data
set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe
SSDs for large data sets. While LeanStore is currently a research prototype, we
hope to make it usable in production in the future.

## Getting started

> NOTE: Ubuntu 22.04 or newer and GCC 12 is required.

```sh
# install dependencies
sudo apt-get update
sudo apt-get install -y libaio-dev

# build
cmake -DCMAKE_BUILD_TYPE=Debug -B build -S .
cmake --build build -j `nproc`
ctest --test-dir build -j `nproc`
```

## Contributing

Contributions are welcomed and greatly appreciated! "good-first-issue" is a good
start point, see the [Contributing](./Contributing.md) file for details about the contributing
workflow, develop guide, and contributor communication etc.

## License

LeanStore is under the MIT License. See the [LICENSE](./LICENSE) file for
details.

## Acknowledgments

Thanks the LeanStore authors for the great
[leanstore](http://github.com/leanstore/leanstore) project and the idea behind
it.