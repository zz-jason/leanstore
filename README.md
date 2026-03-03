[![CI][9]][10]
[![codecov][3]][4]
[![Join Slack][11]][12]

# LeanStore

LeanStore is a larger-than-memory database, optimized for NVMe SSD and
multi-core CPU, achieves performance close to in-memory systems without having
to keep all data in memory.

<div align='center'>
  <img align="center" width=80% src="./docs/images/Architecture.jpg" />
</div>

## Getting started

[vcpkg][13] is used to manage third-party libraries, please install it before
building the project. It's highly recommended to develop the project inside a
docker container, which can be built from this [Dockerfile][5]:

```sh
cmake --preset debug_coro
cmake --build build/debug_coro -j `nproc`
ctest --test-dir build/debug_coro -j 4
```

## Integration Guide

LeanStore is built as a **static library** by default and requires **C++23**. The library provides full CMake package support via `find_package(leanstore)`.

### Quick Start

1. **Build and install LeanStore**:
   ```bash
   cmake --preset release_coro
   cmake --build build/release_coro -j $(nproc)
   cmake --install build/release_coro --prefix ./dist
   ```

2. **In your CMake project**:
   ```cmake
   cmake_minimum_required(VERSION 3.25)
   project(my_project CXX)
   set(CMAKE_CXX_STANDARD 23)
   
   list(APPEND CMAKE_PREFIX_PATH "/path/to/leanstore/dist")
   find_package(leanstore CONFIG REQUIRED)
   
   add_executable(my_target main.cpp)
   target_link_libraries(my_target PRIVATE leanstore::leanstore)
   ```

3. **Configure with vcpkg** (if using the same environment):
   ```bash
   cmake -B build -DCMAKE_PREFIX_PATH="/path/to/leanstore/dist;/path/to/vcpkg/installed/x64-linux"
   ```

### Alternative Integration Methods

- **FetchContent**: For direct source integration without installation (see examples).
- **pkg-config**: Legacy support for non-CMake projects.

### Build Presets

| Preset | Description |
|--------|-------------|
| `debug_coro` | Debug build with coroutine support |
| `release_coro` | Release build with coroutine support |

All presets build **static libraries** by default. Set `BUILD_SHARED_LIBS=ON` for shared libraries.

### Examples

Complete C and C++ examples are available in [`examples/c`](examples/c) and [`examples/cpp`](examples/cpp). For detailed dependency management and advanced usage, see [docs/cmake_integration_guide.md](docs/cmake_integration_guide.md).

## Contributing

Contributions are welcomed and greatly appreciated! See [docs/development.md](docs/development.md) for
comprehensive development guidelines, environment setup, and contribution workflow.

You can also join the [slack workspace][12] to discuss any questions or ideas.

## License

LeanStore is under the [MIT License][7].

## Acknowledgments

Thanks for the LeanStore authors and the [leanstore/leanstore][8] project.

[3]: https://codecov.io/github/zz-jason/leanstore/graph/badge.svg?token=MBS1H361JJ
[4]: https://codecov.io/github/zz-jason/leanstore
[5]: ./docker/Dockerfile
[6]: ./CONTRIBUTING.md
[7]: ./LICENSE
[8]: http://github.com/leanstore/leanstore
[9]: https://github.com/zz-jason/leanstore/actions/workflows/ci.yml/badge.svg
[10]: https://github.com/zz-jason/leanstore/actions/workflows/ci.yml
[11]: https://img.shields.io/badge/Join-Slack-blue.svg?logo=slack
[12]: https://join.slack.com/t/leanstoreworkspace/shared_invite/zt-2o69igywh-yTheoWxjYnD5j3bAFN34Qg
[13]: https://github.com/microsoft/vcpkg
