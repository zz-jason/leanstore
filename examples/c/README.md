# C Examples

## 1. Build the leanstore library

```sh
cd $LEANSTORE_HOME
cmake --preset=debug_coro
cmake --build build/debug_coro -j `nproc`
cmake --install build/debug_coro
```

The leanstore library should be found in `$LEANSTORE_HOME/dist/debug_coro` after the above commands.

## 2. Build the example

```sh
cd $LEANSTORE_HOME/examples/c

# generate build files with leanstore library
# Note: leanstore depends on several libraries (boost, spdlog, rapidjson, etc.)
# It is recommended to use the same CMAKE_TOOLCHAIN_FILE used to build leanstore.
cmake -B build -S . \
      -DCMAKE_TOOLCHAIN_FILE=$LEANSTORE_HOME/vcpkg/scripts/buildsystems/vcpkg.cmake \
      -DLEANSTORE_INCLUDE_DIR=$LEANSTORE_HOME/dist/debug_coro/include \
      -DLEANSTORE_LIBRARY_DIR=$LEANSTORE_HOME/dist/debug_coro/lib

# build the example
cmake --build build -j `nproc`

# run the example
./build/kv_basic_example
```

