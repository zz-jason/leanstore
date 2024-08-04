# Examples

## 1. Build the leanstore library

```sh
cd $LEANSTORE_HOME
cmake --preset=debug
cmake --build build/debug -j `nproc`
cmake --install build/debug
```

The leanstore library should be found in `$LEANSTORE/dist/debug` after the above commands.

## 2. Build the example

```sh
cd $LEANSTORE_HOME/examples/cpp

# generate build files with leanstore library
cmake -B build -S . \
      -DLEANSTORE_INCLUDE_DIR=$LEANSTORE_HOME/dist/debug/include \
      -DLEANSTORE_LIBRARY_DIR=$LEANSTORE_HOME/dist/debug/lib

# build the example
cmake --build build -j `nproc`

# run the example
./build/BasicKvExample
```