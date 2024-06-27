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
cd $LEANSTORE_HOME/examples
cmake -B build -S . -DLEANSTORE_INCLUDE_DIR=$LEANSTORE_HOME/dist/debug/include -DLEANSTORE_LIBRARY_DIR=$LEANSTORE_HOME/dist/debug/lib
cmake --build build -j `nproc`
./build/BasicKvExample
```