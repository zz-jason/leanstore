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
cd $LEANSTORE_HOME/examples/c

# generate build files with leanstore library
cmake -B build -S . \
      -DLEANSTORE_INCLUDE_DIR=$LEANSTORE_HOME/dist/debug/include \
      -DLEANSTORE_LIBRARY_DIR=$LEANSTORE_HOME/dist/debug/lib

# build the example
cmake --build build -j `nproc`

# run the example
./build/BasicKvExample
```

Or you can directly build the example:

```sh
cd $LEANSTORE_HOME/examples/c

# build with leanstore library
gcc -o basickv-example BasicKvExample.c \
    -L$LEANSTORE_HOME/dist/debug/lib -lleanstore -lstdc++ \
    -I$LEANSTORE_HOME/dist/debug/include

# run with LD_LIBRARY_PATH set to the leanstore library path
LD_LIBRARY_PATH=$LEANSTORE_HOME/dist/debug/lib ./basickv-example
```