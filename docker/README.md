## LeanStore Build Image

This is the Dockerfile to construct the build image for LeanStore.

## How to use

Build and update image in dockerhub:

```sh
docker build --tag zzjason/leanstore-dev:latest .
docker push zzjason/leanstore-dev:latest
```

Build leanstore inside a container:

```sh
# Run a container based on the image:
export LEANSTORE_HOME=/path/to/leanstore/on/host
docker run -it --privileged --network=host -v ${LEANSTORE_HOME}:/root/code/leanstore zzjason/leanstore-dev:latest bash

# Build and test LeanStore in the running container:
cd /root/code/leanstore
cmake --preset debug
cmake --build build/debug -j `nproc`
ctest --test-dir build/debug
```

