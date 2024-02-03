## LeanStore Build Image

This is the Dockerfile to construct the build image for LeanStore.

## How to use

Build the docker image:

```sh
docker build -t leanstore-dev .
```

Run a container based on the image:

```sh
docker run -it --privileged -v /path/to/leanstore/on/host:/path/to/leanstore/on/container leanstore-dev bash
```

Build and test LeanStore in the running container:

```sh
cd /path/to/leanstore/on/container
cmake -B build -S .
cmake --build build -j `nproc`
ctest --test-dir build
```
