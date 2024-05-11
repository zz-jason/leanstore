<!--
[![CI](https://github.com/zz-jason/leanstore/actions/workflows/c-cpp.yml/badge.svg)](https://github.com/zz-jason/leanstore/actions/workflows/c-cpp.yml)
-->
[![CircleCI](https://dl.circleci.com/status-badge/img/circleci/MkFUq3aTNH5S7gLVEtwrGF/XCiiFNumkGdcD65tKp4EEy/tree/master.svg?style=shield&circle-token=28e7f69f9698ab8b805730e038065a9f54c29668)](https://dl.circleci.com/status-badge/redirect/circleci/MkFUq3aTNH5S7gLVEtwrGF/XCiiFNumkGdcD65tKp4EEy/tree/master)
[![codecov](https://codecov.io/github/zz-jason/leanstore/graph/badge.svg?token=MBS1H361JJ)](https://codecov.io/github/zz-jason/leanstore)

# LeanStore

LeanStore is a larger-than-memory database, optimized for NVMe SSD and multi-core CPU, achieves performance close to in-memory systems without having to keep all data in memory.

<div align='center'>
<img align="center" width=80% src="./docs/images/Architecture.jpg" />
</div>

## Getting started

It's recommanded to develop the project inside a docker container, which can be
built from the [Dockerfile](./docker/Dockerfile):

```sh
# build and test
cmake --preset debug
cmake --build build/debug -j `nproc`
ctest --test-dir build/debug
```

## Contributing

Contributions are welcomed and greatly appreciated! "good-first-issue" is a good
start point, see the [Contributing](./Contributing.md) file for details about
the contributing workflow, develop guide, and contributor communication etc.

## License

LeanStore is under the MIT License. See the [LICENSE](./LICENSE) file for
details.

## Acknowledgments

Thanks the LeanStore authors for the great
[leanstore](http://github.com/leanstore/leanstore) project and ideas behind it.
