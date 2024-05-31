[![CI][9]][10]
[![CircleCI][1]][2]
[![codecov][3]][4]

# LeanStore

LeanStore is a larger-than-memory database, optimized for NVMe SSD and
multi-core CPU, achieves performance close to in-memory systems without having
to keep all data in memory.

<div align='center'>
  <img align="center" width=80% src="./docs/images/Architecture.jpg" />
</div>

## Getting started

It's recommanded to develop the project inside a docker container, which can be
built from this [Dockerfile][5]:

```sh
cmake --preset debug
cmake --build build/debug -j `nproc`
ctest --test-dir build/debug
```

## Contributing

Contributions are welcomed and greatly appreciated! See [CONTRIBUTING.md][6] for
setting up development environment and contributing.

## License

LeanStore is under the [MIT License][7].

## Acknowledgments

Thanks for the LeanStore authors and the [leanstore/leanstore][8] project.

[1]: https://dl.circleci.com/status-badge/img/circleci/MkFUq3aTNH5S7gLVEtwrGF/XCiiFNumkGdcD65tKp4EEy/tree/main.svg?style=shield&circle-token=28e7f69f9698ab8b805730e038065a9f54c29668
[2]: https://dl.circleci.com/status-badge/redirect/circleci/MkFUq3aTNH5S7gLVEtwrGF/XCiiFNumkGdcD65tKp4EEy/tree/main
[3]: https://codecov.io/github/zz-jason/leanstore/graph/badge.svg?token=MBS1H361JJ
[4]: https://codecov.io/github/zz-jason/leanstore
[5]: ./docker/Dockerfile
[6]: ./CONTRIBUTING.md
[7]: ./LICENSE
[8]: http://github.com/leanstore/leanstore
[9]: https://github.com/zz-jason/leanstore/actions/workflows/ci.yml/badge.svg
[10]: https://github.com/zz-jason/leanstore/actions/workflows/ci.yml
