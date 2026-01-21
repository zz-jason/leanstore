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
ctest --test-dir build/debug_coro
```

## Contributing

Contributions are welcomed and greatly appreciated! See [CONTRIBUTING.md][6] for
setting up development environment and contributing.

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
