## Get Started

### Setup VS Code and Docker development environment

LeanStore requires libaio, c++23. It's recommended to develop the project inside
a docker container for convenience, which can be built from this [Dockerfile][0].

For VS Code, it's recommended to install the ["C/C++ Extension Pack"][1] and
["clangd"][2] plugins.

### Get the code, build and run

```sh
git clone git@github.com:zz-jason/leanstore.git
cd leanstore
```

[vcpkg][3] is used to manage dependencies, all the dependencies can be found in
[vcpkg.json][5]. [CMakePresets.json][4] is used to manage common build configs.
You can choose one of these cmake presets when open the project in VS Code. To
compile LeanStore in debug mode:

```sh
cmake --preset debug
cmake --build build/debug
```

To run unittests in the debug mode:

```sh
ctest --test-dir build/debug
```

To check and fix code formatting with `clang-format-18`:

```sh
# check format
cmake --build build/debug --target=check-format

# fix format
cmake --build build/debug --target=format
```

To run simple ycsb benchmarks:

```sh
./build/debug/benchmarks/ycsb/ycsb \
  --ycsb_threads=8 \
  --ycsb_record_count=100000 \
  --ycsb_workload=c \
  --ycsb_run_for_seconds=600 \
  --ycsb_target=basickv
```

### Commit and submit a pull request

[Conventional Commits][6] is used for pull request titles, all the available
types can be found in [conventional-commits.yml][7].

[0]: ./docker/Dockerfile
[1]: https://marketplace.visualstudio.com/items?itemName=ms-vscode.cpptools-extension-pack
[2]: https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd
[3]: https://github.com/microsoft/vcpkg
[4]: ./CMakePresets.json
[5]: ./vcpkg.json
[6]: https://www.conventionalcommits.org/en/v1.0.0/
[7]: ./.github/workflows/conventional-commits.yml