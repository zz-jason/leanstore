#!/bin/bash

set -uvx

# global variables
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=${SCRIPT_DIR}/..

# build and test
buildAndTest() {
  build_type=$1
  build_dir=$2
  pushd ${PROJECT_DIR}
  cmake -B ${build_dir} -S . -DCMAKE_BUILD_TYPE=${build_type}
  cppcheck --project=${build_dir}/compile_commands.json -i tests --enable=all --error-exitcode=0
  cmake --build ${build_dir} -j `nproc`
  ctest --test-dir ${build_dir}
  gcovr -v -r . --html-details --output coverage/ --exclude 'build/*' --exclude 'tests/*' --exclude 'benchmarks/*'
  # to view the html report:  python3 -m http.server --directory coverage
  popd
}

main() {
  build_type="Debug"
  build_dir="build/debug"
  buildAndTest ${build_type} ${build_dir}
}

main
