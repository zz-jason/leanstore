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
  cppcheck --project=${build_dir}/compile_commands.json -i tests --error-exitcode=0
  cmake --build ${build_dir} -j `nproc`
  ctest --test-dir ${build_dir}
  popd
}

main() {
  build_type="Debug"
  build_dir="build/debug"
  buildAndTest ${build_type} ${build_dir}
}

main