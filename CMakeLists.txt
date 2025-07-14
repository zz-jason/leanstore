# ------------------------------------------------------------------------------
# Project settings
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.10)
project(leanstoredb CXX)

if (NOT UNIX)
  message(SEND_ERROR "unsupported platform")
endif ()

enable_language(ASM)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wextra -rdynamic -fno-omit-frame-pointer -pthread -Wno-error=clobbered")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ftls-model=local-exec")
if (CMAKE_BUILD_TYPE MATCHES Debug)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEBUG -O0 -g3")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG -O3 -g")
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(-Wno-vla-cxx-extension)
endif()

# for clangd
add_compile_definitions(__cpp_concepts=202002L)

# platform specific flags
if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-mavx2 -mcx16 -m64)
else()
  add_compile_options(-march=native)
endif()

if(ENABLE_COVERAGE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
endif()

if(ENABLE_ASAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

if(ENABLE_TSAN)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
  add_compile_options(-fstandalone-debug)
endif ()

if (APPLE)
  LIST(APPEND CMAKE_PREFIX_PATH /usr/local/opt/bison)
  LIST(APPEND CMAKE_PREFIX_PATH /usr/local/opt/flex)
endif (APPLE)

if (CYGWIN)
  SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
endif (CYGWIN)


# ------------------------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------------------------

find_package(RapidJSON CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)
find_package(Crc32c CONFIG REQUIRED)
find_package(httplib CONFIG REQUIRED)
find_package(PkgConfig)
pkg_check_modules(libunwind REQUIRED IMPORTED_TARGET GLOBAL libunwind)

# ------------------------------------------------------------------------------
# Project build targets
# ------------------------------------------------------------------------------

add_subdirectory(src)
add_subdirectory(benchmarks)

# tests
include(CTest)
add_subdirectory(tests)


# ---------------------------------------------------------------------------
# clang-format
# ---------------------------------------------------------------------------

file(GLOB_RECURSE ALL_SOURCE_FILES
  "src/*.cpp" "src/*.hpp"
  "include/*.hpp"
  "tests/*.cpp" "tests/*.hpp"
  "benchmarks/*.cpp" "benchmarks/*.hpp")

# check-format
add_custom_target(check-format
  COMMAND clang-format --style=file --dry-run -Werror -i ${ALL_SOURCE_FILES}
  VERBATIM
)

# fix format
add_custom_target(format
  COMMAND clang-format --style=file -i ${ALL_SOURCE_FILES}
  VERBATIM
)


# ---------------------------------------------------------------------------
# clang-tidy
# ---------------------------------------------------------------------------

# clang-tidy
add_custom_target(check-tidy
    COMMAND clang-tidy -p=${CMAKE_BINARY_DIR} --config-file=${CMAKE_SOURCE_DIR}/.clang-tidy ${ALL_SOURCE_FILES} -extra-arg='--std=c++2b'
    COMMENT "Running Clang-Tidy"
    VERBATIM
)
