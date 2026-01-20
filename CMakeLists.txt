# ------------------------------------------------------------------------------
# Project settings
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.25)
project(leanstoredb CXX)

# ------------------------------------------------------------------------------
# CMake build options
# ------------------------------------------------------------------------------

option(BUILD_SHARED_LIBS "Build using shared libraries" OFF)
option(LEAN_ENABLE_PROFILING "Enable profiling" OFF)
option(LEAN_ENABLE_PERF_COUNTERS "Whether to enable perf counters" ON)
option(LEAN_ENABLE_CORO "Enable coroutine support" ON)
option(LEAN_ENABLE_TESTS "Build tests" ON)
option(LEAN_ENABLE_YCSB "Build YCSB benchmark tool (requires WiredTiger and RocksDB)" OFF)
option(LEAN_ENABLE_DEBUG_EXEC "Enable debug block in debug build" ON)

# ------------------------------------------------------------------------------
# Interface library for compile options, definitions, warnings, and link options
# ------------------------------------------------------------------------------

add_library(lean_project_options INTERFACE)

# ------------------------------------------------------------------------------
# Platform specific settings
# ------------------------------------------------------------------------------

if (NOT UNIX)
  message(SEND_ERROR "unsupported platform")
endif ()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  target_compile_options(lean_project_options INTERFACE -mavx2 -mcx16 -m64)
else()
  target_compile_options(lean_project_options INTERFACE -march=native)
endif()

if(CYGWIN)
  target_link_options(lean_project_options INTERFACE -static-libstdc++)
endif()

# ------------------------------------------------------------------------------
# Compile features
# ------------------------------------------------------------------------------

target_compile_features(lean_project_options INTERFACE cxx_std_23)
set_target_properties(lean_project_options PROPERTIES
  CXX_STANDARD_REQUIRED ON
)

# ------------------------------------------------------------------------------
# Compile options
# ------------------------------------------------------------------------------

target_compile_options(lean_project_options INTERFACE
  -fno-omit-frame-pointer
  $<$<CONFIG:Debug>:-O0 -g3>
  $<$<CONFIG:Release>:-O3>
  $<$<AND:$<CXX_COMPILER_ID:Clang>,$<CONFIG:Debug>>:-fstandalone-debug>
)

if(LEAN_ENABLE_PERF_COUNTERS)
  target_compile_definitions(lean_project_options INTERFACE LEAN_ENABLE_PERF_COUNTERS)
endif()

if(LEAN_ENABLE_DEBUG_EXEC)
  target_compile_definitions(lean_project_options INTERFACE
    $<$<CONFIG:Debug>:LEAN_ENABLE_DEBUG_EXEC>
  )
endif()

if(LEAN_ENABLE_CORO)
  target_compile_definitions(lean_project_options INTERFACE LEAN_ENABLE_CORO)
endif()

# Coverage
if(ENABLE_COVERAGE)
  set(BUILD_SHARED_LIBS OFF)  # Coverage does not work with shared libs
  target_compile_options(lean_project_options INTERFACE --coverage -fprofile-update=atomic)
  target_link_options(lean_project_options INTERFACE --coverage)
endif()

# Sanitizers
if(ENABLE_ASAN)
  target_compile_options(lean_project_options INTERFACE -fsanitize=address)
  target_link_options(lean_project_options INTERFACE -fsanitize=address)
endif()

if(ENABLE_TSAN)
  target_compile_options(lean_project_options INTERFACE -fsanitize=thread)
  target_link_options(lean_project_options INTERFACE -fsanitize=thread)
endif()

# ------------------------------------------------------------------------------
# Compile definitions
# ------------------------------------------------------------------------------

target_compile_definitions(lean_project_options INTERFACE
  $<$<CONFIG:Debug>:DEBUG>
)

# ------------------------------------------------------------------------------
# Compile warnings
# ------------------------------------------------------------------------------

target_compile_options(lean_project_options INTERFACE
  -Wall
  -Wextra
  -Werror
  $<$<CXX_COMPILER_ID:Clang>:-Wno-vla-cxx-extension -Wno-unknown-warning-option>
  $<$<CXX_COMPILER_ID:GNU>:-Wno-error=clobbered>
)

# ------------------------------------------------------------------------------
# Base dependencies
# ------------------------------------------------------------------------------

find_package(benchmark CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)

# ---------------------------------------------------------------------------
# clang-format, clang-tidy
# ---------------------------------------------------------------------------

file(GLOB_RECURSE ALL_SOURCE_FILES
  "src/*.cpp" "src/*.hpp"
  "include/*.hpp"
  "tests/*.cpp" "tests/*.hpp"
  "tools/*.cpp" "tools/*.hpp")

# Exclude ycsb files from checks if YCSB is disabled
if(NOT LEAN_ENABLE_YCSB)
  list(FILTER ALL_SOURCE_FILES EXCLUDE REGEX ".*/tools/ycsb/.*")
endif()

add_custom_target(check-format
  COMMAND clang-format --style=file --dry-run -Werror -i ${ALL_SOURCE_FILES}
  VERBATIM
)

add_custom_target(format
  COMMAND clang-format --style=file -i ${ALL_SOURCE_FILES}
  VERBATIM
)

add_custom_target(check-tidy
    COMMAND run-clang-tidy
            -p=${CMAKE_BINARY_DIR}
            -config-file=${CMAKE_SOURCE_DIR}/.clang-tidy
            -j 32
            -quiet
            -extra-arg=-std=c++2b
            -extra-arg=-Wno-unknown-warning-option
            ${ALL_SOURCE_FILES}
    COMMENT "Running Clang-Tidy"
    VERBATIM
)

# ------------------------------------------------------------------------------
# Project libs, tools, and tests
# ------------------------------------------------------------------------------

add_subdirectory(src)
add_subdirectory(tools)

if(LEAN_ENABLE_TESTS)
  include(CTest)
  enable_testing()

  find_package(GTest CONFIG REQUIRED)
  include(GoogleTest)

  add_subdirectory(tests)
endif()
