# ------------------------------------------------------------------------------
# Project settings
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.10)
project(leanstoredb CXX)
enable_language(ASM)

if(NOT UNIX)
  message(SEND_ERROR "unsupported platform")
endif ()

set(LEANSTORE_CXX_STANDARD 23)

if(NOT DEFINED CMAKE_EXPORT_COMPILE_COMMANDS)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

# ------------------------------------------------------------------------------
# Compiler settings
# ------------------------------------------------------------------------------
set(LEANSTORE_COMPILE_DEFINITIONS __cpp_concepts=202002L) # for clangd
set(LEANSTORE_COMPILE_FEATURES cxx_std_23)
set(LEANSTORE_COMPILE_OPTIONS -Wall -Werror -Wextra -rdynamic -fno-omit-frame-pointer -pthread -Wno-error=clobbered)

if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -DDEBUG -O0 -g3)
else()
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -DNDEBUG -O3 -g)
endif()

# clang
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -Wno-vla-cxx-extension)
endif()

if(${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -fstandalone-debug)
endif ()

# platform specific flags
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -mavx2 -mcx16 -m64)
else()
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -march=native)
endif()

if(CYGWIN)
  SET(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -static-libstdc++)
endif (CYGWIN)

if(ENABLE_COVERAGE)
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} --coverage)
endif()

if(ENABLE_ASAN)
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -fsanitize=address)
endif()

if(ENABLE_TSAN)
  set(LEANSTORE_COMPILE_OPTIONS ${LEANSTORE_COMPILE_OPTIONS} -fsanitize=thread)
endif()


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

add_custom_target(check-tidy
    COMMAND clang-tidy -p=${CMAKE_BINARY_DIR} --config-file=${CMAKE_SOURCE_DIR}/.clang-tidy ${ALL_SOURCE_FILES} -extra-arg='--std=c++2b'
    COMMENT "Running Clang-Tidy"
    VERBATIM
)
