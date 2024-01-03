cmake_minimum_required(VERSION 3.7)

# ------------------------------------------------------------------------------
# Project settings
# ------------------------------------------------------------------------------

project(leanstoredb)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# Define custom build options
option(BUILD_WITH_COVERAGE "Build the library with code coverage" OFF)

option(THIRD_PARTY_SRC_DIR "The directory to store downloaded third-party sources" "${CMAKE_CURRENT_BINARY_DIR}/third-party-src")
if(NOT THIRD_PARTY_SRC_DIR)
  set(THIRD_PARTY_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/third-party-src)
endif()
message(STATUS "THIRD_PARTY_SRC_DIR: ${THIRD_PARTY_SRC_DIR}")


enable_language(ASM)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -rdynamic -fno-omit-frame-pointer -g3 -static-libstdc++")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunused -Wunreachable-code -fno-omit-frame-pointer")
if(BUILD_WITH_COVERAGE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -g -O0")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  add_compile_options(-Wall -Wextra -mavx2 -mcx16 -m64) # -fno-elide-constructors no need for now
else()
  add_compile_options(-Wall -Wextra -march=native)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3") # because we had weird behavior on aws ARM

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

include(${CMAKE_SOURCE_DIR}/third-party/tabluate.cmake)
include(${CMAKE_SOURCE_DIR}/third-party/rapidjson.cmake)
include(${CMAKE_SOURCE_DIR}/third-party/gflags.cmake)
include(${CMAKE_SOURCE_DIR}/third-party/glog.cmake)
include(${CMAKE_SOURCE_DIR}/third-party/gtest.cmake)
include(${CMAKE_SOURCE_DIR}/third-party/gbench.cmake)
include(${CMAKE_SOURCE_DIR}/third-party/prometheus-cpp.cmake)
include(${CMAKE_SOURCE_DIR}/third-party/onetbb.cmake)


# ------------------------------------------------------------------------------
# Project build targets
# ------------------------------------------------------------------------------

add_subdirectory(source)
add_subdirectory(benchmarks)

# tests
include(CTest)
add_subdirectory(tests)
