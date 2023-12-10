# ------------------------------------------------------------------------------
# Project settings
# ------------------------------------------------------------------------------

project(leanstoredb)
cmake_minimum_required(VERSION 3.7)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

enable_language(ASM)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -rdynamic -fno-omit-frame-pointer -g3 -static-libstdc++")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunused -Wunreachable-code")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
  ADD_COMPILE_OPTIONS(-Wall -Wextra -mavx2 -mcx16 -m64) # -fno-elide-constructors no need for now
else()
  ADD_COMPILE_OPTIONS(-Wall -Wextra -march=native)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3") # because we had weird behavior on aws ARM

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_BUILD_TYPE MATCHES Debug)
    ADD_COMPILE_OPTIONS(-fstandalone-debug)
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


# ------------------------------------------------------------------------------
# Project build targets
# ------------------------------------------------------------------------------

add_subdirectory(source)

# tests
include(CTest)
add_subdirectory(tests)
