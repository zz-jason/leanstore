# ------------------------------------------------------------------------------
# Example for using the library
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.25)
project(leanstore-examples C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 23)

# Set the leanstore include and library directory
option(LEANSTORE_INCLUDE_DIR "Path to the leanstore include directory" "")
option(LEANSTORE_LIBRARY_DIR "Path to the leanstore library directory" "")
option(ENABLE_COVERAGE "Enable coverage" OFF)

find_package(Threads REQUIRED)

# Rules to build kv_basic_example
add_executable(kv_basic_example kv_basic_example.c)

target_include_directories(kv_basic_example PUBLIC ${LEANSTORE_INCLUDE_DIR})

if(ENABLE_COVERAGE)
    target_compile_options(kv_basic_example PRIVATE --coverage)
    target_link_options(kv_basic_example PRIVATE --coverage)
endif()

target_link_directories(kv_basic_example PRIVATE ${LEANSTORE_LIBRARY_DIR})
target_link_libraries(kv_basic_example PRIVATE
    leanstore
    aio           # Direct dependency for async I/O
    Threads::Threads  # System library (PUBLIC from leanstore)
    stdc++        # C++ standard library (required for C examples)
    m             # Math library
)
