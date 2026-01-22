# ------------------------------------------------------------------------------
# Example for using the library
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.25)
project(leanstore-examples C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 23)

# Find leanstore package
# This requires CMAKE_PREFIX_PATH to include the leanstore installation directory
find_package(leanstore CONFIG REQUIRED)

# Rules to build kv_basic_example
add_executable(kv_basic_example kv_basic_example.c)

target_link_libraries(kv_basic_example PRIVATE
    leanstore::leanstore
    stdc++        # C++ standard library (required for C examples linking to C++ lib)
    m             # Math library
)