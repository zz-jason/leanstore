# ------------------------------------------------------------------------------
# Example for using the library
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.25)
project(leanstore-examples CXX)

set(CMAKE_CXX_STANDARD 23)

# Find leanstore package
# This requires CMAKE_PREFIX_PATH to include the leanstore installation directory
find_package(leanstore CONFIG REQUIRED)

# Rules to build basic_kv_example
add_executable(basic_kv_example basic_kv_example.cpp)

target_link_libraries(basic_kv_example PRIVATE leanstore::leanstore)