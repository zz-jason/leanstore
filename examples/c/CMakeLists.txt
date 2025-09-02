# ------------------------------------------------------------------------------
# Example for using the library
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.10)
project(leanstore-examples C)

# Set gcc standard to c11
set(CMAKE_C_STANDARD 11)

# Set the leanstore include directory
option(LEANSTORE_INCLUDE_DIR "Path to the leanstore include directory" "")
message(STATUS "LEANSTORE_INCLUDE_DIR: ${LEANSTORE_INCLUDE_DIR}")

# Set the leanstore library directory, add it to the linker search path
option(LEANSTORE_LIBRARY_DIR "Path to the leanstore library directory" "")
message(STATUS "LEANSTORE_LIBRARY_DIR: ${LEANSTORE_LIBRARY_DIR}")
link_directories(${LEANSTORE_LIBRARY_DIR})

# Rules to build kv_basic_example
add_executable(kv_basic_example kv_basic_example.c)
target_link_libraries(kv_basic_example leanstore)
target_include_directories(kv_basic_example PUBLIC ${LEANSTORE_INCLUDE_DIR})
