# ------------------------------------------------------------------------------
# Example for using the library
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.10)
project(leanstore-examples CXX)

# Set gcc standard to c++23
set(CMAKE_CXX_STANDARD 23)

# Set the leanstore include directory
option(LEANSTORE_INCLUDE_DIR "Path to the leanstore include directory" "")
message(STATUS "LEANSTORE_INCLUDE_DIR: ${LEANSTORE_INCLUDE_DIR}")

# Set the leanstore library directory, add it to the linker search path
option(LEANSTORE_LIBRARY_DIR "Path to the leanstore library directory" "")
message(STATUS "LEANSTORE_LIBRARY_DIR: ${LEANSTORE_LIBRARY_DIR}")
link_directories(${LEANSTORE_LIBRARY_DIR})

# Rules to build BasicKvExample
add_executable(BasicKvExample BasicKvExample.cpp)
target_link_libraries(BasicKvExample leanstore)
target_include_directories(BasicKvExample PUBLIC ${LEANSTORE_INCLUDE_DIR})
