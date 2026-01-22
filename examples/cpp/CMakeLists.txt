# ------------------------------------------------------------------------------
# Example for using the library
# ------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.25)
project(leanstore-examples CXX)

set(CMAKE_CXX_STANDARD 23)

# Set the leanstore include and library directory
option(LEANSTORE_INCLUDE_DIR "Path to the leanstore include directory" "")
option(LEANSTORE_LIBRARY_DIR "Path to the leanstore library directory" "")
option(ENABLE_COVERAGE "Enable coverage" OFF)

find_package(Threads REQUIRED)

# Rules to build basic_kv_example
add_executable(basic_kv_example basic_kv_example.cpp)

target_include_directories(basic_kv_example PUBLIC ${LEANSTORE_INCLUDE_DIR})

if(ENABLE_COVERAGE)
    target_compile_options(basic_kv_example PRIVATE --coverage)
    target_link_options(basic_kv_example PRIVATE --coverage)
endif()

target_link_directories(basic_kv_example PRIVATE ${LEANSTORE_LIBRARY_DIR})
target_link_libraries(basic_kv_example PRIVATE
    leanstore
    aio           # Direct dependency for async I/O
    Threads::Threads  # System library (PUBLIC from leanstore)
)
