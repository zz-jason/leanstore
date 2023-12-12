# ---------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ---------------------------------------------------------------------------

add_executable(ycsb ycsb.cpp)

# dependencies
target_link_libraries(ycsb leanstore)

# include dirs
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/benchmarks)
target_include_directories(ycsb PRIVATE ${CMAKE_CURRENT_LIST_DIR})
