# ---------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ---------------------------------------------------------------------------

add_executable(ycsb ycsb.cpp)

# dependencies
find_package(RocksDB CONFIG REQUIRED)
target_link_libraries(ycsb PRIVATE
  leanstore
  RocksDB::rocksdb
  RocksDB::rocksdb-shared
)

# include dirs
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/benchmarks)
target_include_directories(ycsb PRIVATE ${CMAKE_CURRENT_LIST_DIR})