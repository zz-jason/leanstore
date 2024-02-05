# ---------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ---------------------------------------------------------------------------

add_executable(ycsb Ycsb.cpp YcsbFlags.cpp)

# dependencies
# find_package(RocksDB CONFIG REQUIRED)
target_link_libraries(ycsb PRIVATE
  leanstore
  # RocksDB::rocksdb
  # RocksDB::rocksdb-shared
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PKG_PROFILER IMPORTED_TARGET libprofiler)
pkg_check_modules(PKG_TCMALLOC IMPORTED_TARGET libtcmalloc)
if (PKG_PROFILER_FOUND)
  message(STATUS "libprofiler found: ${PKG_PROFILER}")
  target_link_libraries(ycsb PUBLIC PkgConfig::PKG_PROFILER)
endif (PKG_PROFILER_FOUND)
if (PKG_TCMALLOC_FOUND)
  message(STATUS "libtcmalloc found: ${PKG_TCMALLOC}")
  target_link_libraries(ycsb PUBLIC PkgConfig::PKG_TCMALLOC)
endif (PKG_TCMALLOC_FOUND)

# include dirs
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/benchmarks)
target_include_directories(ycsb PRIVATE ${CMAKE_CURRENT_LIST_DIR})