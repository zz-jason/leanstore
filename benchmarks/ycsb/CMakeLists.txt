# ---------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ---------------------------------------------------------------------------

set(YCSB_SRC
  ycsb.cpp
  ycsb_flags.cpp
)

find_package(gflags CONFIG REQUIRED)
set(YCSB_DEPS
  leanstore
  gflags::gflags
)

option(ENABLE_ROCKSDB "Enable benchmarking rocksdb" OFF)
if(ENABLE_ROCKSDB)
  find_package(RocksDB CONFIG REQUIRED)
  list(APPEND YCSB_DEPS RocksDB::rocksdb)
endif(ENABLE_ROCKSDB)

option(ENABLE_WIRED_TIGER "Enable benchmarking wiredtiger" OFF)
if(ENABLE_WIRED_TIGER)
  #TODO(zz-jason): use pre-installed wiredtiger
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    link_directories("/root/code/wiredtiger/dist/debug/lib")
  else()
    link_directories("/root/code/wiredtiger/dist/release/lib")
  endif()
  list(APPEND YCSB_DEPS wiredtiger)
endif(ENABLE_WIRED_TIGER)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PKG_PROFILER IMPORTED_TARGET libprofiler)
pkg_check_modules(PKG_TCMALLOC IMPORTED_TARGET libtcmalloc)
if (PKG_PROFILER_FOUND)
  list(APPEND YCSB_DEPS PkgConfig::PKG_PROFILER)
endif (PKG_PROFILER_FOUND)
if (PKG_TCMALLOC_FOUND)
  message(STATUS "libtcmalloc found: ${PKG_TCMALLOC}")
  list(APPEND YCSB_DEPS PkgConfig::PKG_TCMALLOC)
endif (PKG_TCMALLOC_FOUND)

add_executable(ycsb ${YCSB_SRC})
target_link_libraries(ycsb PRIVATE ${YCSB_DEPS})

if(ENABLE_ROCKSDB)
  target_compile_definitions(ycsb PUBLIC ENABLE_ROCKSDB)
endif(ENABLE_ROCKSDB)

if(ENABLE_WIRED_TIGER)
  target_compile_definitions(ycsb PUBLIC ENABLE_WIRED_TIGER)
  target_include_directories(ycsb PUBLIC "/root/code/wiredtiger/dist/debug/include")
endif(ENABLE_WIRED_TIGER)

# include dirs
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/benchmarks)
target_include_directories(ycsb PRIVATE ${CMAKE_CURRENT_LIST_DIR})