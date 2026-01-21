include(ExternalProject)

set(EXTERNAL_CACHE_DIR "${CMAKE_SOURCE_DIR}/third_party/cache")
set(EXTERNAL_PREFIX_DIR "${CMAKE_BINARY_DIR}/third_party")

set(ROCKSDB_GIT_REPOSITORY "https://github.com/facebook/rocksdb.git" CACHE STRING "RocksDB git repository URL")
set(ROCKSDB_VERSION "v10.9.1" CACHE STRING "RocksDB git tag to fetch")

set(ROCKSDB_PREFIX "${EXTERNAL_PREFIX_DIR}/rocksdb" CACHE PATH "Prefix for RocksDB external project")
set(ROCKSDB_DOWNLOAD_DIR "${EXTERNAL_CACHE_DIR}/rocksdb/download" CACHE PATH "Download directory for RocksDB sources")
set(ROCKSDB_SOURCE_DIR "${EXTERNAL_CACHE_DIR}/rocksdb/src" CACHE PATH "Source directory for RocksDB sources")
set(ROCKSDB_BINARY_DIR "${EXTERNAL_PREFIX_DIR}/rocksdb/build" CACHE PATH "Build directory for RocksDB build artifacts")
set(ROCKSDB_INSTALL_DIR "${EXTERNAL_CACHE_DIR}/rocksdb/install/${CMAKE_BUILD_TYPE}" CACHE PATH "Install directory for RocksDB build artifacts")
set(ROCKSDB_STAMP_DIR "${EXTERNAL_CACHE_DIR}/rocksdb/stamp" CACHE PATH "Stamp directory for RocksDB build status")
set(ROCKSDB_C_FLAGS "-Wno-error=array-bounds -Wno-array-bounds -Wno-error=format-overflow -Wno-error=unterminated-string-initialization")
set(ROCKSDB_CXX_FLAGS "-Wno-error=array-bounds -Wno-array-bounds -Wno-error=format-overflow -Wno-error=unterminated-string-initialization")

set(ROCKSDB_LIB_PATH "${ROCKSDB_INSTALL_DIR}/lib/librocksdb.a")

find_library(ROCKSDB_FOUND_LIB
  NAMES rocksdb
  PATHS "${ROCKSDB_INSTALL_DIR}/lib"
  NO_DEFAULT_PATH
)

if(ROCKSDB_FOUND_LIB)
  message(STATUS "Found prebuilt RocksDB: ${ROCKSDB_FOUND_LIB}")
  add_library(rocksdb STATIC IMPORTED GLOBAL)
  set_target_properties(rocksdb PROPERTIES
    IMPORTED_LOCATION "${ROCKSDB_FOUND_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INSTALL_DIR}/include"
  )
else()
  message(STATUS "RocksDB not found in cache. Building from source...")
  ExternalProject_Add(rocksdb_ext
      GIT_REPOSITORY    "${ROCKSDB_GIT_REPOSITORY}"
      GIT_TAG           "${ROCKSDB_VERSION}"
      GIT_SHALLOW       TRUE
      PREFIX            "${ROCKSDB_PREFIX}"
      DOWNLOAD_DIR      "${ROCKSDB_DOWNLOAD_DIR}"
      SOURCE_DIR        "${ROCKSDB_SOURCE_DIR}"
      BINARY_DIR        "${ROCKSDB_BINARY_DIR}"
      STAMP_DIR         "${ROCKSDB_STAMP_DIR}"
      INSTALL_DIR       "${ROCKSDB_INSTALL_DIR}"
      UPDATE_COMMAND    ""
      UPDATE_DISCONNECTED TRUE
      BUILD_BYPRODUCTS  "${ROCKSDB_LIB_PATH}"
      CMAKE_CACHE_ARGS
        -DCMAKE_INTERPROCEDURAL_OPTIMIZATION:BOOL=ON  # LTO
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON

        -DWITH_TESTS:BOOL=OFF                     # 不编译测试
        -DWITH_TOOLS:BOOL=OFF                     # 不编译工具
        -DWITH_BENCHMARK_TOOLS:BOOL=OFF           # 不编译 benchmark
        -DWITH_CORE_TOOLS:BOOL=OFF                # 不编译核心工具
        -DROCKSDB_BUILD_SHARED:BOOL=OFF           # 不编译动态库
        -DFAIL_ON_WARNINGS:BOOL=OFF               # 允许警告

        -DWITH_SNAPPY:BOOL=ON                     # Snappy: 快速压缩/解压
        -DWITH_LZ4:BOOL=ON                        # LZ4: 更快
        -DWITH_ZSTD:BOOL=OFF                      # Zstd: 最佳压缩比. TODO: Enable later
        -DWITH_ZLIB:BOOL=ON                       # Zlib: 兼容性
        -DWITH_BZ2:BOOL=OFF                       # BZ2: 慢，不推荐

        -DWITH_JEMALLOC:BOOL=OFF                  # ⭐ 关键：使用 jemalloc（性能提升 10-20%）. TODO: Enable later
        -DWITH_TBB:BOOL=OFF                       # Intel TBB（可选）

        -DPORTABLE:BOOL=OFF                       # 允许使用 CPU 特定指令（SSE, AVX）
        -DFORCE_SSE42:BOOL=ON                     # 强制使用 SSE4.2

        -DWITH_NUMA:BOOL=OFF                      # NUMA 感知内存分配. TODO: Enable later
        -DWITH_LIBURING:BOOL=ON                   # io_uring（Linux 5.1+，异步 I/O）
        -DWITH_TRACE_TOOLS:BOOL=OFF               # 不需要 trace
        -DWITH_JNI:BOOL=OFF                       # 不需要 Java 支持
        -DWITH_GFLAGS:BOOL=OFF                    # 不需要 gflags
        -DWITH_BENCHMARK:BOOL=OFF                 # 不需要 Google Benchmark

        -DCMAKE_INSTALL_PREFIX:PATH=${ROCKSDB_INSTALL_DIR}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=ON

        "-DCMAKE_C_FLAGS:STRING=${ROCKSDB_C_FLAGS}"
        "-DCMAKE_CXX_FLAGS:STRING=${ROCKSDB_CXX_FLAGS}"
  )

  file(MAKE_DIRECTORY "${ROCKSDB_INSTALL_DIR}/include")
  add_library(rocksdb STATIC IMPORTED GLOBAL)
  set_target_properties(rocksdb PROPERTIES
      IMPORTED_LOCATION "${ROCKSDB_LIB_PATH}"
      INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INSTALL_DIR}/include"
  )
  add_dependencies(rocksdb rocksdb_ext)
endif()

add_library(rocksdb::rocksdb ALIAS rocksdb)