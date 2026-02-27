# ------------------------------------------------------------------------------
# Add library
# ------------------------------------------------------------------------------

set(LEAN_SOURCES
  lean_store.cpp
  # c
  c/btree_impl.cpp
  c/btree_impl.hpp
  c/btree_mvcc_impl.cpp
  c/btree_mvcc_impl.hpp
  c/cursor_impl.cpp
  c/cursor_impl.hpp
  c/cursor_mvcc_impl.cpp
  c/cursor_mvcc_impl.hpp
  c/lean_str.cpp
  c/session_impl.cpp
  c/session_impl.hpp
  c/store.cpp
  c/store_impl.hpp
  c/table_impl.cpp
  c/table_impl.hpp
  c/tx_guard.hpp
  c/perf_counters.cpp
  c/store_option.cpp
  c/utils.cpp
  base/stacktrace.cpp
  # btree
  btree/b_tree_generic.cpp
  btree/b_tree_node.cpp
  btree/b_tree_wal_payload.hpp
  btree/basic_kv.cpp
  btree/btree_iter.cpp
  btree/btree_iter_mut.cpp
  # buffer
  buffer/async_write_buffer.cpp
  buffer/buffer_manager.cpp
  buffer/page_evictor.cpp
  buffer/partition.cpp
  # checkpoint
  checkpoint/checkpoint_processor.cpp
  # column_store
  btree/column_store/column_compression_common.cpp
  btree/column_store/column_compression_ordered_dictionary.cpp
  btree/column_store/column_compression_raw.cpp
  btree/column_store/column_compression_single_value.cpp
  btree/column_store/column_compression_truncation.cpp
  btree/column_store/column_compression.cpp
  btree/column_store/column_block_builder.cpp
  btree/column_store/column_block_reader.cpp
  btree/column_store/column_block_writer.cpp
  btree/column_store/column_leaf_ops.cpp
  # coro
  coro/auto_commit_protocol.cpp
  coro/coro_env.cpp
  coro/coro_executor.cpp
  coro/coro_io.cpp
  coro/coro_mutex.cpp
  coro/coro_scheduler.cpp
  coro/coroutine.cpp
  coro/mvcc_manager.cpp
  # failpoint
  failpoint/failpoint.hpp
  # recovery
  recovery/parallel_recovery.cpp
  recovery/recovery_analyzer.cpp
  recovery/recovery_redoer.cpp
  recovery/recovery_undoer.cpp
  # table
  table/encoding.cpp
  table/table.cpp
  table/table_column_store.cpp
  table/table_registry.cpp
  table/table_schema.cpp
  # telemetry
  telemetry/metrics_http_exposer.cpp
  telemetry/metrics_http_exposer.hpp
  # tx
  tx/chained_tuple.cpp
  tx/concurrency_control.cpp
  tx/cr_manager.cpp
  tx/group_committer.cpp
  tx/history_storage.cpp
  tx/logging.cpp
  tx/recovery.cpp
  tx/transaction_kv.cpp
  tx/tuple.cpp
  tx/tx_manager.cpp
  # utils
  utils/json.cpp
  utils/json.hpp
  utils/jump_mu.cpp
  utils/log.cpp
  utils/misc.cpp
  utils/parallelize.cpp
  utils/random_generator.cpp
  utils/to_json.hpp
  utils/zipfian_generator.cpp
  # wal
  wal/wal_builder.hpp
  wal/wal_cursor.cpp
  wal/wal_file_cursor.cpp
  wal/wal_file_cursor.hpp
  wal/wal_serde.cpp
)

if(BUILD_SHARED_LIBS)
  add_library(leanstore SHARED ${LEAN_SOURCES})
else()
  add_library(leanstore STATIC ${LEAN_SOURCES})
endif()
add_library(leanstore::leanstore ALIAS leanstore)

# ------------------------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------------------------

find_package(Threads REQUIRED)

# libaio is a public dependency (headers include <libaio.h>)
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
  pkg_check_modules(LIBAIO QUIET IMPORTED_TARGET libaio)
endif()

if(TARGET LIBAIO::LIBAIO)
  # Use pkg-config imported target, but create an alias with consistent naming
  if(NOT TARGET libaio::libaio)
    add_library(libaio::libaio ALIAS LIBAIO::LIBAIO)
  endif()
  set(LIBAIO_TARGET libaio::libaio)
else()
  # Fall back to find_library
  find_library(LIBAIO_LIBRARY aio)
  find_path(LIBAIO_INCLUDE_DIR libaio.h)
  if(LIBAIO_LIBRARY AND LIBAIO_INCLUDE_DIR)
    add_library(libaio::libaio INTERFACE IMPORTED)
    set_target_properties(libaio::libaio PROPERTIES
      INTERFACE_LINK_LIBRARIES "${LIBAIO_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${LIBAIO_INCLUDE_DIR}"
    )
    set(LIBAIO_TARGET libaio::libaio)
  else()
    message(FATAL_ERROR "libaio not found. Required for leanstore.")
  endif()
endif()

find_package(spdlog CONFIG REQUIRED)
find_package(Crc32c CONFIG REQUIRED)
find_package(RapidJSON CONFIG REQUIRED)
find_package(cpptrace CONFIG REQUIRED)
find_package(boost_context CONFIG REQUIRED)
find_package(httplib CONFIG REQUIRED)

# Profiling support using gperftools (heap profiler and CPU profiler).
set(LEAN_PROFILING_DEPS "")
if(LEAN_ENABLE_PROFILING AND NOT BUILD_SHARED_LIBS)
  pkg_check_modules(PKG_PROFILER IMPORTED_TARGET libprofiler)
  pkg_check_modules(PKG_TCMALLOC IMPORTED_TARGET libtcmalloc)

  if(PKG_PROFILER_FOUND AND PKG_TCMALLOC_FOUND)
    message(STATUS "Profiling enabled: libprofiler and libtcmalloc found")
    target_compile_definitions(leanstore PUBLIC LEAN_ENABLE_PROFILING)
    list(APPEND LEAN_PROFILING_DEPS PkgConfig::PKG_PROFILER PkgConfig::PKG_TCMALLOC)
  else()
    message(WARNING "LEAN_ENABLE_PROFILING is ON but required libraries not found. Profiling disabled.")
  endif()
elseif(LEAN_ENABLE_PROFILING AND BUILD_SHARED_LIBS)
  message(STATUS "Profiling disabled: not supported with BUILD_SHARED_LIBS=ON (gperftools compatibility)")
endif()

# Public compile definitions needed by consumers of the library
if(LEAN_ENABLE_CORO)
  target_compile_definitions(leanstore PUBLIC LEAN_ENABLE_CORO)
endif()
if(LEAN_ENABLE_PERF_COUNTERS)
  target_compile_definitions(leanstore PUBLIC LEAN_ENABLE_PERF_COUNTERS)
endif()
# LEAN_ENABLE_DEBUG_EXEC is only relevant for debug builds
if(LEAN_ENABLE_DEBUG_EXEC)
  target_compile_definitions(leanstore PUBLIC $<$<CONFIG:Debug>:LEAN_ENABLE_DEBUG_EXEC>)
endif()

target_link_libraries(leanstore
  PUBLIC
    Threads::Threads      # System library, required by public API (std::thread)
    ${LIBAIO_TARGET}      # Public dependency (headers include <libaio.h>)
  PRIVATE
    lean_project_options
    Boost::context        # Hidden by Coroutine pimpl
    spdlog::spdlog_header_only
    Crc32c::crc32c
    rapidjson             # Moved to private API
    cpptrace::cpptrace
    httplib::httplib
    ${LEAN_PROFILING_DEPS}
)

set_target_properties(leanstore PROPERTIES
  POSITION_INDEPENDENT_CODE ON
)

# ------------------------------------------------------------------------------
# Include directories
# ------------------------------------------------------------------------------

target_include_directories(leanstore
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# ------------------------------------------------------------------------------
# Exporting and Installation
# ------------------------------------------------------------------------------

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Generate version file
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/leanstoreConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion
)

# Generate config file from template
configure_package_config_file(
  "${CMAKE_SOURCE_DIR}/cmake/leanstoreConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/leanstoreConfig.cmake"
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/leanstore
)

# Install targets and export them
# Added lean_project_options to the export set to satisfy dependency requirements
install(
  TARGETS leanstore lean_project_options
  EXPORT leanstoreTargets
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Install the export set
install(
  EXPORT leanstoreTargets
  FILE leanstoreTargets.cmake
  NAMESPACE leanstore::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/leanstore
)

# Install config files
install(
  FILES
    "${CMAKE_CURRENT_BINARY_DIR}/leanstoreConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/leanstoreConfigVersion.cmake"
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/leanstore
)

# Install public headers
install(
  DIRECTORY ${CMAKE_SOURCE_DIR}/include/leanstore
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# ------------------------------------------------------------------------------
# pkg-config export
# ------------------------------------------------------------------------------

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/leanstore.pc.in
  ${CMAKE_CURRENT_BINARY_DIR}/leanstore.pc
  @ONLY
)

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/leanstore.pc
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)
