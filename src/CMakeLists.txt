# ------------------------------------------------------------------------------
# Files for the lib
# ------------------------------------------------------------------------------
file(GLOB_RECURSE LEANSTORE_SRC **.cpp **/**.cpp **.cc **/**.cc **.hpp **/**.hpp **.h **/**.h)


# ------------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ------------------------------------------------------------------------------
option(BUILD_SHARED_LIBS "Build using shared libraries" ON)
if(BUILD_SHARED_LIBS)
  add_library(leanstore SHARED ${LEANSTORE_SRC})
else()
  add_library(leanstore STATIC ${LEANSTORE_SRC})
endif()

# dependencies
find_package(cpptrace CONFIG REQUIRED)
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 OLD)
endif()
find_package(Boost REQUIRED COMPONENTS context)

find_package(concurrentqueue CONFIG)

set(leanstore_deps
  aio
  Boost::context
  cpptrace::cpptrace
  rapidjson
  spdlog::spdlog_header_only
  Crc32c::crc32c
  concurrentqueue::concurrentqueue
  httplib::httplib
  PkgConfig::libunwind
)
target_link_libraries(leanstore PUBLIC ${leanstore_deps})

option(ENABLE_PROFILING "Enable profiling" OFF)
if(ENABLE_PROFILING)
  target_compile_definitions(leanstore PUBLIC ENABLE_PROFILING)

  find_package(PkgConfig REQUIRED)
  pkg_check_modules(PKG_PROFILER IMPORTED_TARGET libprofiler)
  pkg_check_modules(PKG_TCMALLOC IMPORTED_TARGET libtcmalloc)
  if (PKG_PROFILER_FOUND)
    message(STATUS "libprofiler found: ${PKG_PROFILER}")
    target_link_libraries(leanstore PUBLIC PkgConfig::PKG_PROFILER)
  endif (PKG_PROFILER_FOUND)
  if (PKG_TCMALLOC_FOUND)
    message(STATUS "libtcmalloc found: ${PKG_TCMALLOC}")
    target_link_libraries(leanstore PUBLIC PkgConfig::PKG_TCMALLOC)
  endif (PKG_TCMALLOC_FOUND)
endif(ENABLE_PROFILING)

# include dirs
target_include_directories(leanstore PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(leanstore PRIVATE ${CMAKE_CURRENT_LIST_DIR})

# Get the include directories for the target.
get_target_property(LIBA_INCLUDES leanstore INCLUDE_DIRECTORIES)
foreach(dir ${LIBA_INCLUDES})
  message(STATUS "include dir: ${dir}")
endforeach()


# ---------------------------------------------------------------------------
# Set compile options for the lib
# ---------------------------------------------------------------------------

option(SANI "Compile leanstore with sanitizers" OFF)
if(SANI)
  target_compile_options(leanstore PUBLIC -fsanitize=address)
  target_link_libraries(leanstore asan)
endif(SANI)


option(ENABLE_PERF_COUNTERS "Whether to enable perf counters" OFF)
if(ENABLE_PERF_COUNTERS)
  target_compile_definitions(leanstore PUBLIC ENABLE_PERF_COUNTERS)
endif()

set(CHECKS_LEVEL "default" CACHE STRING "Which checks to leave in leanstore build")
if(CHECKS_LEVEL STREQUAL "default")
  if(CMAKE_BUILD_TYPE MATCHES Debug)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
  elseif(CMAKE_BUILD_TYPE MATCHES RelWithDebInfo OR CMAKE_BUILD_TYPE MATCHES Release)
    target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
  endif()
elseif(CHECKS_LEVEL STREQUAL "debug")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_DEBUG)
elseif(CHECKS_LEVEL STREQUAL "release")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_RELEASE)
elseif(CHECKS_LEVEL STREQUAL "benchmark")
  target_compile_definitions(leanstore PUBLIC MACRO_CHECK_BENCHMARK)
endif()

option(ENABLE_COROUTINE "Enable coroutine support" OFF)
if(ENABLE_COROUTINE)
  target_compile_definitions(leanstore PUBLIC ENABLE_COROUTINE)
endif()

# ------------------------------------------------------------------------------
# export header files for lib consumers
# ------------------------------------------------------------------------------
set(LEANSTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET leanstore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LEANSTORE_INCLUDE_DIR})

# ------------------------------------------------------------------------------
# install rule
# ------------------------------------------------------------------------------
include(GNUInstallDirs)
install(TARGETS leanstore
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY
  ${CMAKE_SOURCE_DIR}/include/leanstore
  DESTINATION include)

# ------------------------------------------------------------------------------
# export pkg-config files for lib consumers
# ------------------------------------------------------------------------------
configure_file(leanstore.pc.in leanstore.pc @ONLY)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/leanstore.pc DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/pkgconfig)
