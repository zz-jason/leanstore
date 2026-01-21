# ------------------------------------------------------------------------------
# Add library
# ------------------------------------------------------------------------------

file(GLOB_RECURSE LEAN_SRCS
  **.cpp
  **.cc
  **.hpp
  **.h
)

if(BUILD_SHARED_LIBS)
  add_library(leanstore SHARED ${LEAN_SRCS})
else()
  add_library(leanstore STATIC ${LEAN_SRCS})
endif()
add_library(leanstore::leanstore ALIAS leanstore)

# ------------------------------------------------------------------------------
# Dependencies
# ------------------------------------------------------------------------------

set(LEAN_DEPS
  lean_project_options
  aio
)

find_package(Threads REQUIRED)
list(APPEND LEAN_DEPS Threads::Threads)

find_package(spdlog CONFIG REQUIRED)
list(APPEND LEAN_DEPS spdlog::spdlog_header_only)

find_package(Crc32c CONFIG REQUIRED)
list(APPEND LEAN_DEPS Crc32c::crc32c)

find_package(RapidJSON CONFIG REQUIRED)
list(APPEND LEAN_DEPS rapidjson)

find_package(cpptrace CONFIG REQUIRED)
list(APPEND LEAN_DEPS cpptrace::cpptrace)

find_package(boost_context CONFIG REQUIRED)
list(APPEND LEAN_DEPS Boost::context)

find_package(concurrentqueue CONFIG)
list(APPEND LEAN_DEPS concurrentqueue::concurrentqueue)

find_package(httplib CONFIG REQUIRED)
list(APPEND LEAN_DEPS httplib::httplib)

# Profiling support using gperftools (heap profiler and CPU profiler).
# Disabled for shared library builds due to gperftools compatibility issues:
# 1. gperftools (libprofiler + libtcmalloc) are static libraries from vcpkg
# 2. When building shared library, static symbols need --whole-archive to be included
# 3. However, this causes heap-checker initialization to run twice (once in the
#    shared library, once in executables linking libprofiler), resulting in:
#    "Heap-check constructor called twice" runtime errors
# Profiling is primarily for development/debugging, so disabling it for shared
# library builds is acceptable. Use static builds for profiling support.
if(LEAN_ENABLE_PROFILING AND NOT BUILD_SHARED_LIBS)
  pkg_check_modules(PKG_PROFILER IMPORTED_TARGET libprofiler)
  pkg_check_modules(PKG_TCMALLOC IMPORTED_TARGET libtcmalloc)

  if(PKG_PROFILER_FOUND AND PKG_TCMALLOC_FOUND)
    message(STATUS "Profiling enabled: libprofiler and libtcmalloc found")
    target_compile_definitions(leanstore PUBLIC LEAN_ENABLE_PROFILING)
    list(APPEND LEAN_DEPS PkgConfig::PKG_PROFILER)
    list(APPEND LEAN_DEPS PkgConfig::PKG_TCMALLOC)
  else()
    message(WARNING "LEAN_ENABLE_PROFILING is ON but required libraries not found:")
    if(NOT PKG_PROFILER_FOUND)
      message(WARNING "  - libprofiler not found")
    endif()
    if(NOT PKG_TCMALLOC_FOUND)
      message(WARNING "  - libtcmalloc not found")
    endif()
    message(WARNING "Profiling will be disabled. Install gperftools or check PKG_CONFIG_PATH.")
  endif()
elseif(LEAN_ENABLE_PROFILING AND BUILD_SHARED_LIBS)
  message(STATUS "Profiling disabled: not supported with BUILD_SHARED_LIBS=ON (gperftools compatibility)")
endif()

target_link_libraries(leanstore PUBLIC ${LEAN_DEPS})

message(STATUS "leanstore dependencies:")
foreach(dep IN LISTS LEAN_DEPS)
  message(STATUS "  - ${dep}")
endforeach()

# ------------------------------------------------------------------------------
# Include directories
# ------------------------------------------------------------------------------

target_include_directories(leanstore
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

get_target_property(LIBA_INCLUDES leanstore INCLUDE_DIRECTORIES)
message(STATUS "leanstore include directories:")
foreach(dir ${LIBA_INCLUDES})
  message(STATUS "  - ${dir}")
endforeach()

# ------------------------------------------------------------------------------
# export header files for lib consumers
# ------------------------------------------------------------------------------

set(LEANSTORE_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET leanstore APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LEANSTORE_INCLUDE_DIR})

# ------------------------------------------------------------------------------
# Install
# ------------------------------------------------------------------------------

include(GNUInstallDirs)

install(
  TARGETS leanstore
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(
  DIRECTORY ${CMAKE_SOURCE_DIR}/include/leanstore
  DESTINATION include
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
  DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/pkgconfig
)
