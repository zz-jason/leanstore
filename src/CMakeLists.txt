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

pkg_check_modules(libunwind REQUIRED IMPORTED_TARGET GLOBAL libunwind)
list(APPEND LEAN_DEPS PkgConfig::libunwind)

if(LEAN_ENABLE_PROFILING)
  target_compile_definitions(leanstore PUBLIC LEAN_ENABLE_PROFILING)

  pkg_check_modules(PKG_PROFILER IMPORTED_TARGET libprofiler)
  if (PKG_PROFILER_FOUND)
    message(STATUS "libprofiler found: ${PKG_PROFILER}")
    list(APPEND LEAN_DEPS PkgConfig::PKG_PROFILER)
  endif (PKG_PROFILER_FOUND)

  pkg_check_modules(PKG_TCMALLOC IMPORTED_TARGET libtcmalloc)
  if (PKG_TCMALLOC_FOUND)
    message(STATUS "libtcmalloc found: ${PKG_TCMALLOC}")
    list(APPEND LEAN_DEPS PkgConfig::PKG_TCMALLOC)
  endif (PKG_TCMALLOC_FOUND)

endif(LEAN_ENABLE_PROFILING)

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
