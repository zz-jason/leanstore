# ------------------------------------------------------------------------------
# WiredTiger External Dependency
# ------------------------------------------------------------------------------
#
# Search order:
#   1. Container prebuilt (WT_PREBUILT_DIR env var, e.g., /opt/wiredtiger)
#   2. Project cache (third_party/cache/wiredtiger-{version}/install/)
#   3. Build from source using scripts/build-wiredtiger.sh
#
# User-configurable options:
#   - WT_VERSION: WiredTiger version tag (default: 11.3.1)
#   - WT_GIT_REPOSITORY: Git repository URL
#   - WT_PREBUILT_DIR: Path to prebuilt WiredTiger (usually set via env var)
# ------------------------------------------------------------------------------

include(ExternalProject)

# User-configurable options
set(WT_VERSION "11.3.1" CACHE STRING "WiredTiger version tag")
set(WT_GIT_REPOSITORY "https://github.com/wiredtiger/wiredtiger.git" CACHE STRING "WiredTiger git repository URL")
set(WT_PREBUILT_DIR "$ENV{WT_PREBUILT_DIR}" CACHE PATH "Prebuilt WiredTiger directory")

# Internal paths (not cached - derived from version and build type)
set(WT_CACHE_DIR "${CMAKE_SOURCE_DIR}/third_party/cache/wiredtiger-${WT_VERSION}")
set(WT_INSTALL_DIR "${WT_CACHE_DIR}/install/${CMAKE_BUILD_TYPE}")
set(WT_BUILD_SCRIPT "${CMAKE_SOURCE_DIR}/scripts/build-wiredtiger.sh")

# Helper function to create imported wiredtiger library
function(wiredtiger_create_imported_target lib_path include_dir)
  add_library(wiredtiger STATIC IMPORTED GLOBAL)
  set_target_properties(wiredtiger PROPERTIES
    IMPORTED_LOCATION "${lib_path}"
    INTERFACE_INCLUDE_DIRECTORIES "${include_dir}"
  )
endfunction()

# Helper function to find and validate wiredtiger library
function(wiredtiger_find_library result_var search_path)
  find_library(_wt_lib NAMES wiredtiger PATHS "${search_path}/lib" NO_DEFAULT_PATH)
  if(_wt_lib AND EXISTS "${_wt_lib}")
    set(${result_var} "${_wt_lib}" PARENT_SCOPE)
  else()
    unset(${result_var} PARENT_SCOPE)
  endif()
  unset(_wt_lib CACHE)
endfunction()

# ------------------------------------------------------------------------------
# Search for WiredTiger in order of preference
# ------------------------------------------------------------------------------

# 1. Check container prebuilt (CI/Docker environment)
if(WT_PREBUILT_DIR)
  set(_wt_container_dir "${WT_PREBUILT_DIR}/${WT_VERSION}/${CMAKE_BUILD_TYPE}")
  wiredtiger_find_library(_wt_container_lib "${_wt_container_dir}")
  if(_wt_container_lib)
    message(STATUS "Found WiredTiger in container: ${_wt_container_lib}")
    wiredtiger_create_imported_target("${_wt_container_lib}" "${_wt_container_dir}/include")
    add_library(wiredtiger::wiredtiger ALIAS wiredtiger)
    return()
  endif()
endif()

# 2. Check project cache
wiredtiger_find_library(_wt_cached_lib "${WT_INSTALL_DIR}")
if(_wt_cached_lib)
  message(STATUS "Found WiredTiger in cache: ${_wt_cached_lib}")
  wiredtiger_create_imported_target("${_wt_cached_lib}" "${WT_INSTALL_DIR}/include")
  add_library(wiredtiger::wiredtiger ALIAS wiredtiger)
  return()
endif()

# 3. Build from source
message(STATUS "WiredTiger not found. Building from source...")

include(ProcessorCount)
ProcessorCount(NPROC)
if(NPROC EQUAL 0)
  set(NPROC 4)
endif()

ExternalProject_Add(wiredtiger_ext
  GIT_REPOSITORY    "${WT_GIT_REPOSITORY}"
  GIT_TAG           "${WT_VERSION}"
  GIT_SHALLOW       TRUE
  PREFIX            "${CMAKE_BINARY_DIR}/third_party/wiredtiger-${WT_VERSION}"
  SOURCE_DIR        "${WT_CACHE_DIR}/src"
  STAMP_DIR         "${WT_CACHE_DIR}/stamp"
  BINARY_DIR        "${WT_CACHE_DIR}/build"
  UPDATE_COMMAND    ""
  UPDATE_DISCONNECTED TRUE
  CONFIGURE_COMMAND ""
  BUILD_COMMAND     ${WT_BUILD_SCRIPT}
                    --version ${WT_VERSION}
                    --build-type ${CMAKE_BUILD_TYPE}
                    --source-dir <SOURCE_DIR>
                    --install-prefix ${WT_INSTALL_DIR}
                    --jobs ${NPROC}
  INSTALL_COMMAND   ""
  BUILD_BYPRODUCTS  "${WT_INSTALL_DIR}/lib/libwiredtiger.a"
)

file(MAKE_DIRECTORY "${WT_INSTALL_DIR}/include")
wiredtiger_create_imported_target("${WT_INSTALL_DIR}/lib/libwiredtiger.a" "${WT_INSTALL_DIR}/include")
add_dependencies(wiredtiger wiredtiger_ext)
add_library(wiredtiger::wiredtiger ALIAS wiredtiger)
