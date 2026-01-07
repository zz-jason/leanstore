include(ExternalProject)

set(EXTERNAL_CACHE_DIR "${CMAKE_SOURCE_DIR}/third_party/cache")
set(EXTERNAL_PREFIX_DIR "${CMAKE_BINARY_DIR}/third_party")

set(WT_GIT_REPOSITORY "https://github.com/wiredtiger/wiredtiger.git" CACHE STRING "WiredTiger git repository URL")
set(WT_VERSION "11.3.1" CACHE STRING "WiredTiger git tag to fetch")

set(WT_PREFIX "${EXTERNAL_PREFIX_DIR}/wiredtiger-${WT_VERSION}" CACHE PATH "Prefix for WiredTiger external project")
set(WT_DOWNLOAD_DIR "${EXTERNAL_CACHE_DIR}/wiredtiger-${WT_VERSION}/download" CACHE PATH "Download directory for WiredTiger sources")
set(WT_SOURCE_DIR "${EXTERNAL_CACHE_DIR}/wiredtiger-${WT_VERSION}/src" CACHE PATH "Source directory for WiredTiger sources")
set(WT_BINARY_DIR "${EXTERNAL_PREFIX_DIR}/wiredtiger-${WT_VERSION}/build" CACHE PATH "Build directory for WiredTiger build artifacts")
set(WT_INSTALL_DIR "${EXTERNAL_CACHE_DIR}/wiredtiger-${WT_VERSION}/install/${CMAKE_BUILD_TYPE}" CACHE PATH "Install directory for WiredTiger build artifacts")
set(WT_STAMP_DIR "${EXTERNAL_CACHE_DIR}/wiredtiger-${WT_VERSION}/stamp" CACHE PATH "Stamp directory for WiredTiger build status")
set(WT_C_FLAGS "-Wno-error=array-bounds -Wno-array-bounds -Wno-error=format-overflow -Wno-error=unterminated-string-initialization")
set(WT_CXX_FLAGS "-Wno-error=array-bounds -Wno-array-bounds -Wno-error=format-overflow -Wno-error=unterminated-string-initialization")

set(WT_LIB_PATH "${WT_INSTALL_DIR}/lib/libwiredtiger.a")

find_library(WT_FOUND_LIB
  NAMES wiredtiger
  PATHS "${WT_INSTALL_DIR}/lib"
  NO_DEFAULT_PATH
)

if(WT_FOUND_LIB)
  message(STATUS "Found prebuilt WiredTiger: ${WT_FOUND_LIB}")
  add_library(wiredtiger STATIC IMPORTED GLOBAL)
  set_target_properties(wiredtiger PROPERTIES
    IMPORTED_LOCATION "${WT_FOUND_LIB}"
    INTERFACE_INCLUDE_DIRECTORIES "${WT_INSTALL_DIR}/include"
  )
else()
  message(STATUS "WiredTiger not found in cache. Building from source...")
  ExternalProject_Add(wiredtiger_ext
      GIT_REPOSITORY    "${WT_GIT_REPOSITORY}"
      GIT_TAG           "${WT_VERSION}"
      GIT_SHALLOW       TRUE
      PREFIX            "${WT_PREFIX}"
      DOWNLOAD_DIR      "${WT_DOWNLOAD_DIR}"
      SOURCE_DIR        "${WT_SOURCE_DIR}"
      BINARY_DIR        "${WT_BINARY_DIR}"
      STAMP_DIR         "${WT_STAMP_DIR}"
      INSTALL_DIR       "${WT_INSTALL_DIR}"
      UPDATE_COMMAND    ""
      UPDATE_DISCONNECTED TRUE
      BUILD_BYPRODUCTS  "${WT_LIB_PATH}"
      CMAKE_CACHE_ARGS
        -DCMAKE_INSTALL_PREFIX:PATH=${WT_INSTALL_DIR}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=ON
        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        -DENABLE_STATIC:BOOL=ON
        -DENABLE_SHARED:BOOL=OFF
        -DENABLE_PYTHON:BOOL=OFF
        -DENABLE_JAVA:BOOL=OFF
        -DENABLE_TEST:BOOL=OFF
        -DENABLE_EXAMPLES:BOOL=OFF
        -DENABLE_ZLIB:BOOL=ON
        "-DCMAKE_C_FLAGS:STRING=${WT_C_FLAGS}"
        "-DCMAKE_CXX_FLAGS:STRING=${WT_CXX_FLAGS}"
  )

  file(MAKE_DIRECTORY "${WT_INSTALL_DIR}/include")
  add_library(wiredtiger STATIC IMPORTED GLOBAL)
  set_target_properties(wiredtiger PROPERTIES
      IMPORTED_LOCATION "${WT_LIB_PATH}"
      INTERFACE_INCLUDE_DIRECTORIES "${WT_INSTALL_DIR}/include"
  )
  add_dependencies(wiredtiger wiredtiger_ext)
endif()

add_library(wiredtiger::wiredtiger ALIAS wiredtiger)
