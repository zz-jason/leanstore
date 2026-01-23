# ---------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ---------------------------------------------------------------------------

file(GLOB_RECURSE YCSB_SRC
  **.cpp
  **.cc
)

add_executable(ycsb ${YCSB_SRC})

set(YCSB_DEPS lean_project_options leanstore)

include(${CMAKE_SOURCE_DIR}/third_party/wiredtiger.cmake)
list(APPEND YCSB_DEPS wiredtiger::wiredtiger)

# Ensure check-tidy depends on wiredtiger_ext (if building from source)
# so that wiredtiger.h is available during clang-tidy checks
if(TARGET wiredtiger_ext)
  if(TARGET check-tidy)
    add_dependencies(check-tidy wiredtiger_ext)
  endif()
endif()

find_package(RocksDB CONFIG REQUIRED)
list(APPEND YCSB_DEPS RocksDB::rocksdb)

target_link_libraries(ycsb PRIVATE ${YCSB_DEPS})

# include dirs
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR})
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(ycsb PUBLIC ${CMAKE_SOURCE_DIR}/tools)
target_include_directories(ycsb PRIVATE ${CMAKE_CURRENT_LIST_DIR})
find_path(TANAKH_CMDLINE_INCLUDE_DIRS "tanakh-cmdline/cmdline.h")
target_include_directories(ycsb PRIVATE ${TANAKH_CMDLINE_INCLUDE_DIRS})

# install the executable
install(TARGETS ycsb
  EXPORT leanstore-targets
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
