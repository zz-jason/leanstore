# ---------------------------------------------------------------------------
# Set build target and dependencies for the lib
# ---------------------------------------------------------------------------

file(GLOB_RECURSE YCSB_SRC
  **.cpp
  **.cc
  **.hpp
  **.h
)

set(YCSB_DEPS
  leanstore
)

find_package(RocksDB CONFIG REQUIRED)
list(APPEND YCSB_DEPS RocksDB::rocksdb)

if(LEAN_ENABLE_WIRED_TIGER)
  #TODO(zz-jason): use pre-installed wiredtiger
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    link_directories("/root/code/wiredtiger/dist/debug/lib")
  else()
    link_directories("/root/code/wiredtiger/dist/release/lib")
  endif()
  list(APPEND YCSB_DEPS wiredtiger)
endif(LEAN_ENABLE_WIRED_TIGER)

add_executable(ycsb ${YCSB_SRC})
target_link_libraries(ycsb PRIVATE ${YCSB_DEPS})

if(LEAN_ENABLE_WIRED_TIGER)
  target_compile_definitions(ycsb PUBLIC LEAN_ENABLE_WIRED_TIGER)
  target_include_directories(ycsb PUBLIC "/root/code/wiredtiger/dist/debug/include")
endif(LEAN_ENABLE_WIRED_TIGER)

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