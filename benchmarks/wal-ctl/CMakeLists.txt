file(GLOB_RECURSE LEAN_WAL_CTLL_SRC **.cpp **/**.cpp **.cc **/**.cc **.hpp **/**.hpp **.h **/**.h)

find_package(gflags CONFIG REQUIRED)
set(LEAN_WAL_CTLL_DEPS
  leanstore
  gflags::gflags
)

add_executable(lean_wal_ctl ${LEAN_WAL_CTLL_SRC})
target_link_libraries(lean_wal_ctl PRIVATE ${LEAN_WAL_CTLL_DEPS})

# include dirs
target_include_directories(lean_wal_ctl PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(lean_wal_ctl PRIVATE ${CMAKE_CURRENT_LIST_DIR})

# install the executable
install(TARGETS lean_wal_ctl
  EXPORT leanstore-targets
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)