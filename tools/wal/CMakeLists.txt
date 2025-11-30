function(lean_add_wal_executable TARGET_NAME SOURCE_FILE)
  add_executable(
    ${TARGET_NAME}
    ${SOURCE_FILE}
  )
  target_link_libraries(
    ${TARGET_NAME}
    leanstore
  )
  # include dirs
  target_include_directories(${TARGET_NAME} PUBLIC ${CMAKE_SOURCE_DIR}/include)
  target_include_directories(${TARGET_NAME} PRIVATE ${CMAKE_CURRENT_LIST_DIR})
  find_path(TANAKH_CMDLINE_INCLUDE_DIRS "tanakh-cmdline/cmdline.h")
  target_include_directories(${TARGET_NAME} PRIVATE ${TANAKH_CMDLINE_INCLUDE_DIRS})
  # install the executable
  install(TARGETS ${TARGET_NAME}
    EXPORT leanstore-targets
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
  )
endfunction(lean_add_wal_executable)

lean_add_wal_executable(lean_wal_dump wal_dump.cpp)
lean_add_wal_executable(lean_wal_analysis wal_analysis.cpp)
