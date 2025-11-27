include(GoogleTest)
enable_testing()

set(TEST_EXECUTABLES "")

# Define a cmake function to add a test file linked with Google Test
function(leanstore_add_test TARGET_NAME)
  add_executable(
    ${TARGET_NAME}
    ${TARGET_NAME}.cpp
  )
  target_link_libraries(
    ${TARGET_NAME}
    GTest::gtest
    GTest::gtest_main
    leanstore
  )
  gtest_discover_tests(${TARGET_NAME})
  list(APPEND TEST_EXECUTABLES ${TARGET_NAME})
endfunction(leanstore_add_test)


# add test in dir
function(leanstore_add_test_in_dir TARGET_DIR)
  # list all files in the directory
  file(GLOB_RECURSE TEST_FILES "${TARGET_DIR}/*.cpp")
  foreach(TEST_FILE ${TEST_FILES})
    get_filename_component(TEST_NAME ${TEST_FILE} NAME_WE)
    add_executable(
      ${TEST_NAME}
      ${TEST_FILE}
    )
    target_link_libraries(
      ${TEST_NAME}
      GTest::gtest
      GTest::gtest_main
      leanstore
    )
    target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    gtest_discover_tests(${TEST_NAME})
    list(APPEND TEST_EXECUTABLES ${TEST_NAME})
  endforeach()
endfunction(leanstore_add_test_in_dir)


if(${ENABLE_COROUTINE})
  leanstore_add_test_in_dir(coroutine)
  leanstore_add_test_in_dir(c)
  leanstore_add_test_in_dir(cpp)
else()
  # Add tests
  leanstore_add_test(recovery_test)
  leanstore_add_test(optimistic_guarded_test)
  leanstore_add_test(transaction_kv_test)
  leanstore_add_test(mvcc_test)
  leanstore_add_test(anomalies_test)
  leanstore_add_test(abort_test)
  leanstore_add_test(long_running_tx_test)

  # tests in sub-directories
  leanstore_add_test_in_dir(btree)
  leanstore_add_test_in_dir(buffer-manager)
  leanstore_add_test_in_dir(concurrency)
  leanstore_add_test_in_dir(sync)
  leanstore_add_test_in_dir(telemetry)
endif()
