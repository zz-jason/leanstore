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

function(leanstore_add_test_without_gtest_main TARGET_NAME)
  add_executable(
    ${TARGET_NAME}
    ${TARGET_NAME}.cpp
  )
  target_link_libraries(
    ${TARGET_NAME}
    GTest::gtest
    leanstore
  )
  gtest_discover_tests(${TARGET_NAME})
  list(APPEND TEST_EXECUTABLES ${TARGET_NAME})
endfunction(leanstore_add_test_without_gtest_main)


# Add tests
leanstore_add_test(BasicKVTest)
leanstore_add_test(RecoveryTest)
leanstore_add_test(BufferFrameTest)
leanstore_add_test(OptimisticGuardedTest)
leanstore_add_test(TransactionKVTest)
leanstore_add_test(MvccTest)
leanstore_add_test(AnomaliesTest)
leanstore_add_test(AbortTest)
leanstore_add_test(LongRunningTxTest)

add_subdirectory(sync)