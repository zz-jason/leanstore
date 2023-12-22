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
    gtest_main
    gtest
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
    gtest
    leanstore
  )
  gtest_discover_tests(${TARGET_NAME})
  list(APPEND TEST_EXECUTABLES ${TARGET_NAME})
endfunction(leanstore_add_test_without_gtest_main)


# Add tests
leanstore_add_test(BTreeLLTest)
leanstore_add_test(BTreeVILoggingAndRecoveryTest)
leanstore_add_test(BufferFrameTest)

leanstore_add_test_without_gtest_main(BTreeVITest)

# for code coverages
if(BUILD_WITH_COVERAGE)
  setup_target_for_coverage_gcovr_xml(
    NAME ctest_coverage
    EXECUTABLE ctest
    DEPENDENCIES ${TEST_EXECUTABLES}
    EXCLUDE "build/*"
  )
endif()