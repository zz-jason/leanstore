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

# # for code coverages
# if(BUILD_WITH_COVERAGE)
#   add_custom_target(coverage
#       COMMAND gcovr -r . --json-pretty -json coverage.json --xml-pretty -xml coverage.xml --exclude 'build/*' --exclude 'tests/*'
#       COMMENT "Generating code coverage reports"
#       DEPENDS ${TEST_EXECUTABLES}
#   )
# endif()