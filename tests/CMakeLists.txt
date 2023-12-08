find_package(GTest REQUIRED)
include(GoogleTest)
enable_testing()

# Define a cmake function to add a test file linked with Google Test
function(leanstore_add_test TARGET_NAME)
  add_executable(
    ${TARGET_NAME}
    ${TARGET_NAME}.cpp
  )
  target_link_libraries(
    ${TARGET_NAME}
    GTest::gtest_main
    leanstore
  )
  gtest_discover_tests(${TARGET_NAME})
endfunction(leanstore_add_test)

# Add tests
leanstore_add_test(LeanStoreTest)
leanstore_add_test(BufferFrameTest)

# add_test(NAME AllTestsInLeanStoreTest COMMAND LeanStoreTest)

