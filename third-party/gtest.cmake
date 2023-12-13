set(TARGET_NAME gtest)
set(GIT_REPO "https://github.com/google/googletest.git")
set(GIT_TAG v1.14.0)

set(TARGET_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-src)
set(TARGET_INSTALL ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME})
ExternalProject_Add(${TARGET_NAME}_internal
    PREFIX ${TARGET_PREFIX}
    GIT_REPOSITORY ${GIT_REPO}
    GIT_TAG ${GIT_TAG}
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TARGET_INSTALL}
        -DCMAKE_PREFIX_PATH=${TARGET_INSTALL}
    UPDATE_COMMAND ""
)

# for gtest
set(TARGET_NAME gtest)
set(TARGET_INCLUDE_DIR ${TARGET_INSTALL}/include)
set(TARGET_LIB_PATH ${TARGET_INSTALL}/lib/libgtest.a)

message(STATUS "Adding external target: ${TARGET_NAME} ...")
message(STATUS "  - Target include dir: ${TARGET_INCLUDE_DIR}")
message(STATUS "  - Target link lib: ${TARGET_LIB_PATH}")

add_library(${TARGET_NAME} INTERFACE)
add_dependencies(${TARGET_NAME} gtest_internal)
target_include_directories(${TARGET_NAME} INTERFACE ${TARGET_INCLUDE_DIR})
target_link_libraries(${TARGET_NAME} INTERFACE ${TARGET_LIB_PATH})


# for gtest_main
set(TARGET_NAME gtest_main)
set(TARGET_INCLUDE_DIR ${TARGET_INSTALL}/include)
set(TARGET_LIB_PATH ${TARGET_INSTALL}/lib/libgtest_main.a)

message(STATUS "Adding external target: ${TARGET_NAME} ...")
message(STATUS "  - Target include dir: ${TARGET_INCLUDE_DIR}")
message(STATUS "  - Target link lib: ${TARGET_LIB_PATH}")

add_library(${TARGET_NAME} INTERFACE)
add_dependencies(${TARGET_NAME} gtest_internal)
target_include_directories(${TARGET_NAME} INTERFACE ${TARGET_INCLUDE_DIR})
target_link_libraries(${TARGET_NAME} INTERFACE ${TARGET_LIB_PATH})


list(APPEND CMAKE_PREFIX_PATH ${TARGET_INSTALL}/lib/cmake/${TARGET_NAME})
