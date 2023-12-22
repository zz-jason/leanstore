set(TARGET_NAME gbench)
set(GIT_REPO "https://github.com/google/benchmark.git")
set(GIT_TAG v1.8.3)

set(TARGET_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-src)
set(TARGET_INSTALL ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME})
ExternalProject_Add(${TARGET_NAME}_internal
    SOURCE_DIR ${THIRD_PARTY_SRC_DIR}/${TARGET_NAME}_src
    PREFIX ${TARGET_PREFIX}
    GIT_REPOSITORY ${GIT_REPO}
    GIT_TAG ${GIT_TAG}
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TARGET_INSTALL}
        -DCMAKE_PREFIX_PATH=${TARGET_INSTALL}
        -DBENCHMARK_DOWNLOAD_DEPENDENCIES=on
    UPDATE_COMMAND ""
)

message(STATUS "Adding external target: ${TARGET_NAME} ...")
add_library(${TARGET_NAME} INTERFACE)
add_dependencies(${TARGET_NAME} ${TARGET_NAME}_internal)

target_include_directories(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/include)
message(STATUS "  - Target include dir: ${TARGET_INSTALL}/include")

target_link_libraries(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/lib/libbenchmark.a)
message(STATUS "  - Target link lib: ${TARGET_INSTALL}/lib/libbenchmark.a")

target_link_libraries(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/lib/libbenchmark_main.a)
message(STATUS "  - Target link lib: ${TARGET_INSTALL}/lib/libbenchmark_main.a")

list(APPEND CMAKE_PREFIX_PATH ${TARGET_INSTALL}/lib/cmake/${TARGET_NAME})
