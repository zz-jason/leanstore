
find_package(Git REQUIRED)
include(ExternalProject)

set(TARGET_NAME onetbb)
set(LIB_NAME libtbb.so)
set(GIT_REPO "https://github.com/oneapi-src/oneTBB.git")
set(GIT_TAG v2021.11.0)

set(TARGET_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-src)
set(TARGET_INSTALL ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME})
ExternalProject_Add(${TARGET_NAME}_internal
    PREFIX ${TARGET_PREFIX}
    GIT_REPOSITORY ${GIT_REPO}
    GIT_TAG ${GIT_TAG}
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${TARGET_INSTALL}
        -DCMAKE_PREFIX_PATH=${TARGET_INSTALL}
        -DTBB_TEST=OFF
    UPDATE_COMMAND ""
)

message(STATUS "Adding external target: ${TARGET_NAME} ...")
add_library(${TARGET_NAME} INTERFACE)
add_dependencies(${TARGET_NAME} ${TARGET_NAME}_internal)

target_include_directories(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/include)
message(STATUS "  - Target include dir: ${TARGET_INSTALL}/include")
target_link_libraries(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/lib/${LIB_NAME})
message(STATUS "  - Target link lib: ${TARGET_INSTALL}/lib/${LIB_NAME}")
list(APPEND CMAKE_PREFIX_PATH ${TARGET_INSTALL}/lib/cmake/${TARGET_NAME})