find_package(Git REQUIRED)
include(ExternalProject)

# Define a cmake function to add a header-only external lib
function(leanstore_add_ext_header_only_lib TARGET_NAME GIT_REPO GIT_TAG)
    set(LIB_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/third-party)
    set(TARGET_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-src)
    ExternalProject_Add(${TARGET_NAME}_internal
        SOURCE_DIR ${THIRD_PARTY_SRC_DIR}/${TARGET_NAME}_src
        PREFIX ${TARGET_PREFIX}
        GIT_REPOSITORY ${GIT_REPO}
        GIT_TAG ${GIT_TAG}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR>/include/${TARGET_NAME} ${LIB_INCLUDE_DIR}/${TARGET_NAME}
        INSTALL_COMMAND ""
        UPDATE_COMMAND ""
    )

    message(STATUS "Adding external target: ${TARGET_NAME} ...")
    add_library(${TARGET_NAME} INTERFACE IMPORTED)
    add_dependencies(${TARGET_NAME} ${TARGET_NAME}_internal)

    set_property(TARGET ${TARGET_NAME} APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LIB_INCLUDE_DIR})
    message(STATUS "  - Target include dir: ${LIB_INCLUDE_DIR}")
endfunction(leanstore_add_ext_header_only_lib)
