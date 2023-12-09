find_package(Git REQUIRED)
include(ExternalProject)

# Define a cmake function to add an external lib
function(leanstore_add_ext_lib TARGET_NAME LIB_NAME GIT_REPO GIT_TAG)
    set(TARGET_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-prefix)
    set(TARGET_INSTALL ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-install)
    ExternalProject_Add(${TARGET_NAME}_internal
        PREFIX ${TARGET_PREFIX}
        GIT_REPOSITORY ${GIT_REPO}
        GIT_TAG ${GIT_TAG}
        CMAKE_ARGS
            -DCMAKE_INSTALL_PREFIX=${TARGET_INSTALL}
            -DCMAKE_PREFIX_PATH=${TARGET_INSTALL}
        UPDATE_COMMAND ""
    )

    add_library(${TARGET_NAME} INTERFACE)
    add_dependencies(${TARGET_NAME} ${TARGET_NAME}_internal)
    target_include_directories(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/include)
    target_link_libraries(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/lib/${LIB_NAME})
    list(APPEND CMAKE_PREFIX_PATH ${TARGET_INSTALL}/lib/cmake/${TARGET_NAME})

    message(STATUS "External lib added: ${TARGET_NAME}")
    message(STATUS "  - Include dir: ${TARGET_INSTALL}/include")
    message(STATUS "  - Lib path: ${TARGET_INSTALL}/lib/${LIB_NAME}")
endfunction(leanstore_add_ext_lib)
