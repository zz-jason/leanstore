set(LIB_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR})

find_package(Git REQUIRED)
include(ExternalProject)
ExternalProject_Add(tabulate_src
    GIT_REPOSITORY "https://github.com/p-ranav/tabulate.git"
    GIT_TAG 718d827cf05c2e9bba17e926cac2d7ab2356621c
    CONFIGURE_COMMAND ""
    BUILD_COMMAND
        COMMAND ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR>/include/tabulate ${LIB_INCLUDE_DIR}/tabulate
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
)

add_library(tabluate INTERFACE IMPORTED)
set_property(TARGET tabluate APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${TABLUATE_INCLUDE_DIR})
add_dependencies(tabluate tabulate_src)
