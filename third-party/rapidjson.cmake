set(LIB_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR})

find_package(Git REQUIRED)
include(ExternalProject)
ExternalProject_Add(rapidjson_src
    GIT_REPOSITORY "https://github.com/Tencent/rapidjson.git"
    GIT_TAG v1.1.0
    CONFIGURE_COMMAND ""
    BUILD_COMMAND
        COMMAND ${CMAKE_COMMAND} -E copy_directory <SOURCE_DIR>/include/rapidjson ${LIB_INCLUDE_DIR}/rapidjson
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
)

add_library(rapidjson INTERFACE IMPORTED)
set_property(TARGET rapidjson APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LIB_INCLUDE_DIR})
add_dependencies(rapidjson rapidjson_src)
