# set(LIB_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR})

include(${CMAKE_SOURCE_DIR}/cmake/Modules/LeanStoreAddExternalLib.cmake)


# find_package(Git REQUIRED)
# include(ExternalProject)

# # Define a cmake function to add an external lib
# function(leanstore_add_ext_lib TARGET_NAME LIB_NAME GIT_REPO GIT_TAG)
#     set(TARGET_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-prefix)
#     set(TARGET_INSTALL ${CMAKE_CURRENT_BINARY_DIR}/third-party/${TARGET_NAME}-install)
#     ExternalProject_Add(${TARGET_NAME}_internal
#         PREFIX ${TARGET_PREFIX}
#         GIT_REPOSITORY ${GIT_REPO}
#         GIT_TAG ${GIT_TAG}
#         CMAKE_ARGS
#             -DCMAKE_INSTALL_PREFIX=${TARGET_INSTALL}
#             -DCMAKE_PREFIX_PATH=${TARGET_INSTALL}
#         UPDATE_COMMAND ""
#     )
#
#     add_library(${TARGET_NAME} INTERFACE)
#     add_dependencies(${TARGET_NAME} ${TARGET_NAME}_internal)
#     target_include_directories(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/include)
#     target_link_libraries(${TARGET_NAME} INTERFACE ${TARGET_INSTALL}/lib/${LIB_NAME})
#     list(APPEND CMAKE_PREFIX_PATH ${TARGET_INSTALL}/lib/cmake/${TARGET_NAME})
#
#     message(STATUS "${TARGET_NAME} added.")
#     message(STATUS "${TARGET_NAME} header path: ${TARGET_INSTALL}/include")
#     message(STATUS "${TARGET_NAME} lib path: ${TARGET_INSTALL}/lib/${LIB_NAME}")
# endfunction(leanstore_add_ext_lib)

leanstore_add_ext_lib(gflags libgflags.a "https://github.com/gflags/gflags.git" v2.2.2)

# set(gflags_prefix ${CMAKE_CURRENT_BINARY_DIR}/third-party/gflags-prefix)
# set(gflags_install_dir ${CMAKE_CURRENT_BINARY_DIR}/third-party/gflags-install)
# ExternalProject_Add(gflags
#     PREFIX ${gflags_prefix}
#     GIT_REPOSITORY "https://github.com/gflags/gflags.git"
#     GIT_TAG v2.2.2
#     CMAKE_ARGS
#         -DCMAKE_INSTALL_PREFIX=${gflags_install_dir}
#     UPDATE_COMMAND ""
# )
#
# add_library(gflags_ext INTERFACE)
# add_dependencies(gflags_ext gflags)
# set(gflags_ext_include_dir ${gflags_install_dir}/include)
# set(gflags_ext_lib ${gflags_install_dir}/lib/libgflags.a)
# target_include_directories(gflags_ext INTERFACE ${gflags_ext_include_dir})
# target_link_libraries(gflags_ext INTERFACE ${gflags_ext_lib})
#
# message(STATUS "Include directories for gflags_ext: ${gflags_ext_include_dir}")
# message(STATUS "Link Libraries for gflags_ext: ${gflags_ext_lib}")