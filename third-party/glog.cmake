include(${CMAKE_SOURCE_DIR}/cmake/Modules/LeanStoreAddExternalLib.cmake)
leanstore_add_ext_lib(glog libglog.so "https://github.com/google/glog.git" v0.6.0)

# set(LIB_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR})
#
# find_package(Git REQUIRED)
# include(ExternalProject)
# set(glog_install_dir ${CMAKE_CURRENT_BINARY_DIR}/third-party/glog)
# ExternalProject_Add(glog_build
#     GIT_REPOSITORY "https://github.com/google/glog.git"
#     GIT_TAG v0.6.0
#     CMAKE_ARGS
#         -DCMAKE_INSTALL_PREFIX=${glog_install_dir}
#     UPDATE_COMMAND ""
# )
#
# add_library(glog_ext INTERFACE)
# add_dependencies(glog_ext glog_build)
# set(glog_ext_include_dir ${glog_install_dir}/include)
# set(glog_ext_lib ${glog_install_dir}/lib/libglog.so)
# target_include_directories(glog_ext INTERFACE ${glog_ext_include_dir})
# target_link_libraries(glog_ext INTERFACE ${glog_ext_lib})
#
# message(STATUS "Include directories for glog_ext: ${glog_ext_include_dir}")
# message(STATUS "Link Libraries for glog_ext: ${glog_ext_lib}")