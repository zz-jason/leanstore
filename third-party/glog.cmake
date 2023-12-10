include(${CMAKE_SOURCE_DIR}/cmake/Modules/LeanStoreAddExternalLib.cmake)
leanstore_add_ext_lib(glog libglog.so "https://github.com/google/glog.git" v0.6.0)
