FROM ubuntu:23.04

################################################################################
# install prerequisted libriaries
################################################################################
RUN apt-get update && apt-get install -y \
        git cmake make gcc-13 g++-13 libaio-dev python3-pip cppcheck \
 && ln -sf /usr/bin/cpp-13        /usr/bin/cpp \
 && ln -sf /usr/bin/g++-13        /usr/bin/g++ \
 && ln -sf /usr/bin/gcc-13        /usr/bin/gcc \
 && ln -sf /usr/bin/gcc-ar-13     /usr/bin/gcc-ar \
 && ln -sf /usr/bin/gcc-nm-13     /usr/bin/gcc-nm \
 && ln -sf /usr/bin/gcc-ranlib-13 /usr/bin/gcc-ranlib \
 && ln -sf /usr/bin/gcov-13       /usr/bin/gcov \
 && ln -sf /usr/bin/gcov-dump-13  /usr/bin/gcov-dump \
 && ln -sf /usr/bin/gcov-tool-13  /usr/bin/gcov-tool \
 && ln -sf /usr/bin/lto-dump-13   /usr/bin/lto-dump \
 && ln -sf /usr/bin/g++-13        /usr/bin/c++ \
 && ln -sf /usr/bin/gcc-13        /usr/bin/cc \
 && pip3 install gcovr==6.0 --break-system-packages \
 && rm -rf /var/lib/apt/lists/*

# install vcpkg and its prerequisites
RUN apt-get update && apt-get install -y \
        curl zip unzip tar pkg-config \
 && git clone https://github.com/Microsoft/vcpkg.git /opt/vcpkg \
 && echo 'export VCPKG_ROOT=/opt/vcpkg' >> ~/.bashrc \
 && echo 'export PATH=$VCPKG_ROOT:$PATH' >> ~/.bashrc \
 && rm -rf /var/lib/apt/lists/*

# config vcpkg triplets
RUN mv /opt/vcpkg/triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux-static.cmake
COPY vcpkg-triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux.cmake
COPY vcpkg-triplets/arm64-linux.cmake /opt/vcpkg/triplets/arm64-linux.cmake

# use ninja-build
RUN apt-get update && apt-get install -y \
        ninja-build \
 && rm -rf /var/lib/apt/lists/*


################################################################################
# other settings
################################################################################
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]
