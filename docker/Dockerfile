FROM ubuntu:23.04

################################################################################
# install prerequisted libriaries
################################################################################
RUN apt-get update && apt-get install -y \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        build-essential g++-aarch64-linux-gnu gcc-aarch64-linux-gnu \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        git cmake make gcc-13 g++-13 ninja-build \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        libaio-dev python3-pip cppcheck \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install gcovr==6.0 --break-system-packages

RUN ln -sf /usr/bin/cpp-13        /usr/bin/cpp \
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
 && ln -sf /usr/bin/gcc-13        /usr/bin/cc

# install vcpkg and its prerequisites
RUN apt-get update && apt-get install -y \
        curl zip unzip tar pkg-config \
 && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/Microsoft/vcpkg.git /opt/vcpkg \
 && echo 'export VCPKG_ROOT=/opt/vcpkg' >> ~/.bashrc \
 && echo 'export PATH=$VCPKG_ROOT:$PATH' >> ~/.bashrc \
 && echo 'export VCPKG_FORCE_SYSTEM_BINARIES=1' >> ~/.bashrc

# config vcpkg triplets
RUN mv /opt/vcpkg/triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux-static.cmake
COPY vcpkg-triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux.cmake
COPY vcpkg-triplets/arm64-linux.cmake /opt/vcpkg/triplets/arm64-linux.cmake


################################################################################
# other settings
################################################################################
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]
