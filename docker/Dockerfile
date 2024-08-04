FROM ubuntu:24.04

################################################################################
# install prerequisted libriaries
################################################################################
RUN apt-get update && apt-get install -y \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        build-essential g++-aarch64-linux-gnu gcc-aarch64-linux-gnu \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        git cmake make gcc g++ ninja-build \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        libaio-dev python3-pip cppcheck \
 && rm -rf /var/lib/apt/lists/*


# install vcpkg and its prerequisites
RUN apt-get update && apt-get install -y \
        curl zip unzip tar pkg-config \
 && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/Microsoft/vcpkg.git /opt/vcpkg \
 && echo 'export VCPKG_ROOT=/opt/vcpkg' >> ~/.bashrc \
 && echo 'export PATH=$VCPKG_ROOT:$PATH' >> ~/.bashrc \
 && echo 'export VCPKG_FORCE_SYSTEM_BINARIES=1' >> ~/.bashrc

RUN mv /opt/vcpkg/triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux-static.cmake
COPY vcpkg-triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux.cmake
COPY vcpkg-triplets/arm64-linux.cmake /opt/vcpkg/triplets/arm64-linux.cmake

RUN apt-get update && apt-get install -y \
        autoconf libtool golang graphviz sysstat \
 && rm -rf /var/lib/apt/lists/*


################################################################################
# other settings
################################################################################
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]
