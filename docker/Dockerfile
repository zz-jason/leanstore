FROM ubuntu:24.10


# install prerequisted libriaries
RUN apt update \
 && apt install -y \
        build-essential g++-aarch64-linux-gnu gcc-aarch64-linux-gnu \
        git cmake make gcc g++ clang-format ninja-build \
        libaio-dev python3-pip cppcheck \
        curl zip unzip tar pkg-config \
        autoconf libtool golang graphviz sysstat \
 && rm -rf /var/lib/apt/lists/*


# env.sh
RUN rm -rf /opt/env.sh \
 && touch /opt/env.sh \
 && echo 'source /opt/env.sh' >> ~/.bashrc

# install vcpkg
RUN git clone --depth 1 https://github.com/Microsoft/vcpkg.git /opt/vcpkg \
 && mv /opt/vcpkg/triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux-static.cmake \
 && mkdir -p /opt/vcpkg_cache \
 && echo 'export VCPKG_ROOT=/opt/vcpkg' >> /opt/env.sh \
 && echo 'export PATH=$VCPKG_ROOT:$PATH' >> /opt/env.sh \
 && echo 'export VCPKG_FORCE_SYSTEM_BINARIES=1' >> /opt/env.sh \
 && echo 'export VCPKG_DEFAULT_BINARY_CACHE=/opt/vcpkg_cache' > /opt/env.sh

COPY vcpkg-triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux.cmake
COPY vcpkg-triplets/arm64-linux.cmake /opt/vcpkg/triplets/arm64-linux.cmake

ENV VCPKG_ROOT="/opt/vcpkg"
ENV PATH="$VCPKG_ROOT:$PATH"
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV VCPKG_DEFAULT_BINARY_CACHE=/opt/vcpkg_cache


# install vcpkg packages
RUN ./opt/vcpkg/bootstrap-vcpkg.sh \
 && vcpkg install benchmark tanakh-cmdline spdlog gtest rapidjson gperftools \
          cpp-httplib crc32c libunwind cpptrace concurrentqueue boost-context rocksdb


# install gcov
RUN pip3 install gcovr==6.0 --break-system-packages


# other settings
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]
