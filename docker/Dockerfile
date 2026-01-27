FROM ubuntu:25.10


# install prerequisted libriaries
RUN apt update \
 && apt install -y \
        build-essential g++-aarch64-linux-gnu gcc-aarch64-linux-gnu \
        git cmake make gcc g++ clangd clang-format clang-tidy ninja-build \
        libaio-dev python3-pip cppcheck \
        ca-certificates curl zip unzip tar pkg-config \
        libssl-dev bison libtirpc-dev bash \
        autoconf libtool golang graphviz sysstat jq htop vim \
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

COPY docker/vcpkg-triplets/x64-linux.cmake /opt/vcpkg/triplets/x64-linux.cmake
COPY docker/vcpkg-triplets/arm64-linux.cmake /opt/vcpkg/triplets/arm64-linux.cmake

ENV VCPKG_ROOT="/opt/vcpkg"
ENV PATH="$VCPKG_ROOT:$PATH"
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV VCPKG_DEFAULT_BINARY_CACHE=/opt/vcpkg_cache


# install vcpkg packages
RUN ./opt/vcpkg/bootstrap-vcpkg.sh \
 && vcpkg install benchmark tanakh-cmdline spdlog gtest rapidjson gperftools \
          cpp-httplib crc32c libunwind cpptrace boost-context rocksdb


# install gcov
RUN pip3 install gcovr==6.0 --break-system-packages


# pre-build WiredTiger for faster CI builds
# Copy the build script first (this script is shared with wiredtiger.cmake for DRY)
COPY scripts/build-wiredtiger.sh /tmp/build-wiredtiger.sh
RUN chmod +x /tmp/build-wiredtiger.sh

ARG WT_VERSION=11.3.1
RUN /tmp/build-wiredtiger.sh \
      --version ${WT_VERSION} \
      --build-type Debug \
      --source-dir /tmp/wiredtiger \
      --install-prefix /opt/wiredtiger/${WT_VERSION}/Debug \
 && /tmp/build-wiredtiger.sh \
      --version ${WT_VERSION} \
      --build-type Release \
      --source-dir /tmp/wiredtiger \
      --install-prefix /opt/wiredtiger/${WT_VERSION}/Release \
 && rm -rf /tmp/wiredtiger /tmp/build-wiredtiger.sh

ENV WT_PREBUILT_DIR=/opt/wiredtiger


# other settings
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]
