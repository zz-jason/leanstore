FROM ubuntu:23.04

################################################################################
# install prerequisted libriaries
################################################################################
RUN apt-get update && apt-get install -y \
        build-essential cmake git libaio-dev python3-pip \
 && apt remove gcc g++ -y \
 && apt install -y gcc-13 g++-13 -y \
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


################################################################################
# other settings
################################################################################
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]
