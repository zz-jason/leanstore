FROM ubuntu:23.04

################################################################################
# install prerequisted libriaries
################################################################################
RUN apt-get update && apt-get install -y \
        build-essential cmake git libaio-dev gcovr \
 && rm -rf /var/lib/apt/lists/*


################################################################################
# other settings
################################################################################
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]
