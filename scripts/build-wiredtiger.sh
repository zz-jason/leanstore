#!/bin/bash
# Build WiredTiger from source
# Usage: ./scripts/build-wiredtiger.sh [OPTIONS]
#
# Options:
#   --version VERSION      WiredTiger version/tag (default: 11.3.1)
#   --build-type TYPE      Debug or Release (default: Release)
#   --install-prefix DIR   Installation directory (required)
#   --source-dir DIR       Source directory (will clone if not exists)
#   --jobs N               Parallel jobs (default: nproc)
#   --clone-only           Only clone, don't build

set -e

# Default values
WT_VERSION="11.3.1"
BUILD_TYPE="Release"
INSTALL_PREFIX=""
SOURCE_DIR=""
JOBS=$(nproc 2>/dev/null || echo 4)
CLONE_ONLY=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --version)
            WT_VERSION="$2"
            shift 2
            ;;
        --build-type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        --install-prefix)
            INSTALL_PREFIX="$2"
            shift 2
            ;;
        --source-dir)
            SOURCE_DIR="$2"
            shift 2
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --clone-only)
            CLONE_ONLY=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate required arguments
if [ -z "$INSTALL_PREFIX" ] && [ "$CLONE_ONLY" -eq 0 ]; then
    echo "Error: --install-prefix is required"
    exit 1
fi

if [ -z "$SOURCE_DIR" ]; then
    echo "Error: --source-dir is required"
    exit 1
fi

# Common cmake flags for WiredTiger
WT_C_FLAGS="-Wno-error=array-bounds -Wno-array-bounds -Wno-error=format-overflow -Wno-error=unterminated-string-initialization"
WT_CXX_FLAGS="$WT_C_FLAGS"

# Clone if source directory doesn't exist
if [ ! -d "$SOURCE_DIR" ]; then
    echo "Cloning WiredTiger ${WT_VERSION} to ${SOURCE_DIR}..."
    git clone --depth 1 --branch "${WT_VERSION}" \
        https://github.com/wiredtiger/wiredtiger.git "$SOURCE_DIR"
fi

if [ "$CLONE_ONLY" -eq 1 ]; then
    echo "Clone complete."
    exit 0
fi

# Build directory
BUILD_DIR="${SOURCE_DIR}/build-${BUILD_TYPE,,}"
mkdir -p "$BUILD_DIR"

echo "Configuring WiredTiger ${WT_VERSION} (${BUILD_TYPE})..."
cmake -S "$SOURCE_DIR" -B "$BUILD_DIR" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DENABLE_STATIC=ON \
    -DENABLE_SHARED=OFF \
    -DENABLE_PYTHON=OFF \
    -DENABLE_JAVA=OFF \
    -DENABLE_TEST=OFF \
    -DENABLE_EXAMPLES=OFF \
    -DENABLE_ZLIB=ON \
    "-DCMAKE_C_FLAGS=${WT_C_FLAGS}" \
    "-DCMAKE_CXX_FLAGS=${WT_CXX_FLAGS}"

echo "Building WiredTiger with ${JOBS} parallel jobs..."
cmake --build "$BUILD_DIR" --parallel "$JOBS"

echo "Installing WiredTiger to ${INSTALL_PREFIX}..."
cmake --install "$BUILD_DIR"

echo "WiredTiger ${WT_VERSION} (${BUILD_TYPE}) installed to ${INSTALL_PREFIX}"
