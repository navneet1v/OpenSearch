#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

#
# build-native.sh - Build the native IO-uring wrapper library
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
NATIVE_DIR="${PROJECT_ROOT}/native"
BUILD_DIR="${NATIVE_DIR}/build"
INSTALL_DIR="${PROJECT_ROOT}/dist/native"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "╔════════════════════════════════════════════════════════════╗"
echo "║        Building OpenSearch IO-uring Native Library         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check prerequisites
if ! command -v cmake &> /dev/null; then
    log_error "cmake not found. Please run scripts/setup-al2023.sh first."
    exit 1
fi

if ! pkg-config --exists liburing; then
    log_error "liburing not found. Please run scripts/setup-al2023.sh first."
    exit 1
fi

log_info "liburing version: $(pkg-config --modversion liburing)"

# Clean and create build directory
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"
mkdir -p "${INSTALL_DIR}"

cd "${BUILD_DIR}"

# Configure
log_info "Configuring with CMake..."
cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" \
    -DBUILD_TESTS=ON \
    -DBUILD_SHARED_LIBS=ON

# Build
log_info "Building..."
make -j$(nproc)

# Run tests
log_info "Running native tests..."
if ctest --output-on-failure; then
    log_info "All native tests passed ✓"
else
    log_error "Native tests failed!"
    exit 1
fi

# Install
log_info "Installing to ${INSTALL_DIR}..."
make install

# Verify installation
if [[ -f "${INSTALL_DIR}/lib64/libopensearch_iouring.so" ]]; then
    log_info "Library built successfully ✓"
    log_info "  Location: ${INSTALL_DIR}/lib64/libopensearch_iouring.so"
    log_info "  Header:   ${INSTALL_DIR}/include/opensearch_iouring.h"
else
    log_error "Library not found after installation!"
    exit 1
fi

# Copy to resources for JAR packaging
RESOURCES_DIR="${PROJECT_ROOT}/src/main/resources/native/linux-x86_64"
mkdir -p "${RESOURCES_DIR}"
cp "${INSTALL_DIR}/lib64/libopensearch_iouring.so" "${RESOURCES_DIR}/"
log_info "Copied library to resources: ${RESOURCES_DIR}"

echo ""
log_info "Native build complete!"
echo ""
