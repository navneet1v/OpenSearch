#!/bin/bash
#
# build-native.sh - Build io_uring native library for Panama (Java 25)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

NATIVE_DIR="${PROJECT_ROOT}/native"
BUILD_DIR="${NATIVE_DIR}/build"
INSTALL_DIR="${PROJECT_ROOT}/dist/native"

LIB_NAME="opensearch_iouring"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

log_info "Building native io_uring library"

# Prereqs
command -v cmake >/dev/null || { log_error "cmake not found"; exit 1; }
pkg-config --exists liburing || { log_error "liburing not found"; exit 1; }

log_info "liburing version: $(pkg-config --modversion liburing)"

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}"

cd "${BUILD_DIR}"

log_info "Configuring CMake..."
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="${INSTALL_DIR}" \
  -DBUILD_SHARED_LIBS=ON

log_info "Building..."
make -j$(nproc)

log_info "Installing..."
make install

SO_PATH="${INSTALL_DIR}/lib64/lib${LIB_NAME}.so"
HEADER_PATH="${INSTALL_DIR}/include/opensearch_iouring.h"

if [[ ! -f "${SO_PATH}" ]]; then
  log_error "Shared library not found: ${SO_PATH}"
  exit 1
fi

if [[ ! -f "${HEADER_PATH}" ]]; then
  log_error "Header not found: ${HEADER_PATH}"
  exit 1
fi

# Copy shared lib into resources (OpenSearch plugin layout)
RES_DIR="${PROJECT_ROOT}/src/main/resources/native/linux-x86_64"
mkdir -p "${RES_DIR}"
cp "${SO_PATH}" "${RES_DIR}/"

log_info "Native build complete"
log_info "  Library: ${SO_PATH}"
log_info "  Header : ${HEADER_PATH}"
