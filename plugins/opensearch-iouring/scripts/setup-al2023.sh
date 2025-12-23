#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

#
# setup-al2023.sh - One-time setup for IO-uring OpenSearch plugin on AL2023
#
# Usage: sudo ./scripts/setup-al2023.sh
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Configuration
LIBURING_VERSION="2.5"
JEXTRACT_VERSION="25"
JEXTRACT_BUILD="5-33"
CORRETTO_VERSION="25"

INSTALL_PREFIX="/usr/local"
JEXTRACT_INSTALL_DIR="/opt/jextract-${JEXTRACT_VERSION}"
JAVA_INSTALL_DIR="/opt/corretto-${JEXTRACT_VERSION}"

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║     OpenSearch IO-uring Plugin - AL2023 Setup                      ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    log_error "This script must be run as root (use sudo)"
    exit 1
fi

# Check AL2023
if ! grep -q "Amazon Linux 2023" /etc/os-release 2>/dev/null; then
    log_warn "This script is designed for Amazon Linux 2023"
    read -p "Continue anyway? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# ============================================================================
# Step 1: Install system dependencies
# ============================================================================
log_info "Step 1: Installing system dependencies..."

dnf update -y
dnf install -y \
    gcc \
    gcc-c++ \
    cmake \
    make \
    git \
    tar \
    gzip \
    wget \
    pkg-config \
    kernel-devel \
    kernel-headers

log_info "System dependencies installed ✓"

# ============================================================================
# Step 2: Install liburing from source
# ============================================================================
log_info "Step 2: Installing liburing ${LIBURING_VERSION}..."

LIBURING_INSTALLED=$(pkg-config --modversion liburing 2>/dev/null || echo "none")

if [[ "$LIBURING_INSTALLED" == "$LIBURING_VERSION" ]]; then
    log_info "liburing ${LIBURING_VERSION} already installed ✓"
else
    cd /tmp
    rm -rf liburing-liburing-${LIBURING_VERSION}

    log_info "Downloading liburing ${LIBURING_VERSION}..."
    wget -q "https://github.com/axboe/liburing/archive/refs/tags/liburing-${LIBURING_VERSION}.tar.gz"
    tar -xzf "liburing-${LIBURING_VERSION}.tar.gz"
    cd "liburing-liburing-${LIBURING_VERSION}"

    log_info "Building liburing..."
    ./configure --prefix=${INSTALL_PREFIX}
    make -j$(nproc)
    make install

    # Update library cache
    echo "${INSTALL_PREFIX}/lib" > /etc/ld.so.conf.d/liburing.conf
    ldconfig

    cd /tmp
    rm -rf liburing-liburing-${LIBURING_VERSION} "liburing-${LIBURING_VERSION}.tar.gz"

    log_info "liburing ${LIBURING_VERSION} installed ✓"
fi

# Verify liburing installation
if ! pkg-config --exists liburing; then
    log_error "liburing installation verification failed"
    exit 1
fi

# ============================================================================
# Step 3: Install Amazon Corretto 22 (for FFM API)
# ============================================================================
log_info "Step 3: Installing Amazon Corretto 25..."

if [[ -d "${JAVA_INSTALL_DIR}" ]]; then
    log_info "Corretto 22 already installed ✓"
else
    cd /tmp

    CORRETTO_URL="https://corretto.aws/downloads/resources/${CORRETTO_VERSION}/amazon-corretto-${CORRETTO_VERSION}-linux-x64.tar.gz"
    log_info "Downloading Corretto 25..."
    wget -q "${CORRETTO_URL}" -O corretto.tar.gz

    mkdir -p "${JAVA_INSTALL_DIR}"
    tar -xzf corretto.tar.gz -C "${JAVA_INSTALL_DIR}" --strip-components=1

    rm corretto.tar.gz

    log_info "Corretto 25 installed to ${JAVA_INSTALL_DIR} ✓"
fi

# Set up alternatives
update-alternatives --install /usr/bin/java java "${JAVA_INSTALL_DIR}/bin/java" 2200
update-alternatives --install /usr/bin/javac javac "${JAVA_INSTALL_DIR}/bin/javac" 2200

# ============================================================================
# Step 4: Install Jextract
# ============================================================================
log_info "Step 4: Installing Jextract ${JEXTRACT_VERSION}..."

if [[ -d "${JEXTRACT_INSTALL_DIR}" ]]; then
    log_info "Jextract already installed ✓"
else
    cd /tmp

    JEXTRACT_URL="https://download.java.net/java/early_access/jextract/${JEXTRACT_VERSION}/${JEXTRACT_BUILD}/openjdk-${JEXTRACT_VERSION}-jextract+${JEXTRACT_BUILD}_linux-x64_bin.tar.gz"
    log_info "Downloading Jextract..."
    wget -q "${JEXTRACT_URL}" -O jextract.tar.gz

    mkdir -p "${JEXTRACT_INSTALL_DIR}"
    tar -xzf jextract.tar.gz -C "${JEXTRACT_INSTALL_DIR}" --strip-components=1

    rm jextract.tar.gz

    # Create symlink
    ln -sf "${JEXTRACT_INSTALL_DIR}/bin/jextract" /usr/local/bin/jextract

    log_info "Jextract installed to ${JEXTRACT_INSTALL_DIR} ✓"
fi

# ============================================================================
# Step 5: Verify IO-uring kernel support
# ============================================================================
log_info "Step 5: Verifying IO-uring kernel support..."

cat > /tmp/test_iouring.c << 'EOF'
#include <liburing.h>
#include <stdio.h>
#include <string.h>

int main() {
    struct io_uring ring;
    int ret = io_uring_queue_init(8, &ring, 0);
    if (ret < 0) {
        printf("IO-uring NOT supported: %s\n", strerror(-ret));
        return 1;
    }
    printf("IO-uring supported ✓\n");
    printf("  Kernel: %s\n", "$(uname -r)");
    printf("  SQ entries: %u\n", ring.sq.ring_entries);
    printf("  CQ entries: %u\n", ring.cq.ring_entries);
    io_uring_queue_exit(&ring);
    return 0;
}
EOF

gcc /tmp/test_iouring.c -o /tmp/test_iouring $(pkg-config --cflags --libs liburing)
/tmp/test_iouring
rm -f /tmp/test_iouring /tmp/test_iouring.c

# ============================================================================
# Step 6: Create environment file
# ============================================================================
log_info "Step 6: Creating environment configuration..."

cat > /etc/profile.d/opensearch-iouring.sh << EOF
# OpenSearch IO-uring environment
export JAVA_HOME=${JAVA_INSTALL_DIR}
export JEXTRACT_HOME=${JEXTRACT_INSTALL_DIR}
export PATH=\${JAVA_HOME}/bin:\${JEXTRACT_HOME}/bin:\${PATH}
export PKG_CONFIG_PATH=${INSTALL_PREFIX}/lib/pkgconfig:\${PKG_CONFIG_PATH}
EOF

chmod 644 /etc/profile.d/opensearch-iouring.sh

echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║                    SETUP COMPLETE                                  ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Installed components:"
echo "  • liburing:  $(pkg-config --modversion liburing)"
echo "  • Java:      ${JAVA_INSTALL_DIR}"
echo "  • Jextract:  ${JEXTRACT_INSTALL_DIR}"
echo ""
echo "To activate the environment in your current shell, run:"
echo "  source /etc/profile.d/opensearch-iouring.sh"
echo ""
echo "Or log out and log back in."
echo ""
