#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

#
# run-opensearch.sh - Run OpenSearch with IO-uring support
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
NATIVE_LIB_DIR="${PROJECT_ROOT}/dist/native/lib"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Default OpenSearch home
OPENSEARCH_HOME="${OPENSEARCH_HOME:-/usr/share/opensearch}"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║        Running OpenSearch with IO-uring Support            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check native library exists
if [[ ! -f "${NATIVE_LIB_DIR}/libopensearch_iouring.so" ]]; then
    log_error "Native library not found!"
    log_error "Please run: ./scripts/build-native.sh"
    exit 1
fi

# Check OpenSearch exists
if [[ ! -d "${OPENSEARCH_HOME}" ]]; then
    log_error "OpenSearch not found at: ${OPENSEARCH_HOME}"
    log_error "Set OPENSEARCH_HOME environment variable"
    exit 1
fi

# Check plugin is installed
PLUGIN_DIR="${OPENSEARCH_HOME}/plugins/opensearch-iouring"
if [[ ! -d "${PLUGIN_DIR}" ]]; then
    log_warn "Plugin not installed. Installing..."

    # Build the plugin first
    cd "${PROJECT_ROOT}"
    ./gradlew build

    # Install plugin
    PLUGIN_ZIP=$(find "${PROJECT_ROOT}/build/distributions" -name "*.zip" | head -1)
    if [[ -z "${PLUGIN_ZIP}" ]]; then
        log_error "Plugin ZIP not found. Build may have failed."
        exit 1
    fi

    "${OPENSEARCH_HOME}/bin/opensearch-plugin" install "file://${PLUGIN_ZIP}"
fi

log_info "Native library: ${NATIVE_LIB_DIR}/libopensearch_iouring.so"
log_info "OpenSearch:     ${OPENSEARCH_HOME}"

# Set up environment
export LD_LIBRARY_PATH="${NATIVE_LIB_DIR}:${LD_LIBRARY_PATH:-}"

# Enable native access for Panama FFM
export OPENSEARCH_JAVA_OPTS="${OPENSEARCH_JAVA_OPTS:-} --enable-native-access=ALL-UNNAMED"

# Optional: Add library path to JVM options
export OPENSEARCH_JAVA_OPTS="${OPENSEARCH_JAVA_OPTS} -Djava.library.path=${NATIVE_LIB_DIR}"

log_info "LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}"
log_info "Starting OpenSearch..."
echo ""

# Start OpenSearch
exec "${OPENSEARCH_HOME}/bin/opensearch" "$@"
