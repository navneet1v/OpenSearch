#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

#
# generate-bindings.sh - Generate Java bindings using jextract
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
NATIVE_DIR="${PROJECT_ROOT}/native"
OUTPUT_DIR="${PROJECT_ROOT}/src/main/generated"
HEADER_FILE="${NATIVE_DIR}/include/opensearch_iouring.h"

# Configuration
PACKAGE="org.opensearch.index.store.iouring.native"
LIBRARY_NAME="opensearch_iouring"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "╔════════════════════════════════════════════════════════════╗"
echo "║        Generating Java Bindings with Jextract              ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Find jextract
JEXTRACT_CMD=""
if command -v jextract &> /dev/null; then
    JEXTRACT_CMD="jextract"
elif [[ -n "${JEXTRACT_HOME:-}" ]] && [[ -x "${JEXTRACT_HOME}/bin/jextract" ]]; then
    JEXTRACT_CMD="${JEXTRACT_HOME}/bin/jextract"
elif [[ -x "/opt/jextract-22/bin/jextract" ]]; then
    JEXTRACT_CMD="/opt/jextract-22/bin/jextract"
else
    log_error "jextract not found!"
    log_error "Please run scripts/setup-al2023.sh or set JEXTRACT_HOME"
    exit 1
fi

log_info "Using jextract: ${JEXTRACT_CMD}"
log_info "Jextract version: $(${JEXTRACT_CMD} --version 2>&1 | head -1)"

# Check header exists
if [[ ! -f "${HEADER_FILE}" ]]; then
    log_error "Header file not found: ${HEADER_FILE}"
    log_error "Please run scripts/build-native.sh first"
    exit 1
fi

# Clean output directory
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

log_info "Generating bindings..."
log_info "  Header:  ${HEADER_FILE}"
log_info "  Output:  ${OUTPUT_DIR}"
log_info "  Package: ${PACKAGE}"
log_info "  Library: ${LIBRARY_NAME}"
echo ""

# Run jextract
${JEXTRACT_CMD} \
    --source \
    --output "${OUTPUT_DIR}" \
    --target-package "${PACKAGE}" \
    --library "${LIBRARY_NAME}" \
    --header-class-name "opensearch_iouring_h" \
    \
    --include-function "osur_is_supported" \
    --include-function "osur_version" \
    --include-function "osur_strerror" \
    \
    --include-function "osur_ring_create" \
    --include-function "osur_ring_destroy" \
    --include-function "osur_ring_queue_depth" \
    --include-function "osur_ring_pending" \
    \
    --include-function "osur_register_files" \
    --include-function "osur_update_registered_file" \
    --include-function "osur_unregister_files" \
    \
    --include-function "osur_register_buffers" \
    --include-function "osur_unregister_buffers" \
    \
    --include-function "osur_submit_read" \
    --include-function "osur_submit_write" \
    --include-function "osur_submit_read_registered" \
    --include-function "osur_submit_write_registered" \
    --include-function "osur_submit_read_fixed" \
    --include-function "osur_submit_fsync" \
    --include-function "osur_submit" \
    --include-function "osur_submit_and_wait" \
    \
    --include-function "osur_poll_completion" \
    --include-function "osur_wait_completion" \
    --include-function "osur_wait_completion_timeout" \
    --include-function "osur_poll_completions" \
    --include-function "osur_completion_ready" \
    \
    --include-function "osur_alloc_aligned" \
    --include-function "osur_free_aligned" \
    \
    --include-struct "osur_buffer_t" \
    --include-struct "osur_completion_t" \
    \
    --include-constant "OSUR_VERSION" \
    --include-constant "OSUR_RING_DEFAULT" \
    --include-constant "OSUR_RING_SQPOLL" \
    --include-constant "OSUR_RING_IOPOLL" \
    --include-constant "OSUR_RING_SINGLE_ISSUER" \
    \
    "${HEADER_FILE}"

# Count generated files
FILE_COUNT=$(find "${OUTPUT_DIR}" -name "*.java" -type f | wc -l)

echo ""
log_info "Generated ${FILE_COUNT} Java files:"
find "${OUTPUT_DIR}" -name "*.java" -type f | sort | while read -r f; do
    echo "    $(basename "$f")"
done

echo ""
log_info "Bindings generated successfully! ✓"
echo ""
