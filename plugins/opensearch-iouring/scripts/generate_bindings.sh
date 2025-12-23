#!/bin/bash
#
# generate-bindings.sh - Generate Java bindings using jextract
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${PROJECT_ROOT}/src/main/generated"
HEADER_FILE="${PROJECT_ROOT}/native/include/opensearch_iouring.h"

# CHANGED: 'native' -> 'ffi' (Foreign Function Interface)
PACKAGE="org.opensearch.index.store.iouring.ffi"
LIBRARY_NAME="opensearch_iouring"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║        Generating Java Bindings with Jextract                      ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

# Find jextract
JEXTRACT=""

if [[ -n "${JEXTRACT_CMD:-}" ]] && [[ -x "${JEXTRACT_CMD}" ]]; then
    JEXTRACT="${JEXTRACT_CMD}"
elif command -v jextract &> /dev/null; then
    JEXTRACT="$(command -v jextract)"
elif [[ -n "${JEXTRACT_HOME:-}" ]] && [[ -x "${JEXTRACT_HOME}/bin/jextract" ]]; then
    JEXTRACT="${JEXTRACT_HOME}/bin/jextract"
else
    for path in /opt/jextract-22/bin/jextract /opt/jextract-21/bin/jextract /opt/jextract/bin/jextract /usr/local/bin/jextract; do
        if [[ -x "$path" ]]; then
            JEXTRACT="$path"
            break
        fi
    done
fi

if [[ -z "${JEXTRACT}" ]]; then
    log_error "jextract not found!"
    exit 1
fi

log_info "Using jextract: ${JEXTRACT}"

if [[ ! -f "${HEADER_FILE}" ]]; then
    log_error "Header not found: ${HEADER_FILE}"
    exit 1
fi

# Clean output
rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

log_info "Header:  ${HEADER_FILE}"
log_info "Output:  ${OUTPUT_DIR}"
log_info "Package: ${PACKAGE}"
echo ""

# Run jextract
"${JEXTRACT}" \
    --output "${OUTPUT_DIR}" \
    --target-package "${PACKAGE}" \
    --library "${LIBRARY_NAME}" \
    --header-class-name "opensearch_iouring_h" \
    --include-function "osur_is_supported" \
    --include-function "osur_version" \
    --include-function "osur_strerror" \
    --include-function "osur_ring_create" \
    --include-function "osur_ring_destroy" \
    --include-function "osur_ring_queue_depth" \
    --include-function "osur_ring_pending" \
    --include-function "osur_register_files" \
    --include-function "osur_update_registered_file" \
    --include-function "osur_unregister_files" \
    --include-function "osur_register_buffers" \
    --include-function "osur_unregister_buffers" \
    --include-function "osur_submit_read" \
    --include-function "osur_submit_write" \
    --include-function "osur_submit_read_registered" \
    --include-function "osur_submit_write_registered" \
    --include-function "osur_submit_read_fixed" \
    --include-function "osur_submit_fsync" \
    --include-function "osur_submit" \
    --include-function "osur_submit_and_wait" \
    --include-function "osur_poll_completion" \
    --include-function "osur_wait_completion" \
    --include-function "osur_wait_completion_timeout" \
    --include-function "osur_poll_completions" \
    --include-function "osur_completion_ready" \
    --include-function "osur_alloc_aligned" \
    --include-function "osur_free_aligned" \
    --include-typedef "osur_buffer_t" \
    --include-typedef "osur_completion_t" \
    --include-constant "OSUR_VERSION" \
    --include-constant "OSUR_RING_DEFAULT" \
    --include-constant "OSUR_RING_SQPOLL" \
    --include-constant "OSUR_RING_IOPOLL" \
    --include-constant "OSUR_RING_SINGLE_ISSUER" \
    "${HEADER_FILE}"

FILE_COUNT=$(find "${OUTPUT_DIR}" -name "*.java" | wc -l)
log_info "Generated ${FILE_COUNT} Java files ✓"