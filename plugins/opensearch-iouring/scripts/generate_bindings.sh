#!/bin/bash
#
# generate-bindings.sh - Generate Panama bindings (Java 25)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

HEADER_FILE="${PROJECT_ROOT}/dist/native/include/opensearch_iouring.h"
OUTPUT_DIR="${PROJECT_ROOT}/src/main/generated"

PACKAGE="org.opensearch.index.store.iouring.ffi"
LIB_NAME="opensearch_iouring"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Locate jextract (Java 25)
if [[ -n "${JEXTRACT_CMD:-}" ]]; then
  JEXTRACT="${JEXTRACT_CMD}"
elif command -v jextract >/dev/null; then
  JEXTRACT="$(command -v jextract)"
else
  log_error "jextract not found (Java 25 required)"
  exit 1
fi

log_info "Using jextract: ${JEXTRACT}"

if [[ ! -f "${HEADER_FILE}" ]]; then
  log_error "Header not found: ${HEADER_FILE}"
  exit 1
fi

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

log_info "Generating bindings"
log_info "  Header : ${HEADER_FILE}"
log_info "  Output : ${OUTPUT_DIR}"
log_info "  Package: ${PACKAGE}"

"${JEXTRACT}" \
  --output "${OUTPUT_DIR}" \
  --target-package "${PACKAGE}" \
  --library "${LIB_NAME}" \
  --header-class-name "opensearch_iouring_h" \
  --include-function "osur_ring_create" \
  --include-function "osur_ring_destroy" \
  --include-function "osur_submit_read" \
  --include-function "osur_submit" \
  --include-function "osur_poll_completion" \
  --include-function "osur_wait_completion" \
  --include-typedef "osur_ring_t" \
  --include-typedef "osur_completion_t" \
  --include-constant OSUR_RING_DEFAULT \
  "${HEADER_FILE}"

COUNT=$(find "${OUTPUT_DIR}" -name "*.java" | wc -l)
log_info "Generated ${COUNT} Java files"
