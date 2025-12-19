#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

#
# build-all.sh - Complete build pipeline for IO-uring OpenSearch plugin
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║     OpenSearch IO-uring Plugin - Complete Build Pipeline           ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

cd "${PROJECT_ROOT}"

# Step 1: Build native library
echo ""
log_step "Step 1/4: Building native library..."
echo "────────────────────────────────────────────────────────────────────"
"${SCRIPT_DIR}/build-native.sh"

# Step 2: Generate Java bindings
echo ""
log_step "Step 2/4: Generating Java bindings with jextract..."
echo "────────────────────────────────────────────────────────────────────"
"${SCRIPT_DIR}/generate-bindings.sh"

# Step 3: Build Java plugin
echo ""
log_step "Step 3/4: Building Java plugin..."
echo "────────────────────────────────────────────────────────────────────"

# Source environment if available
if [[ -f /etc/profile.d/opensearch-iouring.sh ]]; then
    source /etc/profile.d/opensearch-iouring.sh
fi

./gradlew clean build -x test

# Step 4: Run tests
echo ""
log_step "Step 4/4: Running tests..."
echo "────────────────────────────────────────────────────────────────────"
./gradlew test

# Summary
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║                        BUILD COMPLETE                              ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""
log_info "Artifacts:"
echo ""
echo "  Native library:"
echo "    ${PROJECT_ROOT}/dist/native/lib/libopensearch_iouring.so"
echo ""
echo "  Plugin ZIP:"
find "${PROJECT_ROOT}/build/distributions" -name "*.zip" 2>/dev/null | head -1 | while read -r f; do
    echo "    $f"
done
echo ""
log_info "To install the plugin:"
echo "    opensearch-plugin install file://\$(pwd)/build/distributions/opensearch-iouring-*.zip"
echo ""
log_info "To run OpenSearch with IO-uring:"
echo "    ./scripts/run-opensearch.sh"
echo ""
