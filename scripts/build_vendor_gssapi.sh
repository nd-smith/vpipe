#!/usr/bin/env bash
# Build the gssapi wheel for vendoring into the Cloud Foundry deployment.
#
# The CF stack (cflinuxfs4) already ships libkrb5 runtime libraries,
# but gssapi has no pre-built wheel on PyPI -- it must be compiled
# against libkrb5-dev headers. This script uses a Docker container
# to compile the wheel on a matching platform.
#
# Output:
#   vendor/gssapi-*.whl   (cp312, linux x86_64)
#
# Usage:
#   ./scripts/build_vendor_gssapi.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENDOR_DIR="$PROJECT_ROOT/vendor"

# Match the CF runtime version from Jenkinsfile
BASE_IMAGE="python:3.12-slim"

echo "Building gssapi wheel using $BASE_IMAGE..."
echo "Output: $VENDOR_DIR"

mkdir -p "$VENDOR_DIR"

# Remove any previously built gssapi wheel
rm -f "$VENDOR_DIR"/gssapi-*.whl

docker run --rm \
    -v "$VENDOR_DIR:/output" \
    "$BASE_IMAGE" \
    bash -c '
        set -euo pipefail

        # Install build dependencies (headers + compiler)
        apt-get update -qq
        apt-get install -y -qq --no-install-recommends libkrb5-dev gcc >/dev/null 2>&1

        # Build the wheel
        pip wheel "gssapi>=1.8.0,<2.0.0" --no-deps -w /output/

        echo ""
        echo "Built:"
        ls -lh /output/gssapi-*.whl
    '

echo ""
echo "Wheel written to: $VENDOR_DIR"
ls "$VENDOR_DIR"/gssapi-*.whl
echo "Done."
