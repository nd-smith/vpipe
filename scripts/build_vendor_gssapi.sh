#!/usr/bin/env bash
# Build gssapi wheel and extract Kerberos runtime libraries for vendoring.
#
# Uses a Docker container matching the production base image (python:3.11-slim)
# to compile the gssapi wheel and copy the required shared libraries.
#
# Output:
#   vendor/
#     gssapi-*.whl           - Pre-built wheel
#     lib/
#       libgssapi_krb5.so*   - Kerberos runtime libraries
#       libkrb5.so*
#       libk5crypto.so*
#       libkrb5support.so*
#       libcom_err.so*
#
# Usage:
#   ./scripts/build_vendor_gssapi.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENDOR_DIR="$PROJECT_ROOT/vendor"
VENDOR_LIB_DIR="$VENDOR_DIR/lib"
BASE_IMAGE="python:3.11-slim"

echo "Building gssapi vendor artifacts using $BASE_IMAGE..."
echo "Output: $VENDOR_DIR"

mkdir -p "$VENDOR_DIR" "$VENDOR_LIB_DIR"

docker run --rm \
    -v "$VENDOR_DIR:/output" \
    "$BASE_IMAGE" \
    bash -c '
        set -euo pipefail

        # Install build dependencies
        apt-get update -qq
        apt-get install -y -qq --no-install-recommends libkrb5-dev gcc >/dev/null 2>&1

        # Build the wheel
        pip wheel "gssapi>=1.8.0,<2.0.0" --no-deps -w /output/ 2>&1 | tail -1

        # Copy Kerberos runtime shared libraries
        mkdir -p /output/lib
        for lib in libgssapi_krb5 libkrb5 libk5crypto libkrb5support libcom_err; do
            # Find the actual .so file (follow symlinks) and copy with its soname
            src=$(find /usr/lib/ -name "${lib}.so*" -type f -o -name "${lib}.so*" -type l 2>/dev/null | head -1)
            if [ -n "$src" ]; then
                # Copy the real file and all symlinks
                cp -a /usr/lib/*/$(basename "$src")* /output/lib/ 2>/dev/null || \
                cp -aL "$src" "/output/lib/$(basename "$src")" 2>/dev/null || true
            fi
        done

        # Also grab any symlinks we need
        for lib in libgssapi_krb5 libkrb5 libk5crypto libkrb5support libcom_err; do
            for f in /usr/lib/*/${lib}.so*; do
                [ -e "$f" ] && cp -a "$f" /output/lib/ 2>/dev/null || true
            done
        done

        echo "Wheel:"
        ls /output/*.whl
        echo ""
        echo "Libraries:"
        ls -la /output/lib/
    '

echo ""
echo "Vendor artifacts written to: $VENDOR_DIR"
echo "Done."
