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
#
# On Windows (PowerShell), run the docker command directly:
#   docker run --rm -v "${PWD}/vendor:/output" python:3.12-slim bash -c \
#     'apt-get update -qq && apt-get install -y -qq --no-install-recommends libkrb5-dev gcc >/dev/null 2>&1 && pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org Cython && pip wheel gssapi --no-deps --no-build-isolation --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org -w /output/ && ls -lh /output/gssapi-*.whl'

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENDOR_DIR="$PROJECT_ROOT/vendor"

# Match the CF runtime version from Jenkinsfile
BASE_IMAGE="python:3.12-slim"
TRUSTED_HOSTS="--trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org"

echo "Building gssapi wheel using $BASE_IMAGE..."
echo "Output: $VENDOR_DIR"

mkdir -p "$VENDOR_DIR"

# Remove any previously built gssapi wheel
rm -f "$VENDOR_DIR"/gssapi-*.whl

docker run --rm \
    -v "$VENDOR_DIR:/output" \
    "$BASE_IMAGE" \
    bash -c "
        set -euo pipefail

        # Install build dependencies (headers + compiler)
        apt-get update -qq
        apt-get install -y -qq --no-install-recommends libkrb5-dev gcc >/dev/null 2>&1

        # Install Cython (setuptools already in image)
        pip install $TRUSTED_HOSTS Cython

        # Build the wheel (--no-build-isolation uses pre-installed Cython/setuptools)
        pip wheel gssapi --no-deps --no-build-isolation $TRUSTED_HOSTS -w /output/

        echo ''
        echo 'Built:'
        ls -lh /output/gssapi-*.whl
    "

echo ""
echo "Wheel written to: $VENDOR_DIR"
ls "$VENDOR_DIR"/gssapi-*.whl
echo "Done."
