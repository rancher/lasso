#!/bin/sh
set -e

# Updates the pinned envtest version and checksums in scripts/install-envtest.sh.
#
# Usage:
#   ./scripts/bump-envtest.sh            # bump to the newest release matching the k8s.io/client-go minor
#   ./scripts/bump-envtest.sh v1.37.0    # bump to a specific release
#
# The checksums are read from the upstream release manifest, so they never have
# to be copied by hand.

RELEASES_URL=https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml
TARGET_SCRIPT=scripts/install-envtest.sh
PLATFORMS="linux_amd64 linux_arm64 darwin_amd64 darwin_arm64"

if [ ! -f "$TARGET_SCRIPT" ]; then
    echo "$TARGET_SCRIPT not found - run this from the repository root" >&2
    exit 1
fi

RELEASES=$(curl -sSfL "$RELEASES_URL")

if [ -z "$RELEASES" ]; then
    echo "Failed to fetch $RELEASES_URL" >&2
    exit 1
fi

CLIENT_GO_MINOR=$(go mod graph | grep ' k8s.io/client-go@' | head -n1 | cut -d@ -f2 | cut -d '.' -f 2)

if [ -n "${1:-}" ]; then
    ENVTEST_VERSION=$1
else
    # Newest release sharing a minor version with k8s.io/client-go.
    ENVTEST_VERSION=$(echo "$RELEASES" \
        | sed -n "s/^  \(v1\.${CLIENT_GO_MINOR}\.[0-9]*\):$/\1/p" \
        | sort -V \
        | tail -n1)

    if [ -z "$ENVTEST_VERSION" ]; then
        echo "No envtest release found for k8s.io/client-go minor version ${CLIENT_GO_MINOR}" >&2
        exit 1
    fi
fi

ENVTEST_MINOR=$(echo "$ENVTEST_VERSION" | cut -d '.' -f 2)

if [ "$CLIENT_GO_MINOR" != "$ENVTEST_MINOR" ]; then
    echo "k8s.io/client-go minor version ($CLIENT_GO_MINOR) does not match requested envtest minor version ($ENVTEST_MINOR)" >&2
    echo "Bump k8s.io/client-go first, or pass a matching envtest version." >&2
    exit 1
fi

CURRENT_VERSION=$(sed -n 's/^ENVTEST_VERSION=//p' "$TARGET_SCRIPT")

if [ "$CURRENT_VERSION" = "$ENVTEST_VERSION" ]; then
    echo "Already pinned to ${ENVTEST_VERSION}"
    exit 0
fi

TMP_SCRIPT=$(mktemp)
trap 'rm -f "$TMP_SCRIPT"' EXIT

cp "$TARGET_SCRIPT" "$TMP_SCRIPT"

sed -i.bak "s/^ENVTEST_VERSION=.*/ENVTEST_VERSION=${ENVTEST_VERSION}/" "$TMP_SCRIPT"
rm -f "${TMP_SCRIPT}.bak"

for PLATFORM in $PLATFORMS; do
    OS=${PLATFORM%_*}
    ARCH=${PLATFORM#*_}
    TARBALL="envtest-${ENVTEST_VERSION}-${OS}-${ARCH}.tar.gz"

    SUM=$(echo "$RELEASES" \
        | grep -A1 "^    ${TARBALL}:\$" \
        | sed -n 's/^ *hash: //p')

    if [ -z "$SUM" ]; then
        echo "No checksum for ${TARBALL} in the release manifest" >&2
        exit 1
    fi

    sed -i.bak "s/^ENVTEST_SUM_${PLATFORM}=.*/ENVTEST_SUM_${PLATFORM}=${SUM}/" "$TMP_SCRIPT"
    rm -f "${TMP_SCRIPT}.bak"
done

cp "$TMP_SCRIPT" "$TARGET_SCRIPT"

echo "Bumped envtest ${CURRENT_VERSION} -> ${ENVTEST_VERSION} in ${TARGET_SCRIPT}"
echo "Run 'make test-integration' to download and verify the new binaries."
