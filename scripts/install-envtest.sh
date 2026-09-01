#!/bin/sh
set -e

# The envtest version and SHAs below are updated by scripts/bump-envtest.sh.
# They come from https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/HEAD/envtest-releases.yaml

ENVTEST_VERSION=v1.36.2
ENVTEST_SUM_linux_amd64=ea743186c8a799f5cf8faf16969f86189d003cb7d130e0ac4b58789f1e5748dcf30ebe91c837a10d5ac415383da3e10b9e64d65785c938c23e739781cfb76f08
ENVTEST_SUM_linux_arm64=2d72ee985a8e262a3c57dc9f7f0fd891f6a8c7bf7ebaa2db6dc6d8eac8ae28181afe51c1f368b67756cdb40b10de9b205609e1726e5f27f7c6d824dd9c6649ac
ENVTEST_SUM_darwin_amd64=b4fe9f973cd1992e3880f8b230ed32c0c243b83371591e3e3315f4404df2cdbef8ee94eeb9bb245b7e5feb987d5c4a76c43e0877654ab73f00d7bbc2b9984514
ENVTEST_SUM_darwin_arm64=9278f9e5af556b2f1f2d139769c1f0d717c7b4426917fdebdba898bcb725a916e4910d6160886194ff9be9589ea7c5c32c2b8ae0754703874b7c8ba8ddfc41ce

CLIENT_GO_MINOR=$(go mod graph | grep ' k8s.io/client-go@' | head -n1 | cut -d@ -f2 | cut -d '.' -f 2)
ENVTEST_MINOR=$(echo "$ENVTEST_VERSION" | cut -d '.' -f 2)

if [ "$CLIENT_GO_MINOR" != "$ENVTEST_MINOR" ]; then
    echo "k8s.io/client-go minor version ($CLIENT_GO_MINOR) does not match envtest minor version ($ENVTEST_MINOR)" >&2
    exit 1
fi

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

eval "ENVTEST_SUM=\$ENVTEST_SUM_${OS}_${ARCH}"

if [ -z "$ENVTEST_SUM" ]; then
    echo "Unsupported platform: ${OS}-${ARCH}" >&2
    exit 1
fi

TARBALL="envtest-${ENVTEST_VERSION}-${OS}-${ARCH}.tar.gz"
URL="https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-${ENVTEST_VERSION}/${TARBALL}"
DEST="/tmp/${TARBALL}"

SEMVER=${ENVTEST_VERSION#v}

if ! go tool -modfile gotools/setup-envtest/go.mod setup-envtest list -i | grep -q "v${SEMVER}"; then
    curl -sL -o "$DEST" "$URL"

    if command -v sha512sum >/dev/null 2>&1; then
        ACTUAL_SUM=$(sha512sum "$DEST" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
        ACTUAL_SUM=$(shasum -a 512 "$DEST" | awk '{print $1}')
    else
        echo "No SHA-512 checksum tool found (need sha512sum or shasum)" >&2
        exit 1
    fi

    if [ "$ACTUAL_SUM" != "$ENVTEST_SUM" ]; then
        echo "Checksum verification failed for ${DEST}" >&2
        exit 1
    fi

    cat "$DEST" | go tool -modfile gotools/setup-envtest/go.mod setup-envtest sideload "${SEMVER}" > /dev/null
fi
