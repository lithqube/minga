#!/bin/sh
# Install the Minga release binary.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/lithqube/minga/main/install.sh | sh
#
# Environment:
#   MINGA_VERSION  release tag to install (default: latest)
#   MINGA_PREFIX   install directory (default: /usr/local/bin)
set -eu

REPO="lithqube/minga"
PREFIX="${MINGA_PREFIX:-/usr/local/bin}"
VERSION="${MINGA_VERSION:-latest}"

os=$(uname -s | tr '[:upper:]' '[:lower:]')
if [ "$os" != "linux" ]; then
    echo "error: minga supports linux only (got: $os)" >&2
    exit 1
fi

arch=$(uname -m)
case "$arch" in
    x86_64 | amd64) target="x86_64-unknown-linux-gnu" ;;
    aarch64 | arm64) target="aarch64-unknown-linux-gnu" ;;
    *)
        echo "error: unsupported architecture: $arch" >&2
        exit 1
        ;;
esac

if [ "$VERSION" = "latest" ]; then
    url="https://github.com/$REPO/releases/latest/download/minga-$target.tar.gz"
else
    url="https://github.com/$REPO/releases/download/$VERSION/minga-$target.tar.gz"
fi

echo "downloading minga ($target) from $url"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL "$url" | tar -xz -C "$tmp"

if [ -w "$PREFIX" ]; then
    install -m 0755 "$tmp/minga" "$PREFIX/minga"
else
    sudo install -m 0755 "$tmp/minga" "$PREFIX/minga"
fi

echo "installed: $PREFIX/minga"
"$PREFIX/minga" --version
