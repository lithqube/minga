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
    base="https://github.com/$REPO/releases/latest/download"
else
    base="https://github.com/$REPO/releases/download/$VERSION"
fi
archive="minga-$target.tar.gz"

echo "downloading minga ($target) from $base/$archive"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL -o "$tmp/$archive" "$base/$archive"
curl -fsSL -o "$tmp/SHA256SUMS" "$base/SHA256SUMS"
(cd "$tmp" && grep " $archive\$" SHA256SUMS | sha256sum -c -)
tar -xzf "$tmp/$archive" -C "$tmp"

# nats-server goes next to minga: the manager's NATS sidecar looks there.
if mkdir -p "$PREFIX" 2>/dev/null && [ -w "$PREFIX" ]; then
    sudo=""
else
    sudo="sudo"
    $sudo mkdir -p "$PREFIX"
fi
$sudo install -m 0755 "$tmp/minga" "$PREFIX/minga"
$sudo install -m 0755 "$tmp/nats-server" "$PREFIX/nats-server"

# Licenses: Minga (MIT) and the bundled nats-server (Apache 2.0).
share="${PREFIX%/bin}/share/minga"
$sudo mkdir -p "$share"
$sudo install -m 0644 "$tmp/LICENSE" "$tmp/NATS-LICENSE" "$share/"

echo "installed: $PREFIX/minga, $PREFIX/nats-server (licenses in $share)"
"$PREFIX/minga" --version
