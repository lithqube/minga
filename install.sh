#!/bin/sh
# Install the Minga release binary.
#
# Usage:
#   curl -fsSL https://github.com/lithqube/minga/releases/latest/download/install.sh | sh
#   curl -fsSL .../install.sh | sh -s -- --with-systemd
#   ./install.sh --version v1.2.0 --prefix /opt/minga/bin
#   ./install.sh --uninstall
#
# Options (environment equivalents work too):
#   --version TAG     release tag to install      [MINGA_VERSION, default: latest]
#   --prefix DIR      install directory           [MINGA_PREFIX,  default: /usr/local/bin]
#   --with-systemd    also install the systemd unit and a /etc/minga/minga.env
#                     template from the archive (does not enable or start)
#   --uninstall       remove binaries, licenses and the systemd unit
#   -h, --help        show this help
#
#   MINGA_BASE_URL    mirror root for air-gapped installs; must mirror the
#                     GitHub releases layout (<base>/latest/download/... and
#                     <base>/download/<tag>/...), including SHA256SUMS.
set -eu

REPO="lithqube/minga"
PREFIX="${MINGA_PREFIX:-/usr/local/bin}"
VERSION="${MINGA_VERSION:-latest}"
BASE_URL="${MINGA_BASE_URL:-https://github.com/$REPO/releases}"
WITH_SYSTEMD=0
UNINSTALL=0

usage() {
    cat <<'EOF'
Usage:
  curl -fsSL https://github.com/lithqube/minga/releases/latest/download/install.sh | sh
  curl -fsSL .../install.sh | sh -s -- --with-systemd
  ./install.sh --version v1.2.0 --prefix /opt/minga/bin
  ./install.sh --uninstall

Options (environment equivalents work too):
  --version TAG     release tag to install      [MINGA_VERSION, default: latest]
  --prefix DIR      install directory           [MINGA_PREFIX,  default: /usr/local/bin]
  --with-systemd    also install the systemd unit and a /etc/minga/minga.env
                    template from the archive (does not enable or start)
  --uninstall       remove binaries, licenses and the systemd unit
  -h, --help        show this help

  MINGA_BASE_URL    mirror root for air-gapped installs; must mirror the
                    GitHub releases layout (<base>/latest/download/... and
                    <base>/download/<tag>/...), including SHA256SUMS.
EOF
}

fail() {
    echo "error: $*" >&2
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --version)
            [ $# -ge 2 ] || fail "--version needs a tag"
            VERSION="$2"
            shift 2
            ;;
        --prefix)
            [ $# -ge 2 ] || fail "--prefix needs a directory"
            PREFIX="$2"
            shift 2
            ;;
        --with-systemd | --systemd)
            WITH_SYSTEMD=1
            shift
            ;;
        --no-systemd)
            WITH_SYSTEMD=0
            shift
            ;;
        --uninstall)
            UNINSTALL=1
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument: $1 (try --help)"
            ;;
    esac
done

os=$(uname -s | tr '[:upper:]' '[:lower:]')
[ "$os" = "linux" ] || fail "minga supports linux only (got: $os)"

# sudo_bin touches $PREFIX (may be a user-writable dir, then empty);
# sudo_sys touches /etc and systemctl (root or sudo, only used when needed).
sudo_sys=""
if [ "$(id -u)" != "0" ] && command -v sudo >/dev/null 2>&1; then
    sudo_sys="sudo"
fi

if [ "$UNINSTALL" = "1" ]; then
    sudo_bin="$sudo_sys"
    if [ -d "$PREFIX" ] && [ -w "$PREFIX" ]; then
        sudo_bin=""
    fi
    if command -v systemctl >/dev/null 2>&1 && [ -f /etc/systemd/system/minga.service ]; then
        [ "$(id -u)" = "0" ] || [ -n "$sudo_sys" ] || fail "removing the systemd unit needs root or sudo"
        $sudo_sys systemctl disable --now minga 2>/dev/null || true
        $sudo_sys rm -f /etc/systemd/system/minga.service
        $sudo_sys systemctl daemon-reload 2>/dev/null || true
        echo "removed systemd unit"
    fi
    $sudo_bin rm -f "$PREFIX/minga" "$PREFIX/nats-server"
    # Only remove a ctr this script installed (marker written at install).
    if [ -f "${PREFIX%/bin}/share/minga/.ctr-installed" ]; then
        $sudo_bin rm -f "$PREFIX/ctr"
        echo "removed $PREFIX/ctr"
    fi
    $sudo_bin rm -rf "${PREFIX%/bin}/share/minga"
    echo "removed $PREFIX/minga, $PREFIX/nats-server and licenses"
    echo "kept /etc/minga and /var/lib/minga; remove them by hand if unneeded"
    exit 0
fi

arch=$(uname -m)
case "$arch" in
    x86_64 | amd64) target="x86_64-unknown-linux-gnu" ;;
    aarch64 | arm64) target="aarch64-unknown-linux-gnu" ;;
    *) fail "unsupported architecture: $arch" ;;
esac

# Accept "1.2.0" for "v1.2.0".
if [ "$VERSION" != "latest" ] && [ "${VERSION#v}" = "$VERSION" ]; then
    VERSION="v$VERSION"
fi

if [ "$VERSION" = "latest" ]; then
    base="$BASE_URL/latest/download"
else
    base="$BASE_URL/download/$VERSION"
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
sudo_bin="$sudo_sys"
if mkdir -p "$PREFIX" 2>/dev/null && [ -w "$PREFIX" ]; then
    sudo_bin=""
fi
$sudo_bin mkdir -p "$PREFIX"
$sudo_bin install -m 0755 "$tmp/minga" "$PREFIX/minga"
$sudo_bin install -m 0755 "$tmp/nats-server" "$PREFIX/nats-server"

share="${PREFIX%/bin}/share/minga"
$sudo_bin mkdir -p "$share"

# ctr preloads workload images into the `minga` containerd namespace.
# Installed only when the host has none: never shadow a distro ctr.
ctr_note="ctr already on PATH; bundled one not installed"
if [ -f "$tmp/ctr" ] && ! command -v ctr >/dev/null 2>&1; then
    $sudo_bin install -m 0755 "$tmp/ctr" "$PREFIX/ctr"
    ctr_note="installed: $PREFIX/ctr"
    # Marker so --uninstall only removes a ctr this script installed.
    $sudo_bin touch "$share/.ctr-installed"
fi

# Licenses: Minga (MIT) and the bundled nats-server (Apache 2.0).
$sudo_bin install -m 0644 "$tmp/LICENSE" "$tmp/NATS-LICENSE" "$share/"

echo "installed: $PREFIX/minga, $PREFIX/nats-server (licenses in $share)"
echo "$ctr_note"

if [ "$WITH_SYSTEMD" = "1" ]; then
    [ -d /run/systemd/system ] || fail "--with-systemd needs a systemd host; install binaries only and write your own supervisor unit"
    [ -f "$tmp/minga.service" ] || fail "this archive predates the systemd files; use a newer release"
    [ "$(id -u)" = "0" ] || [ -n "$sudo_sys" ] || fail "--with-systemd needs root or sudo"
    $sudo_sys install -m 0644 "$tmp/minga.service" /etc/systemd/system/minga.service
    # minga.env holds tokens (MINGA_NATS_TOKEN, API tokens): root-only.
    $sudo_sys install -d -m 0700 /etc/minga
    # Checked as root: once /etc/minga is 0700, the calling user cannot see
    # into it, and a false negative would overwrite the configured file.
    if $sudo_sys test -f /etc/minga/minga.env; then
        $sudo_sys chmod 0600 /etc/minga/minga.env
        echo "kept existing /etc/minga/minga.env, now mode 0600 (template: $share/minga.env.example)"
        $sudo_sys install -m 0644 "$tmp/minga.env.example" "$share/minga.env.example"
    else
        $sudo_sys install -m 0600 "$tmp/minga.env.example" /etc/minga/minga.env
        echo "wrote /etc/minga/minga.env template (mode 0600)"
    fi
    $sudo_sys systemctl daemon-reload
    echo "next: edit /etc/minga/minga.env, then: sudo systemctl enable --now minga"
fi

"$PREFIX/minga" --version
