#!/bin/sh
# Usage: check-release-archive.sh minga-<target>.tar.gz
# Fails when the release archive lacks a file operators and install.sh expect.
set -eu
archive="${1:?usage: check-release-archive.sh <archive.tar.gz>}"
listing=$(tar -tzf "$archive")
missing=0
for f in minga minga-ctl nats-server ctr NATS-LICENSE install.sh LICENSE minga.service minga.env.example; do
    if ! printf '%s\n' "$listing" | grep -qx "\(\./\)\{0,1\}$f"; then
        echo "error: $archive does not contain $f" >&2
        missing=1
    fi
done
[ "$missing" = 0 ] && echo "ok: $archive has every expected file"
exit "$missing"
