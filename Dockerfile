# syntax=docker/dockerfile:1
# Runtime image for `minga` (manager or worker) and its NATS sidecar.
#
# Consumes the prebuilt release binaries assembled by
# .github/workflows/release.yml into dist/linux-<arch>/, so multi-arch
# builds never compile under QEMU. Base is ubuntu:24.04 to match the
# glibc of the CI runners that built the binaries.
#
# Local build (from a release archive, e.g. aarch64):
#   mkdir -p dist/linux-arm64 && tar -xzf minga-aarch64-unknown-linux-gnu.tar.gz -C dist/linux-arm64
#   cp LICENSE dist/
#   docker buildx build --platform linux/arm64 -t minga:local dist/ --load
FROM ubuntu:24.04
ARG TARGETARCH

# rustls-native-certs reads the system store for outbound TLS.
# The supervisor shells out to ip, iptables and sysctl for netns,
# bridges, veth pairs and the published-port DNAT.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates iproute2 iptables procps \
    && rm -rf /var/lib/apt/lists/*

# nats-server sits next to minga: the manager's sidecar lookup checks
# the minga binary's own directory first. ctr preloads workload images
# into the `minga` containerd namespace (there is no registry pull yet).
COPY linux-${TARGETARCH}/minga /usr/local/bin/minga
COPY linux-${TARGETARCH}/nats-server /usr/local/bin/nats-server
COPY linux-${TARGETARCH}/ctr /usr/local/bin/ctr
COPY linux-${TARGETARCH}/NATS-LICENSE /usr/local/share/minga/NATS-LICENSE
COPY LICENSE /usr/local/share/minga/LICENSE

# Node roles need host resources: run with --network host, NET_ADMIN and
# SYS_ADMIN (or --privileged), and mount the containerd socket.
# See README "Container image".
ENTRYPOINT ["/usr/local/bin/minga"]
CMD ["--help"]
