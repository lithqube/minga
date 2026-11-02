// Feature grid. Every card maps to a capability that ships in v0.1.0; `caveat` says where it stops.
export interface Feature {
  title: string;
  body: string;
  caveat?: string;
  href: string;
  /** Where the claim comes from, shown as the link title. */
  source: string;
  icon: 'install' | 'loud' | 'metrics' | 'lock' | 'rust' | 'api' | 'check' | 'edge';
}

export const features: Feature[] = [
  {
    icon: 'install',
    title: 'One binary, checksum-verified',
    body: 'Release archives bundle minga with a checksum-pinned nats-server and its license, and install.sh verifies SHA256SUMS.',
    caveat: 'No release is tagged yet: build from source for now.',
    href: '/docs/quickstart/',
    source: 'CHANGELOG.md, install.sh',
  },
  {
    icon: 'loud',
    title: 'Fails loudly by default',
    body: 'Unknown Compose keys are rejected with suggestions, unplaceable deploys get HTTP 409, and /readyz says why it is not ready.',
    caveat: 'Healthchecks are carried but not enforced yet.',
    href: '/architecture/',
    source: 'README.md, CHANGELOG.md',
  },
  {
    icon: 'metrics',
    title: 'Metrics, health, logs',
    body: '/metrics, /healthz and /readyz on every role, JSON logs, and minga logs -f with separated stdout and stderr.',
    caveat: 'No distributed tracing yet.',
    href: '/adr/0004-observability/',
    source: 'docs/adr/0004-observability.md',
  },
  {
    icon: 'lock',
    title: 'Transport security you can audit',
    body: 'Raft mTLS, an API bearer token, NATS TLS, a built-in CA (minga tls) and content-addressed secret staging.',
    caveat: 'FIPS, SOC 2 and similar are not assessed.',
    href: '/changelog/',
    source: 'CHANGELOG.md 0.1.0',
  },
  {
    icon: 'rust',
    title: 'Rust on the hot path',
    body: 'Per-packet, per-request and per-sandbox work stays in Rust. Go runs only as a separate process, never through FFI.',
    caveat: 'No published benchmarks yet.',
    href: '/adr/0001-language-split/',
    source: 'docs/adr/0001-language-split.md',
  },
  {
    icon: 'api',
    title: 'Speaks the Swarm API',
    body: 'A Docker Engine API v1.41 subset: docker stack deploy, docker service ls and the Traefik v3 swarm provider work against minga api.',
    caveat: 'A subset, not the full API.',
    href: '/architecture/',
    source: 'README.md',
  },
  {
    icon: 'check',
    title: 'Validate before you deploy',
    body: 'minga validate checks a Compose file offline and prints the startup order, replicas and published ports.',
    href: '/docs/examples/',
    source: 'README.md',
  },
  {
    icon: 'edge',
    title: 'Built for edge and air-gap',
    body: 'The air-gapped path has no network step: copy the release archive and image tarballs, import with ctr, start the roles.',
    caveat: 'Container networking is single-node for now.',
    href: '/docs/quickstart/#deployment-topologies',
    source: 'README.md, CHANGELOG.md',
  },
];
