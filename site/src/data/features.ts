// Feature list. Every entry maps to a capability that ships in v1.0.2; `caveat` says where it stops.
// The two `lead` entries are what an evaluator replacing Swarm checks first, so they get the large
// treatment; the rest are a compact list (DESIGN.md §5.8, antislop R-14).
export interface Feature {
  title: string;
  body: string;
  caveat?: string;
  href: string;
  /** Names the destination, so the link text says where it goes. */
  linkLabel: string;
  /** Where the claim comes from, shown as the link title. */
  source: string;
  lead?: boolean;
  /** Lead entries only. `api`: angle brackets, an API surface. `loud`: a warning sign, a refusal. */
  icon?: 'api' | 'loud';
}

export const features: Feature[] = [
  {
    lead: true,
    icon: 'api',
    title: 'Speaks the Swarm API',
    body: 'A Docker Engine API v1.41 subset: docker stack deploy, docker service ls and the Traefik v3 swarm provider work against minga api, so your tooling stays.',
    caveat: 'A subset, not the full API.',
    href: '/docs/examples/#docker-cli',
    linkLabel: 'See the docker CLI example',
    source: 'README.md',
  },
  {
    lead: true,
    icon: 'loud',
    title: 'Fails loudly by default',
    body: 'Unknown Compose keys are rejected with suggestions, an unplaceable deploy gets HTTP 409 instead of a warning, and /readyz says why it is not ready.',
    caveat: 'Healthchecks gate task readiness; named image users must be numeric uid[:gid].',
    href: '/architecture/',
    linkLabel: 'Read the architecture notes',
    source: 'README.md, CHANGELOG.md',
  },
  {
    title: 'One binary, checksum-verified',
    body: 'Release archives bundle minga with a checksum-pinned nats-server and ctr plus licenses, install.sh verifies SHA256SUMS and can install the systemd unit, and the same binaries ship as a multi-arch ghcr.io/lithqube/minga image (Talos runs it as a ContainerConfig).',
    caveat: 'Private-registry pulls read inline Docker credentials only (no credential helpers), and ctr receives them on its command line.',
    href: '/docs/quickstart/',
    linkLabel: 'Quickstart',
    source: 'CHANGELOG.md, install.sh',
  },
  {
    title: 'Built for edge and air-gap',
    body: 'The air-gapped path has no network step: copy the release archive and image tarballs, import with ctr, start the roles.',
    caveat: 'Container networking is single-node for now.',
    href: '/docs/quickstart/#deployment-topologies',
    linkLabel: 'Deployment topologies',
    source: 'README.md',
  },
  {
    title: 'Metrics, health, logs',
    body: '/metrics, /healthz and /readyz on every role, JSON logs, and minga logs -f with separated stdout and stderr.',
    caveat: 'No distributed tracing yet.',
    href: '/adr/0004-observability/',
    linkLabel: 'ADR 0004: Observability',
    source: 'docs/adr/0004-observability.md',
  },
  {
    title: 'Transport security you can audit',
    body: 'Raft mTLS, an API bearer token, NATS TLS, a built-in CA (minga tls) and content-addressed secret staging.',
    caveat: 'FIPS, SOC 2 and similar are not assessed.',
    href: '/changelog/',
    linkLabel: 'Changelog',
    source: 'CHANGELOG.md 0.1.0',
  },
  {
    title: 'Validate before you deploy',
    body: 'minga validate checks a Compose file offline and prints the startup order, replicas and published ports.',
    href: '/docs/examples/#validate',
    linkLabel: 'Validate example',
    source: 'README.md',
  },
  {
    title: 'Rust on the hot path',
    body: 'Per-packet, per-request and per-sandbox work stays in Rust. Go runs only as a separate process, never through FFI.',
    caveat: 'No published benchmarks yet.',
    href: '/adr/0001-language-split/',
    linkLabel: 'ADR 0001: Language split',
    source: 'docs/adr/0001-language-split.md',
  },
];
