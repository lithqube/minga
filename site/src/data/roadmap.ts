// Roadmap: features and security work only, per release. `backticks` render as code. Fixes and changes live in CHANGELOG.md.
// Shipped items come from CHANGELOG.md; next and proposed items from README.md §Roadmap,
// docs/architecture.md §Order of work, ADR 0005 and the 1.2.0 known limits. Edit those first.

export interface Milestone {
  version: string;
  /** Release date (YYYY-MM-DD); omitted for work not released yet. */
  date?: string;
  status: 'shipped' | 'next' | 'proposed';
  title: string;
  features: string[];
  security: string[];
  /** Repo file the items come from. */
  source: string;
}

export const milestones: Milestone[] = [
  {
    version: 'v0.1.0',
    date: '2026-11-03',
    status: 'shipped',
    title: 'Foundation: seven release gates',
    features: [
      'Nodes self-register with membership leases; a deploy with nowhere to go returns 409',
      '`minga deploy` writes through the same API path as the docker CLI',
      'Per-node container networking: bridges, netns, DNAT and embedded DNS',
      'Compose fidelity, including Traefik v3 routing to Minga services',
      'Real container logs with `minga logs -f`',
      'Release archives and `install.sh`',
    ],
    security: [
      'Raft mTLS, with a cluster CA and node certificates from `minga tls`',
      'Docker API bearer token',
      'NATS token on top of mTLS',
      'Secrets staged per task, content-addressed, never sent over Raft or the bus',
      'Agents hold no provider keys: the egress proxy injects them',
    ],
    source: 'CHANGELOG.md',
  },
  {
    version: 'v1.0.1',
    date: '2026-11-11',
    status: 'shipped',
    title: 'Updates that roll',
    features: [
      'Rolling updates that follow `update_config`',
      'Enforced healthchecks: a task is not running until it is healthy',
      'Automatic image pull for a missing image',
      '`node.role` placement constraints',
    ],
    security: ['`minga secret put` reads the value from stdin, never from arguments'],
    source: 'CHANGELOG.md',
  },
  {
    version: 'v1.0.2',
    date: '2026-11-12',
    status: 'shipped',
    title: 'Real stacks, day-2 tooling',
    features: [
      '`minga-ctl`: contexts, stacks, services, rollback, secrets and registry logins over SSH',
      'Compose `env_file`, `extra_hosts` and named users',
      'Private registry pulls',
      'Relative bind mounts',
    ],
    security: ['The API token works as a Basic-auth password, so Traefik authenticates instead of needing an open API'],
    source: 'CHANGELOG.md',
  },
  {
    version: 'v1.1.0',
    date: '2026-11-12',
    status: 'shipped',
    title: 'Single-node production',
    features: [
      'Versioned state that later upgrades keep',
      'Backup and restore with `minga state export` and `import`',
      'A failed task records its last error and exit code',
      'Docker credential helpers (`credHelpers`, `credsStore`)',
    ],
    security: [
      'Registry credentials go to containerd over its API, never on a command line',
      'Recorded task errors have credentials scrubbed',
      'State exports are written owner-only (0600)',
    ],
    source: 'CHANGELOG.md',
  },
  {
    version: 'v1.2.0',
    date: '2026-11-13',
    status: 'shipped',
    title: 'Security hardening from a source audit',
    features: ['At start, a node adopts the containers Raft still assigns to it and removes the Minga containers it no longer assigns'],
    security: [
      'Owner-only TLS keys, Raft data and NATS config',
      'The admin token never goes through a proxy or redirect',
      'Tasks cannot reach the node’s loopback services or other stacks’ networks',
      'Published ports cannot take a port the node uses',
      'Docker’s masked and read-only `/proc` paths and a deny-all device rule',
      'A bus command only starts what Raft assigns, from the Raft spec',
      'Timeouts and connection caps on the API, metrics, egress and Raft listeners',
      'A tokenless API refuses browser requests (DNS rebinding)',
      'Release workflow pinned to commit SHAs with least-privilege tokens',
    ],
    source: 'CHANGELOG.md',
  },
  {
    version: 'Next',
    status: 'next',
    title: 'Managers-only Raft',
    features: [
      'Workers become plain JetStream clients; only managers vote',
      'Nodes can join a running cluster',
      'Node liveness moves from Raft to JetStream leases',
      'Quorum no longer depends on unreliable edge workers',
    ],
    security: ['Workers no longer hold a copy of the full cluster state'],
    source: 'README.md',
  },
  {
    version: 'Then',
    status: 'next',
    title: 'WireGuard mesh',
    features: ['Containers on different nodes reach each other'],
    security: ['Node-to-node container traffic is encrypted'],
    source: 'README.md',
  },
  {
    version: 'Then',
    status: 'next',
    title: 'Host capabilities for agent sandboxes',
    features: ['WASI for agent workloads, with explicit grants', 'Enforced memory limits'],
    security: [
      'Agents reach the network only through the egress proxy',
      'Workload identity injected by the host, not held by the agent',
    ],
    source: 'docs/architecture.md',
  },
  {
    version: 'Proposed',
    status: 'proposed',
    title: 'Not scheduled yet',
    features: [],
    security: [
      'API roles, TLS on the API listener and a server-side audit log (ADR 0005)',
      'Raft TLS required off loopback, not just warned about',
      'A seccomp profile for containers',
    ],
    source: 'docs/adr/0005-api-identity-and-roles.md',
  },
];
