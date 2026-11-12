// Neutral comparison. Every cell needs a source: a repo path for Minga, upstream docs for the others.
// Non-Minga cells were written from public documentation; corrections are welcome via the issue link.
export type Mark = 'yes' | 'partial' | 'no' | 'na';
export interface Cell {
  mark: Mark;
  text: string;
  source: string;
}
export interface Row {
  capability: string;
  cells: [Cell, Cell, Cell];
}

export const asOf = '2026-11';
export const columns = ['Minga v1.1.0', 'Docker Swarm mode', 'k3s'] as const;

const swarmDocs = 'https://docs.docker.com/engine/swarm/';
const k3sDocs = 'https://docs.k3s.io/';

export const rows: Row[] = [
  {
    capability: 'Packaging',
    cells: [
      { mark: 'yes', text: 'One minga binary plus the nats-server it supervises', source: 'README.md' },
      { mark: 'yes', text: 'Built into Docker Engine (dockerd + containerd)', source: swarmDocs },
      { mark: 'yes', text: 'Single binary bundling Kubernetes components, containerd and CNI', source: k3sDocs },
    ],
  },
  {
    capability: 'Workload format',
    cells: [
      { mark: 'yes', text: 'Compose; unknown keys fail loudly', source: 'README.md' },
      {
        mark: 'yes',
        text: 'Compose via docker stack deploy; unsupported keys are skipped with a warning',
        source: 'https://docs.docker.com/engine/swarm/stack-deploy/',
      },
      { mark: 'partial', text: 'Kubernetes manifests and Helm; Compose needs conversion', source: k3sDocs },
    ],
  },
  {
    capability: 'API',
    cells: [
      { mark: 'partial', text: 'Docker Engine API v1.41 subset', source: 'README.md' },
      { mark: 'yes', text: 'Full Docker Engine API', source: 'https://docs.docker.com/reference/api/engine/' },
      { mark: 'yes', text: 'Kubernetes API', source: k3sDocs },
    ],
  },
  {
    capability: 'Desired-state store',
    cells: [
      {
        mark: 'yes',
        text: 'Embedded Raft (openraft + redb); every process is a voter today',
        source: 'docs/architecture.md',
      },
      { mark: 'yes', text: 'Embedded Raft among managers', source: 'https://docs.docker.com/engine/swarm/raft/' },
      {
        mark: 'yes',
        text: 'SQLite on a single server, embedded etcd or an external datastore',
        source: 'https://docs.k3s.io/datastore',
      },
    ],
  },
  {
    capability: 'Image pull from a registry',
    cells: [
      {
        mark: 'yes',
        text: 'Through containerd’s Transfer API, with Docker credential helpers; credentialed pulls need containerd 1.7+, and no plain-HTTP registries on 1.7',
        source: 'CHANGELOG.md',
      },
      { mark: 'yes', text: 'Yes', source: swarmDocs },
      { mark: 'yes', text: 'Yes', source: k3sDocs },
    ],
  },
  {
    capability: 'Container networking across nodes',
    cells: [
      { mark: 'no', text: 'Single node only; WireGuard mesh planned', source: 'README.md' },
      {
        mark: 'yes',
        text: 'VXLAN overlay networks',
        source: 'https://docs.docker.com/engine/network/drivers/overlay/',
      },
      { mark: 'yes', text: 'Flannel by default', source: 'https://docs.k3s.io/networking/basic-network-options' },
    ],
  },
  {
    capability: 'Add nodes to a running cluster',
    cells: [
      { mark: 'no', text: 'No; membership is fixed at bootstrap', source: 'README.md' },
      { mark: 'yes', text: 'Join tokens', source: 'https://docs.docker.com/engine/swarm/join-nodes/' },
      { mark: 'yes', text: 'Node token', source: 'https://docs.k3s.io/quick-start' },
    ],
  },
  {
    capability: 'Healthchecks, rolling update, rollback',
    cells: [
      { mark: 'yes', text: 'Healthchecks enforced; rolling updates and rollback supported', source: 'CHANGELOG.md' },
      { mark: 'yes', text: 'Yes', source: 'https://docs.docker.com/engine/swarm/swarm-tutorial/rolling-update/' },
      { mark: 'yes', text: 'Probes, rollouts and rollback', source: k3sDocs },
    ],
  },
  {
    capability: 'Backup and restore of cluster state',
    cells: [
      {
        mark: 'yes',
        text: 'minga state export and import of desired state over the API; secret values and volume data copied separately',
        source: 'docs/backup.md',
      },
      {
        mark: 'partial',
        text: 'Copy the swarm state directory from a manager with Docker stopped',
        source: 'https://docs.docker.com/engine/swarm/admin_guide/',
      },
      { mark: 'yes', text: 'etcd snapshots with embedded etcd', source: 'https://docs.k3s.io/cli/etcd-snapshot' },
    ],
  },
  {
    capability: 'Transport security',
    cells: [
      { mark: 'partial', text: 'Raft mTLS (opt-in flags), API bearer token, NATS TLS', source: 'CHANGELOG.md' },
      {
        mark: 'yes',
        text: 'Mutual TLS between nodes by default; overlay encryption opt-in',
        source: 'https://docs.docker.com/engine/swarm/how-swarm-mode-works/pki/',
      },
      { mark: 'yes', text: 'TLS between components by default', source: k3sDocs },
    ],
  },
  {
    capability: 'Air-gapped install',
    cells: [
      {
        mark: 'partial',
        text: 'Published release archives with checksum-pinned nats-server and SHA256SUMS; images can be preloaded as tarballs',
        source: 'README.md',
      },
      { mark: 'partial', text: 'Offline packages plus docker load', source: swarmDocs },
      {
        mark: 'yes',
        text: 'Documented air-gap install with image tarballs',
        source: 'https://docs.k3s.io/installation/airgap',
      },
    ],
  },
  {
    capability: 'WASM and agent workloads',
    cells: [
      {
        mark: 'partial',
        text: 'x-minga-agent under wasmtime; no host I/O yet',
        source: 'docs/adr/0003-agent-workloads.md',
      },
      { mark: 'no', text: 'Not built in', source: swarmDocs },
      { mark: 'no', text: 'Not built in', source: k3sDocs },
    ],
  },
  {
    capability: 'Maturity',
    cells: [
      { mark: 'partial', text: 'v1.1.0; first release 2026-11', source: 'CHANGELOG.md' },
      { mark: 'yes', text: 'In Docker Engine since 2016', source: swarmDocs },
      { mark: 'yes', text: 'CNCF project, certified Kubernetes distribution', source: k3sDocs },
    ],
  },
  {
    capability: 'License',
    cells: [
      { mark: 'na', text: 'MIT (nats-server: Apache 2.0)', source: 'LICENSE' },
      { mark: 'na', text: 'Apache 2.0 (Moby)', source: 'https://github.com/moby/moby' },
      { mark: 'na', text: 'Apache 2.0', source: 'https://github.com/k3s-io/k3s' },
    ],
  },
];
