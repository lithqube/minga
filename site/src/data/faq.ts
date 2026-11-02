// FAQ answers are trusted HTML authored here. Each answer cites the repo file it comes from.
export interface Faq {
  q: string;
  a: string;
  sources: string[];
}

export const faq: Faq[] = [
  {
    q: 'Is Minga production-ready?',
    a: 'It is v0.1.0. The single-host path is verified by execution on Linux (containerd 2.2.1) and macOS, and every claim in the changelog sits next to its known limits. If you need image pull, multi-node container networking or rollback, it is not ready for you yet.',
    sources: ['README.md', 'CHANGELOG.md'],
  },
  {
    q: 'Do my Compose files work unchanged?',
    a: 'That is the goal, and keys Minga does not understand fail loudly with a suggestion instead of being skipped. <code>command</code>, <code>entrypoint</code>, <code>labels</code>, volumes, bind mounts, placement constraints and <code>deploy.restart_policy</code> reach the container; <code>healthcheck</code> is carried but not enforced. Secrets must be <code>external</code> references. Run <code>minga validate -c compose.yaml</code> offline first.',
    sources: ['README.md', 'docs/roadmap-v0.1.0.md'],
  },
  {
    q: 'Can I keep using the docker CLI and Traefik?',
    a: 'Yes, against <code>minga api</code>: <code>docker stack deploy</code>, <code>docker service ls</code>, <code>docker node ls</code> and the Traefik v3 swarm provider. With a token set, the CLI sends it through <code>"HttpHeaders": {"Authorization": "Bearer &lt;token&gt;"}</code> in <code>~/.docker/config.json</code>.',
    sources: ['README.md', 'docs/talos-deployment.md'],
  },
  {
    q: 'Why does Minga not pull images?',
    a: 'The pull path (content transfer plus unpack) is not built yet; it is next on the roadmap. Until then, preload images with <code>ctr -n minga images pull</code>, or <code>ctr -n minga images import &lt;image&gt;.tar</code> on an air-gapped site.',
    sources: ['README.md'],
  },
  {
    q: 'Why does one host run two processes?',
    a: 'In v0.1.0, <code>minga api</code> is its own Raft voter, so the manager and the API peer with each other. In the target design the API folds into the manager.',
    sources: ['README.md', 'docs/architecture.md'],
  },
  {
    q: 'Can I run more than one node?',
    a: 'The control plane works across nodes: Raft with mTLS, plus scheduling. Three limits apply: every node must be listed with <code>--peer</code> and started together, containers on different nodes cannot reach each other yet, and every manager and worker must use one external TLS NATS server.',
    sources: ['README.md'],
  },
  {
    q: 'Does it need Docker, Kubernetes or etcd?',
    a: 'No. It needs containerd and root, because the supervisor creates network namespaces, bridges and iptables rules.',
    sources: ['README.md'],
  },
  {
    q: 'Why is there a Go binary in a Rust project?',
    a: '<code>nats-server</code> is written in Go. ADR 0001 allows Go only as a separate process with no FFI: the manager spawns it as a child, and the release pins its checksum.',
    sources: ['docs/adr/0001-language-split.md', 'CHANGELOG.md'],
  },
  {
    q: 'How do I install on an air-gapped site?',
    a: 'Copy the release archive (minga, nats-server and the licenses) and your image tarballs to the site. Run <code>ctr -n minga images import &lt;image&gt;.tar</code> on each node and start the roles. Once a release is tagged, <code>install.sh</code> verifies <code>SHA256SUMS</code>; until then, build the binary from source before you go offline.',
    sources: ['README.md'],
  },
  {
    q: 'I published a port. Why might it not answer on the same host?',
    a: 'Host-local port hairpinning is not in v0.1.0. Test a published port from another machine against the host IP.',
    sources: ['README.md'],
  },
  {
    q: 'Is it secure? Is it compliant?',
    a: 'Raft mTLS, an API bearer token, NATS TLS, a built-in CA (<code>minga tls</code>) and content-addressed secret staging are implemented. Keep <code>:2375</code> and the Raft ports on a private network: the API token is what stands between the network and container creation. FIPS, SOC 2, VS-NfD and NATO are not assessed.',
    sources: ['CHANGELOG.md', 'README.md'],
  },
  {
    q: 'What are agent workloads?',
    a: 'Compose services with <code>x-minga-agent</code> run as WASM modules under wasmtime with fuel and timeout limits, and their LLM calls go through an egress proxy with token budgets. Today the guest is pure compute: no WASI, network, env or clock, and memory limits are not enforced. Nodes need <code>--sandbox-runtime wasmtime</code>.',
    sources: ['docs/adr/0003-agent-workloads.md', 'README.md'],
  },
];
