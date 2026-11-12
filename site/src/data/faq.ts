// FAQ answers are trusted HTML authored here. Each answer cites the repo file it comes from.
export interface Faq {
  q: string;
  a: string;
  sources: string[];
}

export const faq: Faq[] = [
  {
    q: 'Is Minga production-ready?',
    a: 'It is v1.1.0, aimed at single-host production. It is tested on macOS and in Linux CI, including the containerd suites and a real-kernel check of the published-port paths; this build has not yet been run on a real staging node. v1.1.0 adds versioned state that later upgrades keep, backup and restore, a recorded reason for failed tasks (last error and exit code for containers, the start error for agent tasks), image pulls through containerd’s own API with Docker credential helpers, and published ports that only redirect traffic addressed to the node. Cross-node container networking, joining a running cluster and per-operator API roles are not there yet.',
    sources: ['README.md', 'CHANGELOG.md', 'docs/release-1.1.0.md'],
  },
  {
    q: 'Do my Compose files work unchanged?',
    a: 'That is the goal, and keys Minga does not understand fail loudly with a suggestion instead of being skipped. <code>command</code>, <code>entrypoint</code>, <code>labels</code>, volumes, bind mounts, placement constraints, <code>deploy.restart_policy</code>, healthchecks and <code>deploy.update_config</code> are supported. Secrets must be <code>external</code> references. Run <code>minga validate -c compose.yaml</code> offline first.',
    sources: ['README.md', 'CHANGELOG.md'],
  },
  {
    q: 'Can I keep using the docker CLI and Traefik?',
    a: 'Yes, against <code>minga api</code>: <code>docker stack deploy</code>, <code>docker service ls</code>, <code>docker node ls</code> and the Traefik v3 swarm provider. With a token set, the CLI sends it through <code>"HttpHeaders": {"Authorization": "Bearer &lt;token&gt;"}</code> in <code>~/.docker/config.json</code>.',
    sources: ['README.md', 'docs/talos-deployment.md'],
  },
  {
    q: 'How does Minga pull images?',
    a: 'When an image is missing from the <code>minga</code> containerd namespace, containerd pulls it through its Transfer API on the configured socket, so the daemon’s network, proxy and CA trust apply. Credentials come from <code>$DOCKER_CONFIG/config.json</code>, else <code>~/.docker/config.json</code>: <code>credHelpers</code>, then <code>credsStore</code>, then inline <code>auths</code>. They never appear on a command line. Pulls that need credentials need containerd 1.7 or newer, and a plain-HTTP registry cannot be reached on 1.7. On air-gapped sites, preload with <code>ctr -n minga images import &lt;image&gt;.tar</code>.',
    sources: ['CHANGELOG.md', 'docs/release-1.1.0.md'],
  },
  {
    q: 'How do I upgrade from v1.0.2?',
    a: 'Once, with a wipe: v1.1.0 stores state in a new versioned format and refuses a 1.0.2 data directory with a message that says so. On every node, stop the roles, remove only <code>raft_log.redb</code> and <code>state.redb</code> from each role’s <code>--data-dir</code>, install v1.1.0 everywhere and redeploy. Do not delete the whole directory: with default paths it also holds secret values, named volumes and agent modules. Later upgrades that only add fields keep state.',
    sources: ['docs/release-1.1.0.md', 'CHANGELOG.md'],
  },
  {
    q: 'How do I back up a cluster?',
    a: '<code>minga state export -o backup.json</code> writes services, networks, volumes and secret and config references to a 0600 file; <code>minga state import backup.json</code> restores them into a fresh cluster, where the scheduler places the tasks again. Secret values, named volume data and TLS material are copied separately. The export includes environment values, so treat the file as a secret.',
    sources: ['docs/backup.md'],
  },
  {
    q: 'Why is my service at 0/1?',
    a: 'Run <code>minga-ctl service inspect &lt;stack&gt;.&lt;service&gt;</code>. A failed container task keeps its last error and exit code, for example <code>exited with code 1</code>, <code>killed by signal 9</code> or the start error. An agent (WASM) task records a reason only when it fails to start. <code>minga-ctl service ps</code> shows the same in its <code>ERROR</code> column, and the Docker API returns it in the task status. Then read the task’s logs with <code>minga-ctl service logs</code>.',
    sources: ['docs/minga-ctl-plan.md', 'CHANGELOG.md'],
  },
  {
    q: 'Why does one host run two processes?',
    a: 'In v1.1.0, <code>minga api</code> is its own Raft voter, so the manager and the API peer with each other. In the target design the API folds into the manager.',
    sources: ['README.md', 'docs/architecture.md'],
  },
  {
    q: 'Can I run more than one node?',
    a: 'The control plane works across nodes: Raft with mTLS, plus scheduling. Four limits apply: every node must be listed with <code>--peer</code> and started together, all nodes run the same version, containers on different nodes cannot reach each other yet, and every manager and worker must use one external TLS NATS server.',
    sources: ['README.md', 'docs/architecture.md'],
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
    a: 'Copy the v1.1.0 release archive (minga, nats-server, ctr and the licenses) and your image tarballs to the site. Run <code>ctr -n minga images import &lt;image&gt;.tar</code> on each node and start the roles. The published <code>install.sh</code> verifies <code>SHA256SUMS</code>.',
    sources: ['README.md', 'docs/release-1.1.0.md'],
  },
  {
    q: 'Does it run on Talos Linux?',
    a: 'Minga was validated through workload start on a docker-provisioned Talos v1.14 node before v1.0.1. Later releases have not been verified on Talos; bare-metal validation is also pending. The setup script runs Minga as two privileged <code>ContainerConfig</code> containers.',
    sources: ['docs/talos-deployment.md', 'talos/single-node.yaml'],
  },
  {
    q: 'Does a published port answer on the same host?',
    a: 'From v1.1.0, yes: a published port answers on <code>127.0.0.1</code> and on the node’s own address, and tasks reach each other through the node’s address. Only traffic addressed to the node is redirected, so a task calling a remote host on the same port number is left alone. A CI job checks these paths on a real kernel; v1.0.2 and older did not support host-local access.',
    sources: ['CHANGELOG.md', 'docs/release-1.1.0.md'],
  },
  {
    q: 'Is it secure? Is it compliant?',
    a: 'Raft mTLS, an API bearer token, NATS TLS, a built-in CA (<code>minga tls</code>) and content-addressed secret staging are implemented, and registry credentials stay off command lines. The API has one shared token, with no per-operator identity, roles or server-side audit yet (designed in ADR 0005, not built). Keep <code>:2375</code> and the Raft ports on a private network: the API token is what stands between the network and container creation. FIPS, SOC 2, VS-NfD and NATO are not assessed.',
    sources: ['CHANGELOG.md', 'README.md', 'docs/adr/0005-api-identity-and-roles.md'],
  },
  {
    q: 'What are agent workloads?',
    a: 'Compose services with <code>x-minga-agent</code> run as WASM modules under wasmtime with fuel and timeout limits, and their LLM calls go through an egress proxy with token budgets. Today the guest is pure compute: no WASI, network, env or clock, and memory limits are not enforced. Nodes need <code>--sandbox-runtime wasmtime</code>.',
    sources: ['docs/adr/0003-agent-workloads.md', 'README.md'],
  },
];
