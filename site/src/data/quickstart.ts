// The landing-page quickstart. Commands must appear verbatim in README.md (checked at build),
// so the site can never drift from the verified path.
export interface Step {
  title: string;
  code: string;
  lang?: string;
  note?: string;
  /** New content not in the README (e.g. the sample compose file). Skips the drift check. */
  siteOnly?: boolean;
}

export const prerequisites = [
  'Linux with root (the supervisor creates netns, bridges and iptables rules)',
  'Install from the v1.1.1 release archive; it includes minga, nats-server and ctr.',
  'containerd running (1.7 or newer for private registries)',
  'iproute2, iptables and procps (any standard server install)',
  'Free loopback ports 7001, 7002, 2375 and 9100',
];

export const steps: Step[] = [
  {
    title: 'Install minga + nats-server (install.sh verifies SHA256SUMS)',
    note: 'The installer verifies SHA256SUMS. Supported distro guidance: Debian 11+, Ubuntu 20.04+, Fedora 32+; add --with-systemd for the unit.',
    code: `curl -fsSL https://github.com/lithqube/minga/releases/latest/download/install.sh | sh`,
  },
  {
    title: 'Auto-pull or preload workload images',
    note: 'Missing images are pulled by containerd through its Transfer API. Private registries use $DOCKER_CONFIG/config.json, else ~/.docker/config.json: credential helpers or inline auths, never on a command line. Preload images for air-gapped sites.',
    code: 'sudo ctr -n minga images pull docker.io/library/nginx:1.27',
  },
  {
    title: 'Write a minimal compose.yaml',
    lang: 'yaml',
    siteOnly: true,
    code: `services:
  web:
    image: docker.io/library/nginx:1.27
    ports:
      - "8080:80"`,
  },
  {
    title: 'Terminal 1: start the manager',
    code: `sudo minga manager --node-id 1 --bind 127.0.0.1:7001 --peer 2=127.0.0.1:7002 \\
  --data-dir /var/lib/minga --runtime containerd --metrics-bind 127.0.0.1:9100`,
  },
  {
    title: 'Terminal 2: start the Docker Engine API',
    note: 'Today minga api is its own Raft voter, so one host runs two processes that peer with each other.',
    code: `sudo MINGA_API_TOKEN=change-me minga api --node-id 2 --bind 127.0.0.1:7002 \\
  --peer 1=127.0.0.1:7001 --data-dir /var/lib/minga-api --api-bind 127.0.0.1:2375`,
  },
  {
    title: 'Deploy the stack and check readiness',
    code: `MINGA_API_TOKEN=change-me minga deploy -c compose.yaml shop
curl -s 127.0.0.1:9100/readyz`,
  },
];

/** Output format taken from crates/minga-cli/src/deploy.rs; re-capture from a real run when it changes. */
export const deployOutput = `network shop_default: created
service shop.web: created

deployment of stack \`shop\`:
  [ok ] shop.web: created — 1/1 task(s) placed`;

export function assertInReadme(readme: string) {
  for (const s of steps.filter((s) => !s.siteOnly)) {
    for (const line of s.code.split('\n')) {
      if (!readme.includes(line.trim())) {
        throw new Error(`[site] quickstart command is not in README.md, so the site drifted: ${line.trim()}`);
      }
    }
  }
}

/** Shown over deployOutput everywhere, until it is replaced by output captured from a real run. */
export const deployOutputLabel = 'Expected output (format taken from the CLI source, not captured from a run)';
