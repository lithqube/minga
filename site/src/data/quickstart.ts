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
  'Install from a release archive; it includes minga, nats-server and ctr. These steps use the v1.3.0-rc.2 layout (one minga manager process serves the API); v1.2.0 needs minga api as a second process.',
  'containerd running (1.7 or newer for private registries)',
  'iproute2, iptables and procps (any standard server install)',
  'Free loopback ports 7001, 2375 and 9100',
];

export const steps: Step[] = [
  {
    title: 'Install minga + nats-server (install.sh verifies SHA256SUMS)',
    note: 'The installer verifies SHA256SUMS. This installs v1.2.0, the latest release; for the steps below add --version v1.3.0-rc.2 (see the README). Supported distro guidance: Debian 11+, Ubuntu 20.04+, Fedora 32+; add --with-systemd for the unit.',
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
    title: 'Start the manager, API and NATS sidecar',
    code: `sudo MINGA_API_TOKEN=change-me minga manager --node-id 1 \\
  --bind 127.0.0.1:7001 --api-bind 127.0.0.1:2375 \\
  --data-dir /var/lib/minga --runtime containerd --metrics-bind 127.0.0.1:9100`,
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
