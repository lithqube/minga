// Single source for every name, URL, CTA and toggle the site uses.
// Optional fields set to null are hidden everywhere; they never render as "0" or a placeholder.

const repoUrl = 'https://github.com/lithqube/minga';

export interface Testimonial {
  quote: string;
  name: string;
  role: string;
  org?: string;
  /** Public post or issue where the quote can be checked. Required. */
  sourceUrl: string;
}

export interface Mention {
  title: string;
  kind: 'post' | 'talk' | 'podcast' | 'newsletter';
  url: string;
}

export const site = {
  name: 'Minga',
  tagline: 'Run Compose stacks at the edge from one binary.',
  description:
    'A single-binary container and agent orchestrator for edge, on-prem and air-gapped sites, written in Rust. It runs Compose files natively and speaks enough of the Docker Swarm API to replace it.',
  /** The positioning sentence under the hero headline. */
  pitch:
    'For platform teams running edge, on-prem and air-gapped sites, Minga runs your Compose files on containerd without a Docker daemon, Kubernetes or etcd.',

  siteUrl: 'https://lithqube.github.io',
  /** GitHub Pages project path. Must match `base` in astro.config.mjs. */
  base: '/minga/',
  repoUrl,
  repoBranch: 'main',
  docsUrl: '/docs/',
  issuesUrl: `${repoUrl}/issues`,
  releasesUrl: `${repoUrl}/releases`,
  contributingUrl: `${repoUrl}/blob/main/CONTRIBUTING.md`,
  codeOfConductUrl: `${repoUrl}/blob/main/CONTRIBUTING.md#code-of-conduct`,
  securityUrl: `${repoUrl}/blob/main/CONTRIBUTING.md#security-issues`,
  goodFirstIssueUrl: `${repoUrl}/issues?q=is%3Aopen+label%3A%22good+first+issue%22`,
  /** Set once GitHub Discussions is enabled on the repo. */
  discussionsUrl: null as string | null,

  maintainer: { name: 'Cristhian Ferrufino', url: 'https://devferru.substack.com' },
  maintenanceStatus: 'Stable release. Upgrade every node together.',
  license: 'MIT',
  licenseUrl: `${repoUrl}/blob/main/LICENSE`,
  // The version is not configured here: it is read from CHANGELOG.md (src/lib/repo.ts).
  /** The release page and stable archives are published. Set to true once the tag for the CHANGELOG's latest version is pushed. */
  releasePublished: true,

  /** GitHub counts. Leave null to hide; fill in by hand or from a CI step. */
  stats: { stars: null as number | null, forks: null as number | null, contributors: null as number | null },

  /** Analytics are off unless a provider is set. `null` means no third-party request at all. */
  analytics: { provider: null as null | 'plausible' | 'google', id: '' },

  cta: {
    primary: { label: 'Run it on one Linux host', href: '/docs/quickstart/' },
    secondary: { label: 'View on GitHub', href: repoUrl },
  },

  nav: [
    { label: 'Docs', href: '/docs/' },
    { label: 'Architecture', href: '/architecture/' },
    { label: 'ADRs', href: '/adr/' },
    { label: 'Roadmap', href: '/roadmap/' },
    { label: 'Changelog', href: '/changelog/' },
    { label: 'Blog', href: '/blog/' },
    { label: 'Community', href: '/community/' },
  ],

  social: {
    discord: null as string | null,
    mailingList: null as string | null,
    twitter: null as string | null,
    mastodon: null as string | null,
  },

  /** The maintainer's newsletter; linked from /blog/ and the architecture section. */
  newsletter: {
    name: 'Dev-Ferru',
    url: 'https://devferru.substack.com',
    series: 'Cloud-Native Chronicles',
  } as { name: string; url: string; series: string } | null,

  socialProof: {
    testimonials: [] as Testimonial[],
    adopters: [] as { name: string; url: string }[],
    mentions: [] as Mention[],
    /** Where users can say they run Minga. Hidden when null. */
    shareUsageUrl: `${repoUrl}/issues/new?title=We+run+Minga&labels=adopter` as string | null,
  },

  ogImage: '/og-default.png',
};

/** Prefix an internal path with the GitHub Pages base. External URLs pass through. */
export function url(path: string): string {
  if (/^[a-z]+:/i.test(path) || path.startsWith('#')) return path;
  return site.base.replace(/\/$/, '') + (path.startsWith('/') ? path : `/${path}`);
}

export function absoluteUrl(path: string): string {
  return new URL(url(path), site.siteUrl).href;
}
