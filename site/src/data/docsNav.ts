// Docs sidebar. Static on purpose: order is editorial, not alphabetical.
export const docsNav = [
  {
    group: 'Start',
    items: [
      { label: 'Overview', href: '/docs/' },
      { label: 'Quickstart', href: '/docs/quickstart/' },
      { label: 'Examples', href: '/docs/examples/' },
    ],
  },
  {
    group: 'Deploy',
    items: [
      { label: 'Supported topologies', href: '/docs/topologies/' },
      { label: 'Backup and restore', href: '/docs/backup/' },
      { label: 'Talos Linux', href: '/docs/talos-deployment/', badge: 'partially validated' },
    ],
  },
  {
    group: 'Reference',
    items: [
      { label: 'CLI reference', href: '/docs/cli/' },
      { label: 'Agent usage', href: '/docs/agent-usage/' },
      { label: 'Release notes: v1.3.0 (candidate)', href: '/docs/release-1.3.0/', badge: 'pre-release' },
      { label: 'Release notes: v1.2.0', href: '/docs/release-1.2.0/' },
      { label: 'Architecture', href: '/architecture/' },
      { label: 'Decision records', href: '/adr/' },
      { label: 'Roadmap', href: '/roadmap/' },
      { label: 'Changelog', href: '/changelog/' },
    ],
  },
  {
    group: 'Background',
    items: [
      { label: 'NATS evaluation', href: '/docs/research/nats-evaluation/' },
      { label: 'Design document', href: '/docs/design/' },
      { label: 'Community', href: '/community/' },
    ],
  },
] as { group: string; items: { label: string; href: string; badge?: string }[] }[];

export const docsOrder = docsNav.flatMap((g) => g.items);
