// Maps repo markdown files to site routes. Anything unmapped links to GitHub instead.
import { site } from '../config/site';

/** Repo-relative markdown path -> site route (without base). */
const routes: Record<string, string> = {
  'CHANGELOG.md': '/changelog/',
  'docs/README.md': '/docs/',
  'docs/architecture.md': '/architecture/',
  'docs/roadmap-v0.1.0.md': '/roadmap/',
  'docs/talos-deployment.md': '/docs/talos-deployment/',
  'docs/design.md': '/docs/design/',
  'docs/research/nats-evaluation.md': '/docs/research/nats-evaluation/',
};

/** README anchors that have their own page. */
const readmeAnchors: Record<string, string> = {
  'quick-start-one-linux-host': '/docs/quickstart/',
  'deployment-topologies': '/docs/quickstart/#deployment-topologies',
  architecture: '/architecture/',
  roadmap: '/roadmap/',
};

export function routeFor(repoPath: string, anchor = ''): string | null {
  if (repoPath === 'README.md') return readmeAnchors[anchor] ?? null;
  const adr = repoPath.match(/^docs\/adr\/(.+)\.md$/);
  const route = adr ? `/adr/${adr[1]}/` : routes[repoPath];
  return route ? route + (anchor ? `#${anchor}` : '') : null;
}

export function githubUrl(repoPath: string, anchor = ''): string {
  return `${site.repoUrl}/blob/${site.repoBranch}/${repoPath}${anchor ? `#${anchor}` : ''}`;
}
