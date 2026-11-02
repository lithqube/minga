// Reads and renders the repo's own markdown, so the site never keeps a second copy.
// Missing files or headings fail the build: a silently stale site is worse than a red CI run.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { unified } from '@astrojs/markdown-remark';
import type { MarkdownHeading } from '@astrojs/markdown-remark';
import { REPO_ROOT } from './paths';
import {
  mermaidPlugin,
  rehypeA11y,
  rehypeDiagramScroll,
  rehypeRepoLinks,
  rehypeUnreleasedPill,
  shikiConfig,
  syntaxHighlight,
} from './rehype';

const renderer = await unified({
  rehypePlugins: [rehypeRepoLinks, rehypeUnreleasedPill, mermaidPlugin as never, rehypeDiagramScroll, rehypeA11y],
}).createRenderer({ shikiConfig, syntaxHighlight: syntaxHighlight as never });

export function readRepo(rel: string): string {
  const file = path.join(REPO_ROOT, rel);
  if (!fs.existsSync(file)) throw new Error(`[site] expected repo file is missing: ${rel}`);
  return fs.readFileSync(file, 'utf8');
}

export interface Rendered {
  title: string;
  html: string;
  headings: MarkdownHeading[];
}

/** Renders markdown that lives at `rel` in the repo. A leading `# Title` becomes `title`, not part of the body. */
export async function renderRepoMarkdown(markdown: string, rel: string): Promise<Rendered> {
  const match = markdown.match(/^#\s+(.+)\n/);
  const body = match ? markdown.slice(match[0].length) : markdown;
  const { code, metadata } = await renderer.render(body, {
    fileURL: pathToFileURL(path.join(REPO_ROOT, rel)),
  });
  // Titles go into <title> and <h1> as plain text, so drop markdown code ticks.
  return { title: match?.[1].replace(/`/g, '').trim() ?? '', html: code, headings: metadata.headings };
}

export const renderRepoFile = (rel: string) => renderRepoMarkdown(readRepo(rel), rel);

/** One `## Heading` section of README.md, including its subsections. */
export function readmeSection(heading: string): string {
  const lines = readRepo('README.md').split('\n');
  const start = lines.findIndex((l) => l.trim() === `## ${heading}`);
  if (start < 0) throw new Error(`[site] README.md has no "## ${heading}" section`);
  const end = lines.findIndex((l, i) => i > start && l.startsWith('## '));
  return lines.slice(start, end < 0 ? undefined : end).join('\n');
}

/** The README's "Not in v0.1.0" list plus the caveat line after it, as markdown. */
export function readmeLimits(): { items: string[]; note: string } {
  const quote = readRepo('README.md')
    .split('\n')
    .filter((l) => l.startsWith('>'))
    .map((l) => l.replace(/^>\s?/, ''));
  const start = quote.findIndex((l) => /^\*\*Not in `v[\d.]+`:\*\*/.test(l));
  if (start < 0) throw new Error('[site] README.md status block has no "**Not in `vX.Y.Z`:**" list');
  const items: string[] = [];
  for (const l of quote.slice(start + 1)) {
    if (l.startsWith('- ')) items.push(l.slice(2));
    else if (items.length && l.trim() === '') break;
  }
  if (!items.length) throw new Error('[site] README.md "Not in" list is empty');
  const note = quote.findLast((l) => /not assessed|not been validated/i.test(l)) ?? '';
  return { items, note };
}

export interface Release {
  version: string;
  date: string;
}

/** The latest tagged release, from the first `## [x.y.z] — date` heading in CHANGELOG.md. */
export function latestRelease(): Release {
  const m = readRepo('CHANGELOG.md').match(/^## \[(\d+\.\d+\.\d+)\]\s*[—-]\s*(\d{4}-\d{2}-\d{2})/m);
  if (!m) throw new Error('[site] CHANGELOG.md has no released "## [x.y.z] — YYYY-MM-DD" heading');
  return { version: m[1], date: m[2] };
}

export interface Adr {
  slug: string;
  number: string;
  title: string;
  status: string;
  date: string;
  deviations: number;
  hasImplementationStatus: boolean;
  rel: string;
}

export function listAdrs(): Adr[] {
  const dir = path.join(REPO_ROOT, 'docs/adr');
  return fs
    .readdirSync(dir)
    .filter((f) => /^\d{4}-.+\.md$/.test(f))
    .sort()
    .map((f) => {
      const rel = `docs/adr/${f}`;
      const md = readRepo(rel);
      // ADRs use either "- Status: X" or "**Status:** X".
      const field = (name: string) =>
        md.match(new RegExp(`^(?:- ${name}:|\\*\\*${name}:\\*\\*)\\s*(.+)$`, 'm'))?.[1].trim() ?? '';
      const status = field('Status');
      if (!status) console.warn(`[site] ${rel}: status not found`);
      const impl = md.split(/^## Implementation status.*$/m)[1] ?? '';
      return {
        slug: f.replace(/\.md$/, ''),
        number: f.slice(0, 4),
        title:
          md
            .match(/^#\s+(.+)$/m)?.[1]
            .replace(/^ADR \d+:\s*/, '')
            .replace(/`/g, '') ?? f,
        status: status || 'unknown',
        date: field('Date').split(';')[0],
        deviations: (impl.match(/\*\*Deviation/g) ?? []).length,
        hasImplementationStatus: impl !== '',
        rel,
      };
    });
}
