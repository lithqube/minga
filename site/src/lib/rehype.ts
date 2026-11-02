// Rehype plugins shared by astro.config (blog posts) and src/lib/markdown.ts (repo docs).
import path from 'node:path';
import rehypeMermaid from 'rehype-mermaid';
import { site, url } from '../config/site';
import { githubUrl, routeFor } from './routes';
import { REPO_ROOT } from './paths';

type Node = {
  type: string;
  tagName?: string;
  properties?: Record<string, unknown>;
  children?: Node[];
  value?: string;
};

function walk(node: Node, fn: (n: Node) => void) {
  fn(node);
  node.children?.forEach((c) => walk(c, fn));
}

/**
 * Rewrites relative links in repo markdown: files with a site route go there,
 * everything else goes to the file on GitHub. Relative images load from GitHub raw.
 */
export function rehypeRepoLinks() {
  return (tree: Node, file: { path?: string }) => {
    if (!file.path) return;
    const sourceDir = path.dirname(path.relative(REPO_ROOT, file.path));
    if (sourceDir.startsWith('..')) return; // not a repo doc (e.g. a blog post)
    walk(tree, (n) => {
      if (n.type !== 'element' || !n.properties) return;
      const attr = n.tagName === 'a' ? 'href' : n.tagName === 'img' ? 'src' : null;
      const value = attr && n.properties[attr];
      if (typeof value !== 'string' || /^([a-z]+:|#|\/\/)/i.test(value)) return;
      const [target, anchor = ''] = value.split('#');
      const repoPath = path.posix.normalize(path.posix.join(sourceDir, target));
      if (n.tagName === 'img') {
        n.properties.src = `${site.repoUrl}/raw/${site.repoBranch}/${repoPath}`;
        return;
      }
      const route = routeFor(repoPath, anchor);
      n.properties.href = route ? url(route) : githubUrl(repoPath, anchor);
    });
  };
}

const textOf = (n: Node): string => (n.type === 'text' ? (n.value ?? '') : (n.children ?? []).map(textOf).join(''));

/** Marks a changelog's "## [Unreleased]" heading so nobody mistakes it for a tagged release. */
export function rehypeUnreleasedPill() {
  return (tree: Node) =>
    walk(tree, (n) => {
      if (n.tagName !== 'h2' || textOf(n).trim() !== '[Unreleased]') return;
      n.properties = { ...n.properties, id: 'unreleased' }; // keep the anchor stable despite the added text
      n.children?.push(
        { type: 'text', value: ' ' },
        {
          type: 'element',
          tagName: 'span',
          properties: { className: ['pill', 'warn'] },
          children: [{ type: 'text', value: 'not in a tagged release' }],
        },
      );
    });
}

/** Code blocks get a keyboard-scrollable, labelled <pre> (WCAG 2.1.1); task-list checkboxes get a label. */
export function rehypeA11y() {
  return (tree: Node) =>
    walk(tree, (n) => {
      if (!n.properties) return;
      if (n.tagName === 'pre') {
        n.properties.tabIndex = 0;
        const lang = n.properties.dataLanguage;
        n.properties.ariaLabel = typeof lang === 'string' && lang !== 'plaintext' ? `${lang} code` : 'Code';
      } else if (n.tagName === 'input' && n.properties.type === 'checkbox') {
        n.properties.ariaLabel = n.properties.checked ? 'Done' : 'Not done';
      }
    });
}

/**
 * Keeps Mermaid labels readable at any width: each diagram renders at no less than 75% of its natural
 * size (16px labels stay >= 12px) inside a labelled scroll region, instead of shrinking to the column.
 */
export function rehypeDiagramScroll() {
  return (tree: Node) => {
    const visit = (parent: Node) =>
      parent.children?.forEach((n, i) => {
        const id = n.properties?.id;
        if (n.tagName === 'svg' && typeof id === 'string' && id.startsWith('mermaid')) {
          const natural = Number(String(n.properties?.style ?? '').match(/max-width:\s*([\d.]+)px/)?.[1]);
          if (natural) {
            delete n.properties!.width;
            n.properties!.style = `width: max(100%, ${Math.round(natural * 0.75)}px); max-width: ${Math.round(natural)}px; height: auto;`;
          }
          parent.children![i] = {
            type: 'element',
            tagName: 'div',
            properties: {
              className: ['diagram-scroll'],
              role: 'region',
              ariaLabel: 'Diagram, scrolls horizontally',
              tabIndex: 0,
            },
            children: [n],
          };
        } else visit(n);
      });
    visit(tree);
  };
}

/** Pre-rendered Mermaid: inline SVG at build time; on failure the source block stays (no client JS). */
export const mermaidPlugin = [
  rehypeMermaid,
  {
    strategy: 'inline-svg',
    mermaidConfig: {
      theme: 'base',
      themeVariables: {
        darkMode: true,
        background: '#121829',
        mainBkg: '#16213a',
        primaryColor: '#16213a',
        primaryTextColor: '#e5e9f0',
        primaryBorderColor: '#22d3ee',
        secondaryColor: '#1b1530',
        tertiaryColor: '#0f1626',
        lineColor: '#94a3b8',
        textColor: '#e5e9f0',
        clusterBkg: '#0f1626',
        clusterBorder: '#1f2a44',
        edgeLabelBackground: '#121829',
        actorBkg: '#16213a',
        actorBorder: '#22d3ee',
        actorTextColor: '#e5e9f0',
        signalColor: '#94a3b8',
        signalTextColor: '#e5e9f0',
        noteBkgColor: '#1b1530',
        noteTextColor: '#e5e9f0',
        fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
      },
    },
    errorFallback: (element: Node) => {
      console.warn('[mermaid] render failed; keeping the diagram source');
      return element;
    },
  },
] as const;

export const shikiConfig = { theme: 'github-dark-default' } as const;
export const syntaxHighlight = { type: 'shiki', excludeLangs: ['mermaid'] } as const;
