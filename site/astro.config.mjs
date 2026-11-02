// @ts-check
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import { unified } from '@astrojs/markdown-remark';
import { site } from './src/config/site';
import { mermaidPlugin, rehypeA11y, shikiConfig, syntaxHighlight } from './src/lib/rehype';

export default defineConfig({
  site: site.siteUrl,
  // GitHub Pages project site: https://<user>.github.io/<repo>/
  base: site.base,
  trailingSlash: 'always',
  build: { format: 'directory' },
  integrations: [sitemap()],
  markdown: {
    processor: unified({ rehypePlugins: [/** @type {never} */ (mermaidPlugin), rehypeA11y] }),
    shikiConfig,
    syntaxHighlight: /** @type {never} */ (syntaxHighlight),
  },
});
