// Renders public/og-default.png (1200x630) and public/favicon-32.png with Playwright.
// Placeholder art until the Chiloé illustration exists (DESIGN.md §12.3). Run: node scripts/og-image.mjs
import { readFileSync } from 'node:fs';
import { chromium } from 'playwright';

const favicon = readFileSync(new URL('../public/favicon.svg', import.meta.url), 'utf8');
const og = `<!doctype html><html><body style="margin:0">
<div style="width:1200px;height:630px;box-sizing:border-box;padding:80px 96px;background:
radial-gradient(700px 420px at 92% 8%,rgba(232,121,249,.28),transparent 65%),
radial-gradient(640px 400px at 0% 100%,rgba(34,211,238,.22),transparent 65%),#0b0f1a;
color:#e5e9f0;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;display:flex;flex-direction:column;justify-content:space-between">
  <div style="display:flex;align-items:center;gap:28px">
    <svg width="120" height="120" viewBox="0 0 32 32"><path d="M8 15 16 8l8 7v7H8z" fill="none" stroke="#22d3ee" stroke-width="1.6" stroke-linejoin="round"/><path d="M14 22v-4h4v4" fill="none" stroke="#22d3ee" stroke-width="1.2"/><path d="M6 23h20" stroke="#e5e9f0" stroke-width="1.6" stroke-linecap="round"/><path d="M3 27q3.25-2.5 6.5 0t6.5 0 6.5 0 6.5 0" fill="none" stroke="#e879f9" stroke-width="1.4" stroke-linecap="round"/></svg>
    <span style="font-size:88px;font-weight:700;letter-spacing:-2px">minga</span>
  </div>
  <div style="font-size:58px;font-weight:700;line-height:1.15;max-width:960px">Run Compose stacks at the edge from one binary.</div>
  <div style="font-size:28px;color:#94a3b8">No Docker daemon · no Kubernetes · no etcd · air-gap ready</div>
</div></body></html>`;

const browser = await chromium.launch();
const page = await browser.newPage();
await page.setViewportSize({ width: 1200, height: 630 });
await page.setContent(og);
await page.screenshot({ path: 'public/og-default.png' });
await page.setViewportSize({ width: 32, height: 32 });
await page.setContent(`<body style="margin:0">${favicon.replace('<svg ', '<svg width="32" height="32" ')}</body>`);
await page.screenshot({ path: 'public/favicon-32.png', omitBackground: true });
await browser.close();
console.log('wrote public/og-default.png and public/favicon-32.png');
