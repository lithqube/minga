# Minga website

The GitHub Pages site for Minga, built with [Astro](https://astro.build). It is published at
<https://lithqube.github.io/minga/>.

The site keeps no copy of the project docs. `README.md`, `CHANGELOG.md`, `CONTRIBUTING.md` and `docs/**` are read from
the repo root and rendered at build time, so editing those files updates the site. The build fails when an expected
README section or the "Not in vX.Y.Z" list disappears, or when a landing-page quickstart command no longer appears
verbatim in the README.

## Run locally

Requires Node 22.23+ and npm. Run everything from `site/`.

```bash
npm ci
npx playwright install chromium   # once: Mermaid diagrams render to SVG at build time
npm run dev                       # http://localhost:4321/minga/
```

| Script            | What it does                                              |
| ----------------- | --------------------------------------------------------- |
| `npm run dev`     | Dev server with hot reload                                |
| `npm run build`   | Static build into `dist/`                                 |
| `npm run preview` | Serve `dist/` at <http://localhost:4321/minga/>           |
| `npm run lint`    | ESLint (with jsx-a11y rules) and a Prettier check         |
| `npm run format`  | Prettier, in place                                        |
| `npm run deploy`  | Trigger the Pages workflow on `main` (needs the `gh` CLI) |

`node scripts/og-image.mjs` regenerates `public/og-default.png` and `public/favicon-32.png`.

## Deploy

`.github/workflows/pages.yml` lints and builds on every pull request that touches the site or the docs it renders, and
deploys to GitHub Pages on pushes to `main`. One-time setup: **Settings → Pages → Build and deployment → Source: GitHub
Actions**.

The site is a project site, so every URL lives under `/minga/`. `base` in `astro.config.mjs` comes from
`site.base` in `src/config/site.ts`. If you move to a custom domain, set `siteUrl` to the domain and `base` to `/`, and
add `public/CNAME`.

`robots.txt` is generated, but crawlers only read it at the domain root. On `github.io` the sitemap is discovered through
the `<link rel="sitemap">` tag and Search Console instead.

## Where things live

| Path                              | Contents                                                                        |
| --------------------------------- | ------------------------------------------------------------------------------- |
| `src/config/site.ts`              | Every name, URL, CTA, nav link, stat, social link and the analytics switch      |
| `src/data/`                       | Landing copy that isn't in the repo docs: features, comparison, FAQ, quickstart |
| `src/lib/repo.ts`                 | Reads and renders repo markdown; the build-time checks                          |
| `src/lib/routes.ts`               | Which repo file maps to which page; everything else links to GitHub             |
| `src/content/blog/`               | Blog posts (Markdown with frontmatter)                                          |
| `src/components/`, `src/layouts/` | UI                                                                              |
| `DESIGN.md`                       | Design brief: intent, wireframes, tokens, states, accessibility                 |

### Configuration

Optional fields set to `null` are hidden, never shown as "0" or a placeholder:

- `stats.stars`, `stats.forks`, `stats.contributors`: GitHub counts.
- `discussionsUrl`, `social.*`: community links (Discussions, Discord, mailing list, X, Mastodon).
- `socialProof.testimonials` / `adopters` / `mentions`: each testimonial needs a public `sourceUrl`.
- `analytics.provider`: `'plausible'` (set `id` to the site domain) or `'google'` (set `id` to the GA4 measurement ID).
  With `null`, the site makes no third-party requests.
- `releasePublished`: set to `true` once a GitHub release exists, so the hero stops saying "build from source".
