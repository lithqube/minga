# Minga site: design document (GitHub Pages, Astro, `site/`)

Everything below comes from the repo as of branch `release-image-installer`. The citation keys used throughout:

| Key | File |
|---|---|
| R | `/Users/admin/dev/minga/README.md` |
| C | `/Users/admin/dev/minga/CHANGELOG.md` |
| CON | `/Users/admin/dev/minga/CONTRIBUTING.md` |
| A | `/Users/admin/dev/minga/docs/architecture.md` |
| RM | `/Users/admin/dev/minga/docs/roadmap-v0.1.0.md` |
| T | `/Users/admin/dev/minga/docs/talos-deployment.md` |
| ADR1–4 | `/Users/admin/dev/minga/docs/adr/000{1..4}-*.md` |
| D | `/Users/admin/dev/minga/crates/minga-cli/src/deploy.rs` (deploy output strings) |
| TR | `/Users/admin/dev/minga/crates/minga-scheduler/src/translate.rs` (naming: `shop.web`, `shop_default`) |
| FX | `/Users/admin/dev/minga/crates/minga-compose/tests/fixtures/edge-ai.yaml`, `wordpress.yaml` |

---

## 1. Intent Brief

- **User:** platform and ops engineers who run edge, on-prem or air-gapped sites. They're evaluating Minga as a Docker Swarm replacement. They can read Rust and Raft jargon, and they distrust marketing. Contributors are the secondary audience.
- **Primary action:** run the one-host quickstart, from `install.sh` through `ctr` preload, `minga manager` + `minga api`, and `minga deploy`.
- **Secondary actions:** read the limits and the comparison before committing time, read the architecture and ADRs, star the repo, open an issue or contribute.
- **Context:** the evaluation happens on a laptop next to a Linux box or VM. Often the reader's own work network is restricted. Commands get copied straight into a root shell, and the reader arrives skeptical.
- **Worst case:** the reader follows the quickstart, hits an undisclosed limit (no image pull, `curl localhost:<port>` fails because of the hairpin limit, a second node can't join) and decides the project lied. A fabricated metric or logo would do the same damage. Second worst: a copied command fails because the site drifted from the README.

## 2. Design rationale

- **Limits go in the first screen.** The hero carries a status line (`v0.1.0 · verified on one Linux host · 6 known limits ↓`) that jumps to "When not to use". The worst case is a broken trust contract, so the honesty has to come before the first scroll.
- **Single source, enforced at build time.** ADRs, changelog, roadmap, architecture, Talos, CONTRIBUTING and the README quickstart and limits sections are loaded from the repo markdown. The build fails if an expected README heading or the "Not in `v0.1.0`" list can't be found. This follows the project's "no silent failures" rule (CON §Architecture principles 5). Drift is the second worst case.
- **Code and diagrams sit on an always-dark "terminal" surface in both themes.** One Shiki theme and one Mermaid theme serve both modes, which halves the theming and contrast work and fits the industrial tone. Light mode only changes the page chrome.
- **Near-zero JS.** Mobile nav, FAQ and the docs TOC on mobile use `<details>`. The only scripts are the theme toggle (inline, under 1 KB) and the copy button. Fonts are system stacks: no webfont requests, which protects the Lighthouse 95+ target.
- **Empty means absent.** Testimonials, adopters, star counts, Discord, mailing list and Twitter are optional config fields. When a field is unset, its component renders nothing. A testimonial without a verifiable `sourceUrl` fails the build.

**Pushback on the brief (decisions I made, user can override):**
1. The "Get started in 5 minutes" CTA claims a number nobody has measured, and the prerequisites (root, containerd, preloaded images) can't be done in 5 minutes on a fresh box. I keep the label because the brief requires it, but it sits in config and a prerequisites line goes directly under it. I recommend "Run it on one Linux host" instead (see Open question 1).
2. The "Production-ready defaults" and "Performance" feature cards would be dishonest as written. They're reframed in §6.
3. The brief's "cloud-native" wording is a buzzword and also misleading, since Minga isn't Kubernetes. That card becomes "Built for edge and air-gap".

---

## 3. Information architecture

### 3.1 Route map: source to page

| Route | Source | Kind |
|---|---|---|
| `/` | new, every claim cited to R/C/A/ADR | new copy (§5) |
| `/docs/` | `docs/README.md` rendered (links rewritten) | single source |
| `/docs/quickstart/` | R section `## Quick start: one Linux host` plus `### Container image`, then R `## Deployment topologies` | extracted by heading |
| `/docs/examples/` | new prose around fixtures imported raw from FX and the ADR3 `x-minga-agent` snippet | new + single source |
| `/docs/talos/` | `docs/talos-deployment.md` with an injected "Not validated" callout | single source |
| `/docs/contributing/` | `CONTRIBUTING.md` | single source |
| `/docs/research/nats/` | `docs/research/nats-evaluation.md` | single source |
| `/architecture/` | R `## Architecture` (diagrams and design principles), then `docs/architecture.md` in full | extracted + single source |
| `/adr/` | index built from `docs/adr/*.md` front lines (title, status, date) | generated |
| `/adr/[slug]/` | `docs/adr/0001-language-split.md`, giving slug `0001-language-split` | single source |
| `/roadmap/` | R `## Roadmap` ("Next, in order") on top, then `docs/roadmap-v0.1.0.md` under a "v0.1.0 gates (historical)" heading | extracted + single source |
| `/changelog/` | `CHANGELOG.md`, with an anchor per `## [x.y.z]` and `## [Unreleased]` | single source |
| `/blog/`, `/blog/[slug]/` | `site/src/content/blog/*.md` | new |
| `/community/` | new short page, links into `/docs/contributing/` | new |
| `/404.html` | new | new |

### 3.2 Markdown pipeline rules (spec, not code)

1. **Link rewriting.** A relative `.md` link whose target has a route (for example `adr/0001-language-split.md` or `../CHANGELOG.md`) maps to that route under `/minga/`. Any other repo-relative link (`../CLAUDE.md`, `../scripts/minga.service`, `.planning/STATE.md`, `../Wave-9-PLAN.md`, `LICENSE`) becomes `https://github.com/lithqube/minga/blob/main/<path>`. Anchors are preserved. If an anchor doesn't exist on the target page, the build prints a warning, not an error.
2. **Headings.** When a source file starts with `# Title`, that line becomes the page `<h1>` and the layout adds no second title. Sections extracted from the README (`## …`) are rendered under a site-authored `<h1>`, with heading levels left as they are.
3. **Mermaid** is rendered to inline SVG at build time (rehype-mermaid with the `inline-svg` strategy, and Playwright Chromium in the Pages workflow). The SVG always sits on the dark diagram surface. Mermaid's runtime is several hundred KB of JS, which rules out client-side rendering. If rendering fails, the block falls back to the Mermaid source in a code block with a "View rendered on GitHub" link (see State S9).
4. **Version** comes from the first released heading in C (`## [0.1.0] — 2026-11-01`), not from `site.ts`. That means it can't go stale.
5. **ADR metadata parsing has to accept both header styles.** ADR1–3 use `- Status: Accepted`, while ADR4 uses `**Status:** Accepted` / `**Deciders:**`. The parser should handle both, or ADR4 should be normalized (Open question 9).
6. **Pages workflow trigger.** `.github/workflows/pages.yml` must run on changes to `site/**`, `docs/**`, `README.md`, `CHANGELOG.md` and `CONTRIBUTING.md`. Without that, single source silently goes stale.
7. **Astro config.** `site: 'https://lithqube.github.io'`, `base: '/minga/'`, `trailingSlash: 'always'`, `build.format: 'directory'`. Every internal href goes through `import.meta.env.BASE_URL`, including links rewritten from markdown.

### 3.3 New content

- **Landing copy:** §5.
- **Examples page** (`/docs/examples/`):
  1. **Minimal web** is the quickstart's nginx stack plus the expected deploy output.
  2. **Edge AI site (`plant-7`)** shows the FX `edge-ai.yaml` with its real `minga validate` output (R L211–216). It's labelled "validate-only". The fixture uses a `file:` secret, which deploy rejects because only external secrets are accepted (RM §Secrets). It also references `registry.local` images that would have to be preloaded, and its healthcheck and `update_config` are carried but not enforced (R, C).
  3. **Agent workload** uses the `x-minga-agent` snippet from ADR3 §2, under a callout: "Pure compute today: no WASI, network, env or clock; `memory_limit` and `token_budget` not enforced" (ADR3 Implementation status).
  4. **Traefik v3 route** is held until a real test compose exists in the repo. Gate 4 proved it (RM, C), but I found no compose file for it. I won't invent one (Open question 6).
- **First blog post:** "Minga v0.1.0: seven gates, and what's still missing", dated 2026-11-01, author from `site.ts` maintainer. Outline:
  1. Why Minga (R §Why Minga, 3 bullets).
  2. The gate method: each gate had an executable exit criterion (RM §The release plan). Quote the B1 probe output (RM L51–62) as the story of "a deploy that succeeded into an empty cluster".
  3. What shipped (C 0.1.0, condensed).
  4. What's not in it, verbatim list (R L18–27).
  5. The structural limit that matters most: every node is a Raft voter (A §Known structural limits 1).
  6. What's next (R §Roadmap 1–6).
  7. Try it (link to the quickstart).
  - No performance numbers, because none exist.

---

## 4. Design tokens

### 4.1 Color. Contrast ratios computed with WCAG relative luminance.

> **Superseded by §12.2** (neon-noir palette aligned with Dev-Ferru). The amber palette below is kept as the fallback option.

**Dark theme (default)**

| Token | Hex | Use | Contrast |
|---|---|---|---|
| `--bg` | `#0f1214` | page | n/a |
| `--surface` | `#171b1e` | cards, code, diagrams (both themes) | n/a |
| `--border` | `#2c3236` | decorative dividers only | 1.45:1 on bg (decorative; never the only signal) |
| `--text` | `#e6e8e6` | body | 15.3:1 on bg, 14.1:1 on surface |
| `--muted` | `#9aa39e` | meta, captions, code comments | 7.3:1 on bg, 6.7:1 on surface |
| `--accent` | `#f0b429` (amber) | links, primary button bg, focus ring | 10.1:1 on bg, 9.3:1 on surface. Button text `#0f1214` on amber: 10.1:1 |
| `--ok` | `#4ade80` | `$` prompt, "Yes" marker, `[ok ]` | 10.8:1 on bg, 9.9:1 on surface |
| `--warn` | `#f87171` | "No" marker, limit callouts | 6.8:1 on bg |

**Light theme (page chrome only; code and diagram panels stay on dark `--surface`)**

| Token | Hex | Contrast |
|---|---|---|
| `--bg` | `#f7f7f4` | n/a |
| `--surface-page` | `#ffffff` (cards) | n/a |
| `--text` | `#15191c` | 16.5:1 on bg, 17.7:1 on white |
| `--muted` | `#545b5f` | 6.4:1 on bg, 6.9:1 on white |
| `--accent` | `#8a5a00` (dark amber) | 5.5:1 on bg. White text on it: 5.9:1. Focus ring 5.5:1 (needs ≥3:1) |
| `--ok` | `#1a7f37` | 4.7:1 on bg |
| `--warn` | `#b42318` | 6.1:1 on bg |

- Links are always underlined. Color is never the only signal.
- The Shiki theme must be checked against `--surface`. Override its comment color to `--muted` (6.7:1).

### 4.2 Type

- **Mono:** `ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace`. Used for headings, nav, badges, tables of flags and code.
- **Sans:** `system-ui, -apple-system, "Segoe UI", Roboto, sans-serif`. Used for body prose.
- **Scale** (ratio 1.2, root 16px):

| Step | rem | Use |
|---|---|---|
| -1 | 0.833 | meta, pills, footer |
| 0 | 1.000 | body, code |
| 1 | 1.200 | h4, card titles |
| 2 | 1.440 | h3 |
| 3 | 1.728 | h2 |
| 4 | 2.074 | h1 (docs) |
| 5 | clamp(2.074rem, 5vw, 2.986rem) | hero h1 |

- Line height is 1.6 for body and 1.2 for headings. Prose measure is `72ch`.

### 4.3 Space, radius, layout

- Spacing uses a 4px base: `4 8 12 16 24 32 48 64 96`. Sections are 64px apart on mobile and 96px on desktop.
- Radius is `2px` everywhere (industrial). No shadows, only 1px borders.
- Breakpoints: `sm 640`, `md 960`, `lg 1200`. Content max width is 1200px. The docs grid is `240px | minmax(0,72ch) | 200px`.
- Header is sticky at 56px, with `scroll-padding-top: 72px` (WCAG 2.4.11 focus not obscured).

---

## 5. Landing copy

### 5.1 Hero

- **Eyebrow:** `v0.1.0 · MIT · Linux amd64 / arm64`. Sources: version from C, license R L324, architectures C "Added" (archives x86_64/aarch64, image amd64/arm64).
- **H1:** **Run Compose stacks at the edge from one binary.**
- **Sub (the brief's formula):** For platform teams running edge, on-prem and air-gapped sites, Minga runs your Compose files on containerd without a Docker daemon, Kubernetes or etcd. *(R L7, L37)*
- **Primary CTA:** `[Get started in 5 minutes →]` links to `/docs/quickstart/`. The label is a config field (Open question 1).
- **Secondary CTA:** `[View on GitHub]`.
- **Prereq line (muted, mono):** `Needs: Linux · root · containerd · images preloaded with ctr` *(R L170)*
- **Status line:** `Verified on one Linux host (containerd 2.2.1). No image pull, no cross-node networking yet. → Known limits` (anchor to `#when-not`) *(R L11–23, C 0.1.0)*
- **Terminal block** (copyable, `$` prompts stripped on copy, output lines not copyable):

```
$ curl -fsSL https://github.com/lithqube/minga/releases/latest/download/install.sh | sh
$ sudo ctr -n minga images pull docker.io/library/nginx:1.27
$ minga validate -c compose.yaml
# with `minga manager` and `minga api` running (see quickstart):
$ MINGA_API_TOKEN=change-me minga deploy -c compose.yaml shop

deployment of stack `shop`:
  [ok ] shop.web: created — 1/1 task(s) placed
```

The output format is taken from D L199–203 and the names from TR L9–11. **It must be re-captured from a real run before publishing.** I derived it from code, not from execution.

- **Hero diagram** is a hand-authored inline SVG (no Mermaid on the landing page):

```
compose.yaml ─▶ minga api ─▶ Raft (desired state, placement)
                                 │
              scheduler (leader) ┘─▶ JetStream ─▶ supervisor ─▶ containerd
                                     status · logs ◀───────────┘
```

### 5.2 Problem: "Swarm was built for a datacenter. Your sites aren't one."

- **Edge sites:** a handful of ARM boards, an uplink that drops, and no room for a heavyweight control plane. *(R §Why Minga)*
- **Air-gapped networks:** nothing pulls from a registry and every dependency has to be vendored. *(R §Why Minga; CON "every dependency must be vendored")*
- **Agent workloads:** they need sandboxed execution, token budgets and LLM egress control, not just "run a container". *(R §Why Minga)*
- **Closing line:** Your stacks are already written as Compose. Minga's rule is that Compose files deploy unchanged, and keys it doesn't understand fail loudly instead of being skipped. *(R §Design principles; CLAUDE.md)*

### 5.3 Solution: "One binary that reads your compose.yaml."

**Concept:** Every role (manager, worker, Docker API) is the same `minga` binary. The only thing next to it is the checksum-pinned `nats-server` it supervises. Desired state and placement live in embedded Raft (openraft + redb). Task status and logs move over NATS JetStream. *(R §Architecture, L37; ADR1; ADR2)*

Do **not** say "liveness" here. R L37 says JetStream carries liveness, but A §Known structural limits 2 and ADR2 "Deviation: liveness" say leases go through Raft today. See Finding 1.

**Benefits:**
1. **No Docker daemon, no Kubernetes, no etcd.** containerd plus one binary. *(R L37)*
2. **Your Docker tooling keeps working.** `docker stack deploy`, `docker service ls`, `docker node ls` and Traefik v3's swarm provider run against `minga api`. *(R §Features)*
3. **Failures are loud.** Unknown Compose keys are rejected with suggestions, an unplaceable deploy returns HTTP 409, and `/readyz` returns 503 with the reason. *(R §Features; C 0.1.0 "Cluster lifecycle"; ADR4 Implementation status)*
4. **The air-gap path has no network step.** The release archive carries `minga`, `nats-server` and licenses, and images go in with `ctr -n minga images import`. *(R §Deployment topologies 4)*
5. **Agent workloads live in the same file (experimental).** `x-minga-agent` services run as WASM under wasmtime with fuel and timeout limits. *(R §Features; ADR3)*

**How it works (numbered; sequence from R §How a deploy becomes a running container):**
1. `minga deploy` posts networks, secrets and services to `minga api`, a Docker Engine API v1.41 subset.
2. The API checks that the service is placeable (409 if not) and writes desired state to Raft. Writes are forwarded to the leader.
3. The leader's scheduler places tasks with deterministic IPs and sends an Assign command over JetStream.
4. The node supervisor creates a netns, veth and IP, stages secrets, and starts the container through containerd.
5. Status and log chunks flow back over JetStream. `minga logs -f` reads the retained backlog.

### 5.4 When to use / When not to use (`#when-not`)

**Use Minga when:**
- you run a Compose stack on **one Linux host** at an edge or on-prem site and want the Docker API surface (CLI, Traefik) without dockerd. *(R §Deployment topologies 1: "the verified path")*
- the site is **air-gapped** and you can ship images as tarballs. *(R §Deployment topologies 4)*
- you want an orchestrator whose limits are written down. Each ADR ends with an *Implementation status* section. *(docs/README.md)*
- you want to **experiment** with sandboxed WASM agent workloads next to containers. *(ADR3)*
- you want to **contribute** to a Rust orchestrator early. *(CON)*

**Don't use Minga (yet) when:** this is the verbatim README list, extracted at build time (R L18–27), followed by site-authored items from A:
- you need **image pull** from a registry, because images must be preloaded into the `minga` containerd namespace.
- containers on **different nodes** have to talk to each other, because there's no WireGuard mesh yet.
- you need to **add a node to a running cluster**, because Raft membership is fixed at bootstrap.
- you rely on **healthcheck enforcement, spec-update reprovisioning, automatic rollback or host-local port hairpinning**.
- you're deploying on **Talos**. The guide hasn't been validated on a Talos host and needs a system extension that doesn't exist yet. *(T)*
- you need **FIPS, SOC 2, VS-NfD or NATO** attestation. These are not assessed, which is not the same as "not applicable".
- *(from A)* you have **many or flaky workers**. Every node is a Raft voter in v0.1.0, so quorum depends on the workers. A worker that wins an election pauses placement, and clock skew over 30s misjudges liveness.
- *(from A, ADR3)* agents need **network, WASI or enforced memory and token limits**.

### 5.5 Comparison table (neutral; editable data file)

Data lives in `site/src/data/comparison.ts` as rows × columns. Each cell has `{ mark: 'yes'|'no'|'partial'|'na', text, source }` and `source` is required: a repo path for Minga, an upstream doc URL for the others. A caption states "As of 2026-11. Corrections welcome" and links to an issue template. **Before publishing, every Swarm and k3s cell has to be checked against upstream docs**. I wrote them from general knowledge. The Minga cells are cited.

| | Minga v0.1.0 | Docker Swarm mode | k3s |
|---|---|---|---|
| Packaging | ● One `minga` binary + supervised `nats-server` *(R L37)* | ● Built into Docker Engine (dockerd + containerd) | ● Single binary bundling Kubernetes components, containerd, CNI |
| Workload format | ● Compose; unknown keys fail loudly *(R)* | ● Compose via `docker stack deploy`; unsupported keys skipped with a warning | ◐ Kubernetes manifests / Helm; Compose needs conversion |
| API | ◐ Docker Engine API v1.41 subset *(R)* | ● Full Docker Engine API | ● Kubernetes API |
| Desired-state store | ● Embedded Raft (openraft + redb); every process is a voter today *(A)* | ● Embedded Raft among managers | ● SQLite (single server), embedded etcd or external DB |
| Image pull from registry | ○ No; preload with `ctr` *(R)* | ● Yes | ● Yes |
| Container networking across nodes | ○ No; single-node only, mesh planned *(R)* | ● VXLAN overlay | ● Flannel by default |
| Add nodes to a running cluster | ○ No; static membership *(R, A)* | ● Join tokens | ● Node token |
| Healthchecks / rolling update / rollback | ○ Healthcheck carried, not enforced; no rollback *(R)* | ● Yes | ● Probes, rollouts, rollback |
| Transport security | ◐ Raft mTLS (opt-in flags), API bearer token, NATS TLS *(C 0.1.0)* | ● Mutual TLS between nodes by default; overlay encryption opt-in | ● TLS between components by default |
| Air-gapped install | ● Archive with checksum-pinned `nats-server`, `SHA256SUMS`, `MINGA_BASE_URL` *(C)* | ◐ Offline packages + `docker load` | ● Documented air-gap install with image tarballs |
| WASM / agent workloads | ◐ `x-minga-agent` under wasmtime; no host I/O yet *(ADR3)* | ○ Not built in | ○ Not built in |
| Compliance assessments | ○ Not assessed *(R)* | n/a (verify vendor offerings) | n/a (verify vendor offerings) |
| Maturity | ◐ v0.1.0, released 2026-11-01 *(C)* | ● Mature, in Docker Engine since 2016 | ● Mature, certified Kubernetes distribution |
| License | MIT (+ Apache-2.0 `nats-server`) *(R)* | Apache 2.0 (Moby) | Apache 2.0 |

Legend: ● yes, ◐ partial, ○ no. The glyph is always paired with text, never color alone.

### 5.6 FAQ (11 Q&As; `<details>` elements, no JS)

1. **Is Minga production-ready?**
   It's v0.1.0. The single-host path is verified by execution on Linux (containerd 2.2.1) and macOS. The changelog calls it "production-shaped" and lists known limits next to every claim. If you need image pull, multi-node networking or rollback, it's not ready for you yet. *(R L11–27, C 0.1.0)*
2. **Do my Compose files work unchanged?**
   That's the goal, and keys Minga doesn't understand fail loudly with a suggestion instead of being skipped. `command`, `entrypoint`, `labels`, volumes, bind mounts, placement constraints and `deploy.restart_policy` reach the container. `healthcheck` is carried but not enforced. Secrets must be `external` references, and inline `file:` secrets are rejected. Run `minga validate -c compose.yaml` offline first. *(R §Features; RM §Secrets)*
3. **Can I keep using the `docker` CLI and Traefik?**
   Yes, against `minga api`: `docker stack deploy`, `docker service ls`, `docker node ls`, and Traefik v3's swarm provider. With a token set, the CLI sends it via `"HttpHeaders": {"Authorization": "Bearer <token>"}` in `~/.docker/config.json`. *(R §Features; T §7)*
4. **Why doesn't Minga pull images?**
   The pull path (content transfer plus unpack) isn't built yet. It's next on the roadmap. Until then, preload images with `ctr -n minga images pull` or `ctr -n minga images import <image>.tar`. *(RM §Out of scope; R §Roadmap 2)*
5. **Why does one host run two processes?**
   In v0.1.0, `minga api` is its own Raft voter, so the manager and the API peer with each other. In the target design the API folds into the manager. *(R L187; A §Target shape)*
6. **Can I run more than one node?**
   The control plane works across nodes: Raft with mTLS, plus scheduling. Three limits apply. Every node must be listed with `--peer` and started together. Containers on different nodes can't reach each other. Every manager and worker must use one external TLS NATS server. *(R §Deployment topologies 2)*
7. **Does it need Docker, Kubernetes or etcd?**
   No. It needs containerd and root, because the supervisor creates netns, bridges and iptables rules. *(R L37, L170)*
8. **Why is there a Go binary in a Rust project?**
   `nats-server` is Go. ADR 0001 allows Go only as a separate process with no FFI. The manager spawns it as a child, and the release pins its checksum. *(ADR1; C)*
9. **How do I install on an air-gapped site?**
   Copy the release archive and your image tarballs to the site. Run `ctr -n minga images import` on each node and start the roles. `install.sh` verifies `SHA256SUMS` and accepts `MINGA_BASE_URL` for a local mirror. *(R §Deployment topologies 4; C Unreleased)*
10. **I published a port. Why doesn't `curl localhost:<port>` on the same host answer?**
    Host-local port hairpinning isn't in v0.1.0. Test from another machine against the host's IP. *(R L23)* This is my interpretation of "hairpinning" and the maintainer should confirm it. If the interpretation is right, the quickstart's verify step must not use localhost.
11. **Is it secure? Is it compliant?**
    Raft mTLS, an API bearer token, NATS TLS, a built-in CA (`minga tls`) and content-addressed secret staging are implemented. Keep `:2375` and the Raft ports on a private network, because the API token is the only thing between the network and container creation. FIPS, SOC 2, VS-NfD and NATO are not assessed. *(C 0.1.0 Security; T §7 note; R L27)*
12. **What are agent workloads?**
    Compose services with `x-minga-agent` run as WASM modules under wasmtime with fuel and timeout limits. Today they're pure compute: no WASI, network, env or clock, and memory and token limits aren't enforced. Nodes need `--sandbox-runtime wasmtime`. *(R; ADR3 Implementation status)*

### 5.7 Quickstart section (landing, condensed; the full version is `/docs/quickstart/`)

- **Prereqs checklist (static, not interactive):** Linux · root · containerd running · ports 7001/7002/2375/9100 free on loopback.
- Steps 1–6 are code blocks taken verbatim from R L172–205: install, preload, manager in terminal 1, api in terminal 2, deploy, `curl -s 127.0.0.1:9100/readyz`.
- Step 3 adds a minimal compose file:

```yaml
services:
  web:
    image: docker.io/library/nginx:1.27
    ports:
      - "8080:80"
```

- **Expected output:** the four lines from D/TR (`network shop_default: created` / `service shop.web: created` / blank / summary). These need re-capturing from a real run.
- A build-time check asserts that every command string in the landing quickstart appears verbatim in README.md. Drift fails the build.

### 5.8 Features grid (8 cards; each one maps to a real capability and a doc link)

| Brief's card | Reframed title | Body (≤ 2 lines) | Caveat line (muted) | Link |
|---|---|---|---|---|
| Easy setup | **One binary, checksum-verified** | `install.sh` installs `minga` + pinned `nats-server` and verifies `SHA256SUMS`; `--with-systemd` adds the unit. *(C)* | Needs root, containerd, preloaded images | /docs/quickstart/ |
| Production-ready defaults | **Fails loudly by default** | Unknown Compose keys rejected; 409 on unplaceable deploys; `/readyz` 503 with a reason. *(R, C)* | Healthcheck not enforced yet | /architecture/ |
| Observability | **Metrics, health, logs** | `/metrics`, `/healthz`, `/readyz` on every role; JSON logs; `minga logs -f` with separated stdout/stderr. *(R, ADR4)* | Some metrics registered but not updated; no correlation IDs | /adr/0004-observability/ |
| Security | **Transport security you can audit** | Raft mTLS, API bearer token, NATS TLS, `minga tls` CA, content-addressed secrets. *(C 0.1.0)* | Compliance not assessed | /changelog/#010 |
| Performance | **Rust on the hot path** | Per-packet, per-request and per-sandbox work stays in Rust; no FFI. *(ADR1)* | No benchmarks published yet | /adr/0001-language-split/ |
| Extensibility | **Speaks the Swarm API** | Docker Engine API v1.41 subset: `docker` CLI and Traefik v3 work; `x-` extensions pass through. *(R)* | Subset, not full API | /architecture/ |
| DX | **Validate before you deploy** | `minga validate` checks a Compose file offline and prints startup order. *(R L208)* | n/a | /docs/examples/ |
| Cloud-native/edge | **Built for edge and air-gap** | amd64 + arm64 archives and image; no network step on air-gapped install. *(R, C)* | Single-node networking only | /docs/quickstart/ |

### 5.9 Architecture section (landing)

- **Diagram:** a simplified inline SVG of R's process flowchart (clients → api → Raft; manager scheduler → NATS; supervisors → containerd). Below it is a "Full diagrams →" link to `/architecture/`.
- **Components:** six key crates from R §Crate layout: `minga-compose`, `minga-state`, `minga-scheduler`, `minga-api`, `minga-node`, `minga-bus`, then "+5 more →".
- **ADR links:** 0001 language split, 0002 NATS event bus, 0003 agent workloads, 0004 observability.
- **Trade-offs (from A §Known structural limits):** every node is a Raft voter; liveness goes through Raft; there are two dispatch paths; agents have no host capabilities; there's no cross-node networking. Under them: "Plan: workers leave Raft →" (A §Target shape).
- **Non-goals (from CON principles and R):** no Docker daemon, no Kubernetes or kubelet, no external etcd or storage service, no mixed-language FFI, no silently skipped Compose keys. Plus one explicit line: "Not a 5,000-node orchestrator in v0.1.0 (A)".

---

## 6. ASCII wireframes

### 6.1 Landing, desktop (≥960px)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ minga v0.1.0   Docs · Architecture · ADRs · Roadmap · Changelog · Blog  [GH][◐]│ ← Navbar (sticky)
├──────────────────────────────────────────────────────────────────────────────┤
│ v0.1.0 · MIT · Linux amd64/arm64                                             │ ← eyebrow (mono)
│                                         ┌──────────────────────────────────┐ │
│ Run Compose stacks at the edge          │ $ curl -fsSL …/install.sh | sh [⧉]│ │ ← Hero: copy L,
│ from one binary.                        │ $ sudo ctr -n minga images pull … │ │   terminal R
│                                         │ $ minga validate -c compose.yaml  │ │
│ For platform teams running edge,        │ # with manager + api running:     │ │
│ on-prem and air-gapped sites, Minga     │ $ MINGA_API_TOKEN=… minga deploy …│ │
│ runs your Compose files on containerd   │ deployment of stack `shop`:       │ │
│ without a Docker daemon, Kubernetes     │   [ok ] shop.web: created — 1/1   │ │
│ or etcd.                                └──────────────────────────────────┘ │
│ [Get started in 5 minutes →] [View on GitHub]                                │
│ Needs: Linux · root · containerd · images preloaded with ctr                 │
│ ⚠ Verified on one Linux host. No image pull, no cross-node networking yet.   │ ← status line
│   → Known limits                                                             │
│ ┌──────────────────────────────────────────────────────────────────────────┐ │
│ │ compose.yaml ─▶ minga api ─▶ Raft ──▶ scheduler ─▶ JetStream ─▶ supervisor│ │ ← hero diagram
│ └──────────────────────────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## Swarm was built for a datacenter. Your sites aren't one.                  │ ← Problem
│ ┌ Edge sites ────────┐ ┌ Air-gapped ────────┐ ┌ Agent workloads ───┐         │
│ └────────────────────┘ └────────────────────┘ └────────────────────┘         │
│ Your stacks are already Compose. …                                           │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## One binary that reads your compose.yaml                                   │ ← Solution
│ Concept paragraph                       │ ┌ diagram (deploy sequence) ─────┐ │
│ ✓ No Docker daemon, no K8s, no etcd     │ │ 1 deploy → api                 │ │
│ ✓ Docker tooling keeps working          │ │ 2 api → Raft (409 if no node)  │ │
│ ✓ Failures are loud                     │ │ 3 scheduler → JetStream Assign │ │
│ ✓ Air-gap path, no network step         │ │ 4 supervisor → containerd      │ │
│ ✓ Agent workloads (experimental)        │ │ 5 status/logs → JetStream      │ │
│                                         │ └────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## What's in v0.1.0                                                          │ ← Features (4×2)
│ ┌One binary,─┐ ┌Fails loudly┐ ┌Metrics,────┐ ┌Transport───┐                  │
│ │checksum-   │ │by default  │ │health, logs│ │security    │                  │
│ │caveat…  →  │ │caveat…  →  │ │caveat…  →  │ │caveat…  →  │                  │
│ └────────────┘ └────────────┘ └────────────┘ └────────────┘                  │
│ ┌Rust on the─┐ ┌Speaks the──┐ ┌Validate────┐ ┌Built for───┐                  │
│ │hot path    │ │Swarm API   │ │first       │ │edge/air-gap│                  │
│ └────────────┘ └────────────┘ └────────────┘ └────────────┘                  │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## Quickstart: one Linux host            Prereqs: ▢Linux ▢root ▢containerd   │ ← Quickstart
│ 1 Install          [code ⧉]                                                  │
│ 2 Preload images   [code ⧉]                                                  │
│ 3 compose.yaml     [code ⧉]                                                  │
│ 4 Manager (T1)  │ 5 API (T2)     [code ⧉] [code ⧉]   ← side-by-side on lg    │
│ 6 Deploy        [code ⧉]   Expected output [output, no copy]                 │
│ [Full quickstart →]                                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## How it compares            As of 2026-11 · [Suggest a correction]         │ ← Comparison
│ ┌────────────────────┬──────────────┬──────────────┬──────────────┐          │
│ │                    │ Minga 0.1.0  │ Docker Swarm │ k3s          │          │
│ │ Image pull         │ ○ No …       │ ● Yes        │ ● Yes        │          │
│ │ …14 rows           │              │              │              │          │
│ └────────────────────┴──────────────┴──────────────┴──────────────┘          │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## When to use        │ ## When not to use (yet)  #when-not                  │ ← 2 columns
│ ✓ one-host edge…      │ ✗ need image pull                                    │
│ ✓ air-gapped…         │ ✗ cross-node container traffic                       │
│                       │ ✗ … (8 items, warn-bordered)                         │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## Architecture       [diagram]  Components · ADRs · Trade-offs · Non-goals  │ ← Architecture
├──────────────────────────────────────────────────────────────────────────────┤
│ (Social proof: renders nothing unless config has entries)                    │
├──────────────────────────────────────────────────────────────────────────────┤
│ ## Community   Contributing · Good first issues · Discussions* · CoC         │ ← * hidden if unset
├──────────────────────────────────────────────────────────────────────────────┤
│ ## FAQ   ▸ Is Minga production-ready?  ▸ Do my Compose files work…  (12)     │
├──────────────────────────────────────────────────────────────────────────────┤
│ Try it on one host.  [Try it now] [Read the docs] [Star on GitHub] [Community]│ ← Final CTA
├──────────────────────────────────────────────────────────────────────────────┤
│ Project      GitHub         Status                                           │ ← Footer
│ Docs         Repository     v0.1.0 · active development, pre-1.0             │
│ Quickstart   Issues         MIT · © 2026 Minga contributors                  │
│ ADRs …       Releases …     nats-server: Apache 2.0                          │
│ Named after the minga of Chiloé, where a community moves a house together.   │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Landing, mobile (360–639px)

```
┌──────────────────────────────────┐
│ minga v0.1.0   [◐] [Menu ▾]      │ ← <details> nav, no JS
├──────────────────────────────────┤
│ v0.1.0 · MIT · amd64/arm64       │
│ Run Compose stacks               │
│ at the edge from one             │
│ binary.                          │
│ For platform teams running …     │
│ [Get started in 5 minutes →]     │ ← full width, ≥44px
│ [View on GitHub]                 │
│ Needs: Linux · root · containerd │
│ ⚠ One host verified. No image   │
│   pull, no cross-node net.       │
│   → Known limits                 │
│ ┌──────────────────────────────┐ │
│ │ $ curl -fsSL …/install… [⧉]  │ │ ← horizontal scroll,
│ │ $ sudo ctr -n minga …        │ │   tabindex=0
│ └──────────────────────────────┘ │
│ [diagram: vertical stack]        │ ← SVG reflows to vertical
│  compose.yaml                    │
│      ▼                           │
│  minga api ─▶ Raft               │
│      ▼                           │
│  JetStream ─▶ supervisor         │
├──────────────────────────────────┤
│ ## Problem (cards stacked)       │
│ ## Solution (list, then diagram) │
│ ## Features (1 column, 8 cards)  │
│ ## Quickstart (steps stacked)    │
│ ## Compare                       │
│ ┌ scroll ─────────────────────▶┐ │ ← sticky first column,
│ │ Row  │ Minga │ Swarm │ k3s   │ │   region labelled
│ └──────────────────────────────┘ │
│ ## When not to use (yet)         │ ← on mobile, "not" comes
│ ## When to use                   │   BEFORE "to use"
│ ## Architecture                  │
│ ## Community                     │
│ ## FAQ ▸ ▸ ▸                     │
│ [Try it now] [Docs] [Star] [Comm]│ ← 2×2 grid
│ Footer (stacked columns)         │
└──────────────────────────────────┘
```

On mobile, "When not to use" comes before "When to use". On a phone the reader sees only one column at a time, and the limits are the part that decides the evaluation.

### 6.3 Docs page (`/docs/quickstart/`), desktop and mobile

```
Desktop
┌──────────────────────────────────────────────────────────────────────────────┐
│ Navbar                                                                       │
├────────────────┬─────────────────────────────────────────┬───────────────────┤
│ START          │ Docs › Quickstart                        │ ON THIS PAGE      │ ← breadcrumb
│ ▸ Overview     │ # Quick start: one Linux host            │ Prerequisites     │
│ ● Quickstart   │ Source: README.md · [Edit on GitHub]     │ Install           │ ← sticky TOC
│ ▸ Examples     │ ┌ ⓘ Verified on Linux, containerd 2.2.1 ┐│ Run the roles     │   (h2/h3)
│ DEPLOY         │ └────────────────────────────────────────┘│ Deploy            │
│ ▸ Talos  ⚠     │ prose (72ch)                             │ Topologies        │
│ REFERENCE      │ ┌ bash ─────────────────────────── [⧉] ┐│                   │
│ ▸ Architecture │ │ sudo minga manager --node-id 1 …      ││                   │
│ ▸ ADRs         │ └────────────────────────────────────────┘│                   │
│ ▸ Roadmap      │ …                                        │                   │
│ ▸ Changelog    │ ← Overview            Examples →         │                   │
│ CONTRIBUTE     │                                          │                   │
│ ▸ Contributing │                                          │                   │
│ ▸ NATS research│                                          │                   │
├────────────────┴─────────────────────────────────────────┴───────────────────┤
│ Footer                                                                       │
└──────────────────────────────────────────────────────────────────────────────┘
The sidebar is static config (not auto-generated). ● marks the current page, which
also gets aria-current="page". ⚠ is followed by the text "not validated".

Mobile
┌──────────────────────────────────┐
│ minga v0.1.0   [◐] [Menu ▾]      │
├──────────────────────────────────┤
│ [Docs menu ▾]                    │ ← <details>: sidebar, closed by default
│ Docs › Quickstart                │
│ # Quick start: one Linux host    │
│ Source: README.md · Edit         │
│ [On this page ▾]                 │ ← <details>: TOC, closed by default
│ prose …                          │
│ ┌ bash ──────────────── [⧉] ┐   │
│ └───────────────────────────┘   │
│ ← Overview        Examples →     │
└──────────────────────────────────┘
TOC appears only when the page has ≥3 h2s. Between 960 and 1200px the TOC moves into the
same <details> as on mobile and the sidebar stays.
```

### 6.4 ADR page (`/adr/0002-event-bus-nats/`)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ Navbar                                                                       │
├────────────────┬─────────────────────────────────────────┬───────────────────┤
│ ADRs           │ ADRs › 0002                              │ ON THIS PAGE      │
│ 0001 Language  │ # ADR 0002: Event bus — NATS/JetStream…  │ Context           │
│ ● 0002 Bus     │ [● Accepted] 2026-10-26  Input: NATS eval│ Decision          │ ← status pill:
│ 0003 Agents    │ ┌──────────────────────────────────────┐ │ …                 │   icon + text
│ 0004 Observ.   │ │ ⓘ Code deviates from this ADR in 3   │ │ Implementation    │
│                │ │ places. Implementation status ↓      │ │ status ★          │ ← jump link,
│ [All ADRs]     │ └──────────────────────────────────────┘ │                   │   only if the ADR
│                │ (rendered markdown: tables, code)        │                   │   has the section
│                │ ## Implementation status (2026-11-01)    │                   │
│                │ ┃ highlighted: left border accent        │                   │ ← visual emphasis
│                │ ← 0001 Language split   0003 Agents →    │                   │
│                │ Source: docs/adr/0002-…md · Edit         │                   │
└────────────────┴─────────────────────────────────────────┴───────────────────┘
```

- The ADR page lifts the "Implementation status" section up to the top as a jump-link callout, because that section is where reality is written down.
- The deviation count comes from counting `**Deviation` bullets in the section. With zero, the callout reads "Implementation status ↓" with no count.
- `/adr/` index: a table with columns No · Title · Status · Date · "Deviations". Rows sort by number.

### 6.5 404

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ Navbar (absolute /minga/… links)                                             │
├──────────────────────────────────────────────────────────────────────────────┤
│  ┌ terminal ──────────────────────────────────────────────────────────────┐  │
│  │ $ minga get page                                                       │  │
│  │ error: no such page                                                    │  │ ← no JS; generic,
│  │ hint: the docs moved to /minga/docs/ when the site launched            │  │   no echo of URL
│  └────────────────────────────────────────────────────────────────────────┘  │
│  # Page not found                                                            │ ← the real h1
│  Try:  [Quickstart]  [Docs]  [ADRs]  [Changelog]  [Home]                     │
│  Followed a broken link from this site? [Report it on GitHub]                │ ← issues/new?title=Broken+link
└──────────────────────────────────────────────────────────────────────────────┘
```

On a project site, GitHub Pages serves `/minga/404.html` at any depth, so every link on this page must be absolute and include `/minga/`. The page doesn't echo the requested URL, because that would need JS.

---

## 7. Component spec

All components take content through props. Strings come from `site.ts` or the content data files, not from the component.

### `<Navbar>`
- Wordmark `minga` in mono, a version badge (from C) linking to `/changelog/`, nav links from `site.nav`, an icon-plus-label GitHub link, and `<ThemeToggle>`.
- Below `md`, links collapse into `<details class="nav"><summary>Menu</summary>`. No JS. Escape-to-close is an optional progressive enhancement and can be skipped.
- The current section gets `aria-current="page"`. Includes a skip link ("Skip to content") as the first focusable element.

### `<Hero>`
- Props: `eyebrow`, `title`, `subtitle`, `primary`, `secondary`, `prereq`, `status` (text + href), slots `terminal` and `diagram`.
- Two columns at ≥960px, stacked below that. The status line uses the `--warn` glyph plus text.
- If `primary.href` is unset the build fails (it's required). If `secondary` is unset it's hidden.

### `<CodeBlock>`
- Props: `code`, `lang`, `title?`, `copy=true`, `prompt='$ '`, `output?` (rendered under the code, muted, no copy button).
- Shiki at build time, on the always-dark surface. `<pre tabindex="0">` so keyboard users can scroll it.
- Copy writes `code` with `prompt` prefixes stripped. Comment lines are kept, because they match README.
- The copy button is rendered with `hidden` and revealed by script. That way it never appears without JS.

### `<FeatureCard>`
- Props: `title`, `body`, `caveat?`, `href`, `source` (the repo path, shown on hover and in `title`).
- The whole card is one link (`<a>` wrapping block content) with a visible "Read more" text. `caveat` renders as a muted line prefixed "Limit:". `caveat` isn't required in the schema, but six of the eight cards have one.

### `<ComparisonTable>`
- Props: `columns`, `rows` from `src/data/comparison.ts`, `asOf`, `correctionUrl`.
- Marks are glyph plus text. A cell without `source` fails the build.
- Below `md`: scroll region (`role="region" aria-label="Comparison table, scrolls horizontally" tabindex="0"`) with a sticky first column.

### `<ArchitectureDiagram>`
- Props: `variant: 'hero' | 'process' | 'sequence'`, `caption`.
- Inline SVG authored once in `site/src/diagrams/*.svg`, using `currentColor` and token CSS vars on the dark surface.
- `role="img"`, `<title>` and `<desc>`, plus a `<details>` "Text description" listing the same steps.
- The hero variant has a vertical layout under `sm`: two SVGs, toggled with a `<picture>`-free CSS `display` swap.

### `<Testimonial>`
- Props: `quote`, `name`, `role`, `org?`, `sourceUrl` (required: a public post or issue where the quote can be checked).
- Rendered as `<figure><blockquote cite>…</blockquote><figcaption>`.
- `<SocialProof>` wraps the testimonials and adopters arrays. If both are empty and `shareUsageUrl` is unset, it renders **nothing**: no heading, no whitespace. If both are empty but `shareUsageUrl` is set, it renders one muted line: "Running Minga somewhere? Tell us →". No logos, counts or "trusted by".

### `<FAQ>`
- Props: `items: {q, a (markdown), sources[]}`.
- One `<details>` per item, with `<summary>` holding the question and `h3` semantics via `<summary><h3>`. Sources render as small links under each answer.
- No FAQPage JSON-LD: the brief doesn't ask for it, and Google limits that rich result anyway.

### `<CTA>`
- Props: `title`, `actions[] {label, href, variant: 'primary'|'secondary'}`.
- Final CTA: Try it now (`/docs/quickstart/`), Read the docs (`/docs/`), Star on GitHub (repo URL, **no live count**), Join the community (`/community/`, which always exists, so there's never a dead link and no Discord dependency).

### `<Footer>`
- Columns: Project (internal routes) · GitHub (repo, issues, discussions*, releases, contributing, security reporting) · Status (version, `maintenanceStatus` text, license line, nats-server Apache-2.0 note) · the Chiloé line from R L9.
- `*` items are hidden when the config field is unset. Social links (discord, mailingList, twitter, mastodon) work the same way, and the whole social row disappears if every field is unset.

### Extras the routes need

**`<Callout>`**
- Props: `kind: 'info'|'limit'|'unvalidated'`.
- The text always starts with its label: "Not validated:" or "Limit:". The `unvalidated` kind is injected at the top of `/docs/talos/`.

**`<DocsLayout>` / `<AdrLayout>`**
- Wrap the sidebar, breadcrumb, `<article>`, TOC, prev/next and "Source: <path> · Edit on GitHub".
- They also emit BreadcrumbList JSON-LD.

**`<ThemeToggle>`**
- A `<button hidden>` revealed by the inline head script.
- Uses `aria-pressed` against "dark". The label text changes to "Light theme" / "Dark theme".

### `src/config/site.ts` (fields; schema-validated with zod at build)

| Field | Required | Value / default | Unset behavior |
|---|---|---|---|
| `name` | yes | `"Minga"` | build fails |
| `tagline` | yes | "Run Compose stacks at the edge from one binary." | build fails |
| `description` | yes | R L7 sentence | build fails |
| `siteUrl`, `base` | yes | `https://lithqube.github.io`, `/minga/` | build fails |
| `repoUrl` | yes | `https://github.com/lithqube/minga` | build fails |
| `issuesUrl`, `releasesUrl`, `contributingPath` | derived from repoUrl | n/a | n/a |
| `discussionsUrl` | no | `null` until enabled (Open question 4) | link hidden; community falls back to Issues |
| `goodFirstIssueUrl` | no | `repoUrl/issues?q=is:open+label:"good first issue"` | hidden |
| `securityContact` | no | n/a | falls back to the "see CONTRIBUTING → Security issues" link |
| `maintainer` `{name, url}` | yes | user to confirm (Open question 7) | build fails |
| `maintenanceStatus` | yes | "Active development, pre-1.0. Upgrade every node together (see changelog upgrade note)." *(C Upgrade note)* | build fails |
| `license`, `licenseUrl` | yes | `MIT`, `repoUrl/blob/main/LICENSE` | build fails |
| `version` | n/a | **derived from CHANGELOG**, not a config field | n/a |
| `stats` `{stars, forks, contributors}` | no | all `null` | hidden; never shows "0" or a placeholder |
| `analytics` `{provider: null \| 'plausible' \| 'goatcounter', id}` | no | off | no script tag at all |
| `cta` `{primary, secondary, final[]}` | yes | see §5 | primary missing: build fails |
| `nav[]` | yes | Docs, Architecture, ADRs, Roadmap, Changelog, Blog | n/a |
| `social` `{discord, mailingList, twitter, mastodon}` | no | all `null` | hidden |
| `socialProof` `{testimonials: [], adopters: [], shareUsageUrl: null}` | no | empty | section renders nothing |
| `ogImage` | yes | `/minga/og-default.png` (1200×630 placeholder: mono wordmark + tagline on `--bg`) | build fails |

### SEO and metadata

- **Per page:** `<title>`, meta description, canonical, OG and Twitter (`summary_large_image`).
- **JSON-LD:**
  - `SoftwareApplication` on `/`: name, description, `applicationCategory: DeveloperApplication`, `operatingSystem: Linux`, `softwareVersion` (derived), `license`, `url`, `offers {price: 0}`. No `aggregateRating`, ever.
  - `BlogPosting` on posts.
  - `BreadcrumbList` on docs, ADR and blog pages.
- `@astrojs/sitemap`. `robots.txt` with `Sitemap: https://lithqube.github.io/minga/sitemap-index.xml`. Favicon is an SVG mono "m", with a 32px PNG fallback.

---

## 8. Required states

**Theme**
- [ ] S1 **No JS:** the page follows `prefers-color-scheme`, dark when there's no preference. The toggle stays `hidden`, so there's never a dead control.
- [ ] S2 **JS, no stored choice:** same as S1. Clicking the toggle stores `localStorage.theme` and sets `<html data-theme>`.
- [ ] S3 **JS, stored choice:** an inline `<head>` script sets `data-theme` before first paint, so there's no flash. If `localStorage` throws (private mode), fall back to S1 silently.
- [ ] S4 Code blocks and diagrams look identical in both themes (always-dark surface).

**Copy button**
- [ ] S5 **Idle:** label "Copy" and `aria-label="Copy command: <title or first line>"`.
- [ ] S6 **Success:** label becomes "Copied" for 2s and the `aria-live="polite"` region announces "Copied to clipboard". Focus stays on the button.
- [ ] S7 **Failure** (no `navigator.clipboard`, permission denied, insecure context): label "Copy failed". The script selects the code text so ⌘/Ctrl-C works, and live region announces "Copy failed. The command is selected; press Ctrl+C."
- [ ] S8 **No JS:** no button. The code stays selectable, and nothing in the copy says "click copy".

**Content pipeline**
- [ ] S9 **Mermaid render fails:** show the source code block plus "View rendered on GitHub" (blob link with anchor). Log a build warning, don't fail.
- [ ] S10 **README section or limits list not found:** the build fails with the missing heading in the error message.
- [ ] S11 Rewritten link with no route: GitHub blob URL. Missing anchor: build warning.
- [ ] S12 **ADR status unparseable:** pill reads "Status: unknown" in muted color plus a build warning. **No Implementation status section:** no jump callout.
- [ ] S13 Changelog `[Unreleased]` renders with an "Unreleased (not in a tagged release)" pill. Its anchor is `#unreleased`.

**Config-driven**
- [ ] S14 **Social proof empty:** nothing renders. `shareUsageUrl` set: a single invite line.
- [ ] S15 **Testimonial missing `sourceUrl`:** build fails.
- [ ] S16 `discussionsUrl` unset: Discussions links are hidden everywhere, and the community page shows "Questions: open an issue".
- [ ] S17 `stats.*` unset: no counts anywhere, not even "0".
- [ ] S18 Social fields all unset: the footer social row is removed.
- [ ] S19 Required field missing: build fails with the zod path.
- [ ] S20 `analytics.provider` null: no third-party request at all. Check with a network audit in CI.

**Pages**
- [ ] S21 **Blog with zero posts:** `/blog/` shows "No posts yet. Release notes live in the changelog →". Not a 404. The RSS feed is skipped.
- [ ] S22 **404:** as specified in §6.5 (absolute links, no URL echo, report link).
- [ ] S23 **No JS overall:** nav, FAQ and mobile TOC (`<details>`) work, and so do diagrams (SVG) and the comparison scroll. Only theme override and copy are lost.
- [ ] S24 Good-first-issue search returns zero results. That's an honest empty state on GitHub, but see Open question 5.
- [ ] S25 `prefers-reduced-motion`: there is no motion anyway. The only transition is the copy label, and it's instant.
- [ ] S26 Print: hide nav and copy buttons, expand `<details>`, show link URLs after anchors in prose. Cheap CSS, and ops teams print runbooks.

---

## 9. Accessibility (WCAG 2.2 AA)

- **Landmarks and headings:** `header`/`nav`/`main`/`footer` and a skip link. One `<h1>` per page. Markdown `# Title` *is* the h1 (§3.2-2), so no level is skipped. The landing section h2s are in the order of §6.1.
- **Focus:** a 2px solid `--accent` outline with 2px offset. It measures 10.1:1 dark and 5.5:1 light, against a 3:1 requirement. `scroll-padding-top` keeps the sticky header from covering focused elements (2.4.11).
- **Target size:** buttons and the theme toggle are ≥44×44px, and inline links meet 24×24 through padding (2.5.8). The copy button is 32px tall with a 44px hit area via padding.
- **Color never stands alone:** comparison marks pair a glyph with text, status pills pair an icon with text, and callouts start with "Limit:" / "Not validated:". Links are underlined.
- **Code:** `<pre tabindex="0">` with `aria-label="<lang> code"`. Prompts (`$ `) are in a `user-select:none` span so selection matches what the copy button writes.
- **Tables:** `<caption>`, `<th scope="col">`, and row headers as `<th scope="row">`. The scroll region is labelled and focusable.
- **Diagrams:** `role="img"` plus `<title>` and `<desc>`. The Mermaid SVGs also get a `<details>` "Diagram source", since Mermaid text is a reasonable text alternative.
- **Disclosure widgets:** native `<details>`/`<summary>` for mobile nav, the docs menu, the TOC and FAQ. Summaries carry a visible label ("Menu", not just ☰).
- **Live regions:** one shared `aria-live="polite"` region for copy results.
- **Theme toggle:** `aria-pressed` plus a text label. The dark/light decision is never conveyed by icon alone.
- **External links:** the URL makes them obvious enough, so no `target=_blank`. Links stay in the same tab, which needs no warning.
- **Language:** `<html lang="en">`. The Spanish word "minga" in the footer gets `lang="es"`.
- **Contrast:** every token pair in §4.1 is ≥4.5:1 for text. Borders are decorative only, and inputs don't exist. The Shiki theme must be audited and its comment color overridden.

---

## 10. Findings in the repo (relevant to the site, not fixed here)

1. **The README contradicts itself on liveness.** R L37 says JetStream carries "status, logs and liveness". R L94, A §2 and ADR2 "Deviation: liveness" say leases go through Raft. The site copy avoids the claim, but R L37 should be fixed.
2. **ADR header formats differ.** ADR4 uses `**Status:**` and the others use `- Status:` (§3.2-5).
3. **ADR3 is stale in two places.** ADR3 §6 documents `minga node` and `--module-dir` with default `./modules`. R says the default is `/var/lib/minga/modules`, and C says `minga node` doesn't exist. ADRs are historical, but readers will copy those flags. I suggest an "Implementation status" note or a callout on that page.
4. **The Talos guide still reads like a sales page in places.** T keeps lines such as "Talos is an ideal host", "Recommended for cloud deployments" and a cost table, all under a "not validated" banner. The site injects its own banner. Consider trimming the source.
5. **No `CODE_OF_CONDUCT.md` or `SECURITY.md`.** Both live inside CONTRIBUTING. GitHub's community profile and Security tab look for those files, so the site links to `CONTRIBUTING#code-of-conduct` and `#security-issues`.
6. **The edge-ai fixture can't be deployed.** FX `edge-ai.yaml` uses a `file:` secret, which deploy rejects. It's only usable as a `validate` example.
7. **The changelog links a planning file.** C cites `.planning/STATE.md`. It becomes a GitHub blob link, so check that `.planning/` is meant to be public.

---

## 11. Open questions

1. **Primary CTA label.** (a) Keep "Get started in 5 minutes" (brief). (b) "Run it on one Linux host". (c) "Get started" + prereq line. **I recommend (b):** it's measurable and the reader self-selects. (a) is an unmeasured claim and contradicts the honesty constraint.
2. **Comparison columns.** (a) Swarm + k3s. (b) Add Nomad. Nomad is a fair functional peer, but it has been under BSL since 2023, so the license row matters. (c) Add Kubernetes (kubeadm). **I recommend (a)**, plus a "why not Nomad" FAQ entry if users ask. A maintainer has to verify every non-Minga cell.
3. **Theme default.** (a) Always dark unless the user toggles. (b) Dark unless the OS asks for light (designed above). **I recommend (b):** `prefers-color-scheme: light` is a user accessibility signal, and it's free without JS.
4. **GitHub Discussions.** Are they enabled? If not, should they be? Community and "Join the community" fall back to Issues while `discussionsUrl` is null.
5. **"good first issue" label.** Does it exist, and are there issues with it? An empty search is honest, but a launch with 2–3 labelled issues converts contributors much better.
6. **Traefik example.** Is there a real compose or test for the Gate 4 Traefik route that `/docs/examples/` can import? I won't write one that hasn't been run.
7. **Maintainer and maintenance status.** What name, URL and status wording? RM noted "25 commits, all by one author" as of 2026-10-27. Should the footer say "single maintainer"? It sets expectations for production evaluators.
8. **Custom domain.** Stay on `lithqube.github.io/minga/`, or use a domain like `minga.dev`? This changes `site`/`base`, the canonicals and the OG URLs. Deciding before launch avoids redirect debt.
9. **Normalize ADR 0004's header** in the source (preferred, one-line edit), or teach the parser both formats?
10. **Mermaid in CI.** Is the Pages workflow allowed to install Playwright Chromium (about 150 MB, roughly 1 min of build time) for build-time SVG? The alternative is to pre-render SVGs and commit them, which creates a second source.
11. **Search.** None in v1, because Pagefind adds JS. Is that acceptable for launch?
12. **Expected-output capture.** Who runs the quickstart on a clean Ubuntu + containerd box to capture the real `minga deploy` output, and to confirm the hairpin FAQ (#10) before launch?

---

I did not run Minga, so the expected deploy output (§5.1, §5.7) comes from `/Users/admin/dev/minga/crates/minga-cli/src/deploy.rs` and `/Users/admin/dev/minga/crates/minga-scheduler/src/translate.rs`, not from a real run, and it has to be re-captured. I also did not read `docs/research/nats-evaluation.md`, the `wordpress.yaml` fixture or `scripts/minga.service`.

Risks: every Swarm and k3s cell in the comparison is from general knowledge and needs checking against upstream docs before publishing. FAQ #10 depends on my reading of "host-local port hairpinning"; if that reading is right, the quickstart's verify step must not tell people to curl localhost.
---

## 12. Addendum (2026-11-02): Dev-Ferru alignment, logo, article fit

Source: the 11 posts at https://devferru.substack.com/archive (read 2026-11-02). The 8 tech posts are part of the *Cloud-Native Chronicles* series. Three posts are about workplace culture and are out of scope here: "The Playbook", "The Execution Pattern" and "The Leadership Crisis".

### 12.1 Visual language of the Dev-Ferru graphics

- **Covers** are AI-rendered neon-noir night scenes in Berlin, built around the Fernsehturm: rain-wet streets with reflections and fog, and glass HUD panels with thin glowing borders. Cyan and teal light carries the "trusted" flow. Magenta marks the threat (CVE cubes). Green marks verified things ("SIGNATURE" stamps).
- **In-body graphics** are plain tables. Their only job is the data.
- **Implication for Minga:** keep the industrial and terminal structure from §2. Change the palette to the neon set below, and use the glow only on focus rings, the hero diagram's flow lines and the primary button hover. Body copy and tables stay flat, like the article tables. The cover style becomes the art for the OG image, blog covers and hero illustration: a Chiloé night scene in place of Berlin (§12.3).

### 12.2 Color tokens (replace §4.1). Every pair verified.

| Token | Dark (default) | Contrast | Light | Contrast |
|---|---|---|---|---|
| `--bg` | `#0b0f1a` | n/a | `#f6f7fb` | n/a |
| `--surface` (code, diagrams, both themes) | `#121829` | n/a | `#121829` | n/a |
| `--text` | `#e5e9f0` | 15.7:1 | `#0b0f1a` | ~18:1 |
| `--muted` | `#94a3b8` | 7.5:1 (6.9 on surface) | `#475569` | 7.1:1 |
| `--accent` (links, primary, focus) — cyan | `#22d3ee` | 10.6:1 (9.8 on surface); button text `#0b0f1a` 10.6:1 | `#0e7490` | 5.0:1; white text on it 5.4:1 |
| `--accent-2` (highlights, the "intent" flow) — magenta | `#e879f9` | 7.8:1 | `#a21caf` | 5.9:1 |
| `--ok` (verified, `[ok ]`, "Yes") — green | `#4ade80` | 11.0:1 | `#15803d` | 4.7:1 |
| `--warn` (limits, "No") — rose | `#fb7185` | 7.1:1 | `#be123c` | 5.9:1 |
| `--border` (decorative only) | `#1f2a44` | 1.3:1 | `#d4d8e3` | n/a |

- Magenta never signals danger on its own. Limits use `--warn` plus a glyph, so the article convention (magenta = threat) doesn't fight the site's (magenta = second accent). The glow is a `box-shadow` of `--accent` at 40% alpha and is disabled under `prefers-reduced-motion` / `prefers-contrast: more`.

### 12.3 Logo and the Docker whale (legal check)

**Request:** the Docker whale carrying a tiny house in place of its containers.

**Verdict: don't.** Docker's Trademark Guidelines (https://www.docker.com/legal/trademark-guidelines/, read 2026-11-02) say:
- The marks include the "Moby Dock" whale logo and "the stylist representations of a container, octopus, and other entities". DOCKER SWARM is listed as a mark too.
- Logos and design elements "may not be modified, recombined or displayed in modified scenes, images or other artwork". They also say "Do not create imitations, facsimiles or reproductions of these design elements on your own."
- Preparing "derivative works that imitate these design elements or present them in new scenes" is "prohibited without prior approval from Docker".
- The marks "may not be used in any variations or as a design feature on your product … web page or promotional materials without obtaining prior written approval".

A whale with a house where the containers go is a textbook derivative of Moby Dock, and the guidelines bar it specifically. A "different" whale is risky too. A product that replaces Docker Swarm, with a whale carrying cargo, invites a likelihood-of-confusion claim, and that's the scenario trademark law cares about. This isn't legal advice, but the written policy leaves no room. Only written permission from Docker would change it.

**What is allowed:** word marks in a factual context ("Swarm-compatible Docker Engine API", "works with the `docker` CLI"), plus the attribution legend in the footer: *"Docker and the Docker logo are trademarks or registered trademarks of Docker, Inc. in the United States and/or other countries."* Don't call Minga "Docker Swarm-anything" in its name, domain or logo.

**Alternative that keeps the idea and is more Minga:** the *minga de tiradura* itself. On Chiloé, a community moves a shingled wooden house over land with oxen, and across water floated on barrels and towed by boats. Proposed mark: **a small Chilote house (gable roof, tejuela shingles) riding on a raft on three wave lines.** The house is the workload, the raft is the runtime, and the waves are the edge network. The "moved from the Docker sea" story stays readable without borrowing a whale.
- The mark is a flat SVG: house outline in `--accent`, waves in `--accent-2`, and it works as a 16px favicon (house plus one wave line).
- The hero and OG art is a neon-noir Chiloé night: palafitos on stilts, a house on a raft crossing a channel, cyan tow lines. It's the Dev-Ferru cover style relocated from Berlin to Chiloé. It ships as AVIF/WebP, ≤ 80 KB, with explicit `width` and `height` and `fetchpriority="high"` on the landing page only.

### 12.4 Writing on the site

- `site.ts` gets `newsletter: { name: 'Dev-Ferru', url: 'https://devferru.substack.com', series: 'Cloud-Native Chronicles' }`. If unset, the link is hidden.
- `/blog/` lists Minga posts. Below them, a "Related writing by the maintainer" list links out (no copies, canonical stays on Substack). It shows the 8 tech posts with date and one line each. The three non-tech posts are not listed.
- Landing §Architecture "deeper technical writing" links: "It Works Is Not an Architecture" (evidence over configuration, which matches Minga's gate method), "Your Agent Will Confidently Hand-Roll a DLQ" (JetStream failure paths) and "Production Has No Keys to Give" (agents hold nothing).
- The voice matches the articles: a concrete failure scenario first, then a tradeoffs section, then "Key takeaways". The first Minga blog post (§3.3) should follow that shape.

### 12.5 Does Minga follow the articles' recommendations? (checked against code 2026-11-02)

| Article recommendation | Minga today | Evidence | Fit |
|---|---|---|---|
| NATS with mTLS, no sidecars (Zero trust; Securing microservices) | Yes. NATS TLS for every node, Raft mTLS, built-in CA (`minga tls`) | CHANGELOG 0.1.0, `minga-bus/src/tls.rs` | ● |
| Per-service NKey/JWT identity with **subject-level permissions** (Zero trust; Insider threat) | No. Nodes connect over TLS with no per-node account or subject ACL, so any node can publish an Assign for any other | no `nkey`/`creds` in `crates/` | ○ gap |
| Cert rotation **without restart** ("where zero trust dies") | Not found. No reload path in the TLS code | no reload/rotate in TLS code | ○ gap |
| Secrets out of git, short-lived, never baked into images (Securing microservices) | Partly. External refs only, content-addressed staging, rotation never mutates a running task. No dynamic or short-lived backend (Vault/Infisical) | `minga-node/src/secrets.rs`, R §Features | ◐ |
| JetStream failure path: `max_deliver` + **advisory capture** to a stream, `nak(delay)` vs `term` (DLQ article) | No. Durable explicit-ack pull consumers, but no `max_deliver`, so a poison message redelivers forever. Acks ignore errors (`let _ = m.ack()`), and nothing captures advisories | `minga-bus/src/nats.rs` L326–394, L643–661 | ○ gap. This exact pattern is the maintainer's own published fix |
| Agents **hold nothing**: the broker injects credentials in transit (No keys; Infisical Agent Vault) | Halfway. The LLM egress proxy enforces budgets, caches and redacts, and agents have no host I/O. But the proxy forwards **no** auth header upstream (none injected either), and workload identity is the **self-asserted** `X-Minga-Workload` header | `minga-egress/src/proxy.rs` L6, L255–262 | ◐ The best story fit on the site once the proxy injects the provider key and binds identity to the sandbox |
| Agents as scoped identities on the fabric (Insider threat) | Not yet. `x-minga-agent` has no bus identity | ADR3 | ○ |
| Signed artifacts / provenance / **admission gate**; air-gap needs self-hosted signing (No keys) | Partly. `SHA256SUMS` on releases, checksum-pinned `nats-server`. Images are preloaded by tag with no digest or signature check before a task starts | CHANGELOG, `install.sh` | ◐ A digest-pin plus cosign-verify admission step fits air-gap |
| Pull-based reconcile; deploy authority isn't "holding a token" (No keys) | The scheduler reconciles desired state, but `minga deploy` is push with one bearer token that is "the only thing between the network and container creation" | ADR4, T §7 | ◐ |
| Metrics + logs + **traces**, trace context in NATS headers (Observability) | Metrics, health, JSON logs, `minga logs -f`. No OTel and no `traceparent` over NATS | no `otel`/`traceparent` in `crates/` | ◐ |
| Evidence, not configuration; architecture tests in CI ("It works") | Yes. Every v0.1.0 gate had an executable exit criterion, and each ADR ends with *Implementation status* | RM, docs/README.md | ● |

**On the site:** the ● rows can be claimed. The ◐ and ○ rows go to *When not to use* and the roadmap, never the feature grid. The rows marked "gap" are Minga engineering work, not site work. They're listed here so the copy doesn't over-claim, and as candidates for the roadmap page.
