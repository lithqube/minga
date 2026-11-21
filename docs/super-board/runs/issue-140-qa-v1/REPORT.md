# QA report — issue #140 (ci: nats-server download lacks a checksum)

- Branch: `issue-140-ci-nats-server-checksum` · head `b7c2ab520bbc35961170125fee46ef9d7fb7eada` (PR #156)
- Date: 2026-11-21 · lane: tester (super-qa, issue-scoped) · v1

## Red story (pre-change)

`git diff origin/main...HEAD` shows both pre-change jobs piped the download straight into tar with no verification:

```sh
curl -fsSL "…/v2.15.3/nats-server-v2.15.3-linux-amd64.tar.gz" \
  | sudo tar -xz -C /usr/local/bin --strip-components=1 --wildcards '*/nats-server'
```

and the release build job restored `Swatinem/rust-cache` before `cargo build --release`. Red state confirmed from the diff.

## AC 1 — every nats-server download in ci.yml is checked with `sha256sum -c` before extraction: PASS

- Downloads in ci.yml: `ci.yml:34` (quality job, v2.15.3), `ci.yml:76` (nats matrix job, both versions), `ci.yml:119` (upgrade job helper, already verified before this PR).
- Order in each: `curl -o` → `sha256sum -c` → `tar -xzf` (`ci.yml:34-36`, `ci.yml:76-78`, `ci.yml:119-122`).
- Pinned hashes verified against live upstream SHA256SUMS fetched fresh on 2026-11-21 (see `hash-verification.txt`, `SHA256SUMS-v2.14.10`, `SHA256SUMS-v2.15.3`):
  - v2.14.10 linux-amd64: `464f118a…e9b99` — matches `matrix.nats_sha256` pin.
  - v2.15.3 linux-amd64: `32e18c1e…0797` — matches quality-job pin.
- Positive control: both real tarballs pass `sha256sum -c` with the workflow's exact command shape.
- Negative control: one byte flipped at offset 12345 → `sha256sum -c` exits 1 (`FAILED`). See `checksum-controls.txt`.

## AC 2 — the release build job does not restore rust-cache: PASS

- `grep -n rust-cache .github/workflows/release.yml` → no match (the diff removes the only `Swatinem/rust-cache` step from the build job).
- rust-cache remains in the five ci.yml jobs, which is out of scope for this AC and unchanged by the PR.

## Workflow validity

- `actionlint 1.7.12`: `release.yml` clean; `ci.yml` has 2 pre-existing SC2094 info notes in the private-registry heredoc, identical on `origin/main` — not introduced by this PR. See `actionlint.txt`.

## Cargo gate (board verify_commands)

`cargo fmt --all --check` ✅ · `cargo clippy --all-targets -- -D warnings` ✅ · `cargo test --workspace` ✅ exit 0, 63 suites ok — see `cargo-test.txt` (includes a note on one transient shared-target-dir race, unrelated to the PR).

## Test gaps (not written)

- Low: no automated test asserts the pins stay in sync with upstream; that is inherent to pinning and verified manually here.
