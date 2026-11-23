# QA Report — Issue #164

**Branch:** issue-164-nats-pin
**Head commit:** 2b42386fa1e8f7d517bbf575efe988cb9940dd7f
**Test date:** 2026-11-23

## Test Plan

This is a CI workflow refactor with no UI or runtime behavior changes. All verification is at the YAML structure and checksum verification level.

### AC1: Single occurrence of the SHA-256 in ci.yml

**Test:** Count occurrences of the nats-server v2.15.3 linux-amd64 SHA-256 (32e18c1e...) in `.github/workflows/ci.yml`

**Command:**
```bash
grep -c "32e18c1e" .github/workflows/ci.yml
```

**Result:** ✅ PASS — returns `1`

**Evidence:** The hash appears exactly once at line 24 in the YAML anchor `&current-nats-pin`, which is then referenced via `*current-nats` and `*current-nats-pin` in the quality job (line 20), nats matrix job (line 66), and nats-upgrade matrix job (line 111).

---

### AC2: Quality job installs the matrix-pinned version with checksum verification

**Test:** Verify the quality job:
1. References the same `&current-nats` anchor that the matrix job uses
2. Uses `sha256sum -c` to verify the archive before extraction
3. Would install the same nats-server version as the matrix job

**Manual inspection:**
- Lines 18-24: The quality job's strategy.matrix.include uses the anchor `&current-nats`, which is itself a list containing the `&current-nats-pin` anchor (nats-version: v2.15.3, nats_sha256: 32e18c1e...)
- Line 43: `echo "${{ matrix.nats_sha256 }}  $archive" | sha256sum -c -` verifies the checksum before extraction
- Line 66: The nats matrix job includes `*current-nats-pin`, the same pin

**Result:** ✅ PASS — The quality job and nats matrix job both reference the same YAML anchor, ensuring they use identical version and checksum values. The quality job runs `sha256sum -c` before extraction.

---

### AC3: actionlint reports no new findings

**Test:** Run actionlint on the modified ci.yml and compare with main

**Command:**
```bash
actionlint .github/workflows/ci.yml
```

**Result on branch:** 2 shellcheck warnings at line 172 (SC2094)

**Command on main:**
```bash
cd /Users/admin/dev/minga && actionlint .github/workflows/ci.yml
```

**Result on main:** 2 shellcheck warnings at line 162 (SC2094, same issues, different line due to the refactor)

**Result:** ✅ PASS — The actionlint findings on the branch (line 172) are the same pre-existing shellcheck warnings that exist on main (line 162). No new findings introduced.

---

## Upstream checksum verification

**Test:** Verify that the pinned SHA-256 matches the upstream nats-server v2.15.3 release

**Command:**
```bash
curl -fsSL https://github.com/nats-io/nats-server/releases/download/v2.15.3/SHA256SUMS | grep linux-amd64
```

**Result:**
```
32e18c1e78d7ef4f86e483070eef3abb2647c3d4d67e29668077c7ad81da0797  nats-server-v2.15.3-linux-amd64.tar.gz
```

**Verification:** ✅ PASS — The pinned hash in the workflow exactly matches the upstream SHA256SUMS.

---

## Cross-file consistency check

**Files checked:**
- `.github/workflows/ci.yml` — 1 occurrence (line 24), referenced via anchors in quality, nats, and nats-upgrade jobs
- `.github/workflows/release.yml` — 1 occurrence (line 41) for x86_64-unknown-linux-gnu

**Result:** ✅ All workflow files use the correct upstream checksum.

---

## Summary

All 3 acceptance criteria pass:

1. ✅ grep -c returns 1 in ci.yml
2. ✅ Quality job uses the matrix-pinned version with sha256sum -c verification
3. ✅ actionlint reports no new findings (pre-existing warnings unchanged)

The refactor successfully consolidates the nats-server SHA-256 to a single definition per workflow file using YAML anchors, eliminating duplication and future inconsistency risk.
