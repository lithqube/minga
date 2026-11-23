# ADR 0009: Version the Raft apply rules

**Status:** Accepted — the state mechanism is implemented; the operator command is not (see [Implementation status](#implementation-status-2026-11-23))  
**Date:** 2026-11-23  
**Deciders:** Ferru

## Context

`apply_command` in `crates/minga-state/src/state_machine.rs` applies each committed log entry to a replica's redb state and returns `Ok`, `NotFound`, `Conflict` or `Error`. Every manager must reach the same result for the same entry at the same log index, or two managers hold different desired state for one history.

Apply is not only a mutation. It also refuses entries: a second holder of a WireGuard key, a subnet inside the node mesh, a tenant-crossing node move, an invalid subnet syntax. Issues #86, #93, #95, #99, #114 and #124 (PR #149) each added a refusal like this to apply, with no version gate. A refusal makes an entry that an older build applied as `Ok` apply as an error on a newer build, and the same holds in the other direction for a rule that is loosened.

Two situations apply the same entry under different builds:

1. **A replica rebuilt from the log.** A restart in place does not replay: `last_applied` persists, and the committed tail after it is applied once, by the build that is running. A new or replaced manager does replay. Minga leaves openraft's snapshot policy at its default (a snapshot every 5000 entries, 1000 entries kept), so a small cluster, which is most sites, never snapshots, and a manager added with `POST /minga/managers` is caught up from log index 0 under whatever rules its build holds. A snapshot only moves the problem: the entries after it still replay.
2. **Managers on different builds.** Every manager applies each live entry. During a rolling upgrade an old and a new build apply the same entry under different rules. The wire frame already refuses peers of a different `FORMAT_VERSION` (`codec.rs`), so this exposure exists only for builds that share a format, such as a 1.3.x line without a migration.

Apply must therefore be a function of the replicated state and the entry alone, as it already is for the overlay supernet (the `upsert_network_apply_uses_only_replica_independent_rules` test): no flag, clock or build constant may change its result. This ADR adds the missing input, the rule version, as replicated state.

## Decision

### 1. The rule level is replicated state

The state holds one integer, the **apply-rule level**, under the redb metadata key `apply_rules`. An absent key is level 1. Every build has a constant `APPLY_RULES_VERSION` (`codec.rs`): the highest level it implements. Level 1 is every refusal `apply_command` made through v1.3.0-rc.1, so no existing refusal is gated.

A new log entry, `StateCommand::SetApplyRules { level }`, raises the level. It takes effect at its own log index, for every replica, whether the replica applies it live or replays it later. Because the level is state and the raise is an entry, an entry committed before the raise is applied under the old rules by every replica, always, and an entry committed after it under the new rules.

### 2. A rule change is gated on a level

A change that makes apply return a different result for an entry that an earlier build applied, in either direction, must:

1. take the next level `N` and set `APPLY_RULES_VERSION` to `N`;
2. run only when `rule_in_force(tx, N)` is true, and otherwise return what the earlier build returned;
3. ship with a test that replays one entry before and after the raise on two replicas and expects the old result before it.

A check that does not need to be in apply stays in the API, as today. Apply refusals exist for races the API cannot see, such as two creates that both pass an API check.

### 3. The raise is the operator's statement that every manager is upgraded

Until the level is raised, a build that implements `N` behaves as level `N-1` for every entry, so managers on mixed builds agree. The operator's step is:

1. Upgrade every manager to a build that implements `N`.
2. Confirm that none is left behind.
3. Raise the level to `N` once, on any manager.

The leader never raises it by itself. A raise during a rolling upgrade would stop the managers that are still old.

### 4. A build that implements fewer levels stops

The level only goes up: a raise below the current level answers `Conflict`, and a repeat answers `Ok`. A replica applying `SetApplyRules` with a level above its own `APPLY_RULES_VERSION`, installing a snapshot whose level is above it, or opening a data directory whose level is above it returns a storage error. Raft halts on it and the manager reports the reason, the same shape as a store of a newer format. It does not skip the entry or apply the rest under older rules.

A build from before this ADR does not know the key. It cannot decode the new command, which fails closed at the first entry that carries it, but it would ignore the key in a snapshot. Downgrading below the build that introduced this ADR after a raise is unsupported.

### 5. The state carries the level

`StateSnapshot` (the internal Raft snapshot) carries `apply_rules`, with a serde default of 1, so a snapshot written before the field decodes as level 1 and `last_applied` keeps its meaning. A snapshot install sets the level to the snapshot's, together with `last_applied`. The portable `minga state export` does not carry it: an import is written through Raft under the importing cluster's level.

### 6. No `FORMAT_VERSION` bump

The `codec.rs` rule asks for a bump and a migration whenever a persisted shape changes. This change adds one metadata key, one snapshot field with a default, and one command that no build writes until an operator raises the level. A bump makes every node change in one window, the cost this ADR exists to avoid for later releases, and no stored record changes. The key is read with a default, so an older store opens unchanged.

## Rejected options

| Option | Why not |
| --- | --- |
| **Stamp each entry with the version that proposed it, and apply it under that version's rules** | It changes the encoding of every command, so a build from before the change cannot decode any entry and a rolling upgrade fails at the first write. It does not stop a live mixed cluster diverging either: a new leader stamps level `N`, and an old follower, which has no code for `N`, applies the entry under its own rules. It also needs the old rule code kept for every version. A level in state keeps one gate per change, ordered by log position. |
| **A snapshot barrier before an upgrade that adds refusals, documented** | A barrier cannot be checked by the code, so it lives in an operator's memory. A small cluster never snapshots, and forcing one leaves 1000 entries that replay under the new rules. It does nothing for the rolling window, where old and new builds apply the same live entry. It is still a sound practice for sizing a catch-up, but it does not make apply deterministic. |
| **Let the leader raise the level when it starts on a newer build** | A leader on the new build with a follower on the old one would stall the follower in the middle of the rolling upgrade. The raise is the operator's statement that every manager is upgraded. |
| **Bump `FORMAT_VERSION` for each new refusal** | It forces a full stop for a change that touches no stored record. The bump stays for shape changes. |
| **Never add an apply refusal** | The races that apply refusals close are real. A refusal in the API alone lets two concurrent creates both pass. |

## Consequences

- Replaying a log, or applying it on managers of different builds that share a format, gives the same state for every replica while the level is the same on each. This is shown by a test of the mechanism, not by a live rolling upgrade.
- A new refusal costs one level, one gate and one replay test. The level goes into the release notes with the operator's step.
- A rule that is gated keeps its old code path for as long as any log entry written before its raise can still be replayed, which means as long as the log is retained. A later ADR may retire a path after a snapshot proves no older entry remains.
- Until the follow-up lands, the raise is a library call (`StateStore::set_apply_rules`) with no API route, command, or preflight check. No release gates a rule yet, so none needs it.
- The level is cluster-wide control-plane state. It is not a tenant object, and no tenant identity can read or write it.
- Not covered: entries committed under 1.2.0 are replayed under the level-1 rules of 1.3.0. The 1.2.0 upgrade is a coordinated window, and the release notes keep export and import as the default for a cluster with more than one node. This ADR does not version 1.2.0's rules.

## Tenancy checklist (ADR 0008)

1. **Owner.** No tenant. The level is a property of the replicated state machine, stored under `sm_meta`, not in a tenant table, so it carries no tenant and needs no tenant in a key.
2. **Access.** Only the library call, which no API route reaches yet. The follow-up exposes it to a cluster operator only (`O=*` or the admin token). A tenant identity never reads or writes it.
3. **Bus.** Not applicable: it uses no subject or key, and it is never published to JetStream.
4. **Node.** Managers only. Workers do not open a state store.
5. **Shared plane.** One monotonic integer and at most one log entry per raise, so a tenant has nothing to grow. No quota applies because no tenant writes it.
6. **Audit.** Not applicable until the follow-up adds the route, which must log the caller on each raise.
7. **Test.** Not applicable as a two-tenant test, because no tenant-reachable path exists. The replay, monotonicity, snapshot and open tests in `state_machine.rs` cover the mechanism.

## Implementation status (2026-11-23)

Implemented in `minga-state`:

- `APPLY_RULES_VERSION` and `APPLY_RULES_KEY` in `codec.rs`; `StateCommand::SetApplyRules`; `rule_in_force` for a gated arm; `StateStore::apply_rules` and `StateStore::set_apply_rules`.
- The level in the Raft snapshot with a default of 1; refusal on snapshot install and on open when the level is above the build's.
- Tests: an entry applied under level 1, then the raise, then the same entry, on a live replica and on a rebuilt one, give the same results, and the replay test fails if `rule_in_force` ignores the level; the level never goes down or above the build's; a snapshot or data directory from a newer build is refused; a snapshot from before the field is level 1.

Not implemented:

- No apply rule is gated yet, because level 1 is everything through v1.3.0-rc.1. The replay test uses a stand-in level-2 rule, not a real one.
- The operator surface: an API route, `minga-ctl upgrade apply-rules`, the level in `minga-ctl info`, and a preflight check that every manager implements the target level. It is tracked in [#188](https://github.com/lithqube/minga/issues/188).
- No live rolling-upgrade run has exercised the mechanism.
