# QA: #127, PR #158

Code head: `7527f49`. The logs in this folder ran on that commit. The commit that adds this folder changes docs only.

| AC | Proof | Log |
|---|---|---|
| `UpsertNode` with a key held by another node returns a conflict | `a_wireguard_key_held_by_another_node_is_refused`, `only_a_new_or_changed_wireguard_key_is_checked_for_a_holder`, `a_copied_wireguard_key_is_refused_without_a_raft_write` | `ac1.log` |
| A peer set with one invalid peer still applies the remaining peers | `one_bad_peer_does_not_block_the_rest_of_the_set`, `peers_sharing_a_public_key_are_both_skipped`, `an_allowed_ip_listed_under_two_peers_skips_both` | `ac2.log` |
| A peer with an unresolvable endpoint does not abort the rest of `apply_peer_diff` | `a_peer_wg_cannot_add_does_not_abort_the_others`, `a_failed_removal_still_fails_the_whole_apply` | `ac3.log` |

Suites: `CARGO_INCREMENTAL=0 cargo test -p minga-state -p minga-node -p minga-scheduler -p minga-api` and `cargo test -p minga-cli`. 652 passed, 0 failed (`scoped-tests.log`, `cli-tests.log`).

Mutants (each guarded line was broken, the named test went red, and the line was restored):
- M1: the state key check disabled.
- M2: peer-set key conflicts ignored.
- M3: a failing `wg set` propagated.
- M4: the registration pre-check dropped.
- M5: every key checked, not only new or changed ones.

Not covered locally: the Linux-only `apply_peer_assignment` keeps a failed peer out of the stored set. CI compiles that code on Linux, and retries come from the scheduler re-putting every `.wg` key on each reconcile (`crates/minga-scheduler/src/lib.rs:494-504`).
