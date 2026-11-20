# QA v2 · issue #124 · PR #149

Round 2 after the R1 fix (`3118d35`). Tested on the branch with `main` merged in.

## AC ledger

| AC | Test | Log |
|---|---|---|
| AC1 API returns 400 | `minga-api` `handlers::tests::network_subnet_outside_the_overlay_is_refused`; rule in `minga-net` `network_subnet_must_stay_inside_the_supernet_and_off_the_mesh` | `ac1-api-and-rule.log` |
| AC2 state machine refuses | `minga-state` `upsert_network_apply_uses_only_replica_independent_rules` (apply: mesh, /28, syntax, the same on every replica); `tests/single_node.rs` `upsert_network_refuses_subnets_the_overlay_must_not_route` (supernet, checked before propose); `a_snapshot_with_one_refused_network_writes_none` | `ac2-state-machine-and-store.log` |
| AC3 peer set | `minga-scheduler` `tests/scheduler.rs` `relay_manager_carries_only_unique_addresses_inside_each_network` | `ac3-peer-set.log` |
| AC4 duplicate AllowedIP | `minga-node` `an_allowed_ip_listed_under_two_peers_is_refused` | `ac4-duplicate-allowed-ip.log` |

AC2 note: the supernet half is enforced in `StateStore::upsert_network` (the only proposer of `UpsertNetwork`), not in the Raft apply, so replicas with different `--overlay-supernet` flags agree on every log entry.

## Test-gap check

Run by a read-only agent given only the ACs and the diff. High gaps found and written red-first:

| Gap | Witness | Test |
|---|---|---|
| Broadcast check survived | `10.100.0.0/15` under supernet `10.100.0.0/16` | `overlay.rs` refused list |
| Node-tenant check in `owned` survived | tenant-B tasks on default NAT workers claiming `b-net` | relay test |
| Network-tenant check in `owned` survived | default tasks on default NAT workers claiming tenant B's `b-net` | relay test |
| Snapshot import wrote partway (CodeRabbit) | snapshot with `a` 10.100.1.0/24 and `b` 192.168.1.0/24 under supernet 10.100.0.0/16 | `single_node.rs` |

The tenancy test (`multi_tenant_placement_and_wireguard_routes_stay_tenant_scoped`) moved to `10.100.70.0/24`: its old `10.70.0.0/24` is outside the supernet, so since R2 none of its addresses routed.

## Mutants (each broken, run, restored, rebuilt)

| Mutant | Result |
|---|---|
| Drop the broadcast check | caught |
| Drop the node-tenant check in `owned` | caught |
| Drop the network-tenant check in `owned` | caught |
| Drop the stored-subnet filter (R2) | caught |
| Drop the import pre-check | caught |
| Drop the store's pre-propose check | caught |

## Test gaps (not written)

- Medium: one stopped and one live claimant of the same address (witness: node 2 Stopped, node 3 Running on `10.100.1.20`, expect it routes to node 3 only). These mutants fail closed: the address stops routing, nothing misroutes.
- Medium: the API uses the store's supernet, not the default (witness: `set_overlay_supernet("10.50.0.0/16")`, create `10.50.1.0/24`, expect 201).
- Low: `10.192.0.0/10` through the apply (mesh superset, no supernet); IPv6 input; malformed subnet at the API.

## Run

- `scoped-suite.log`: `cargo test -p minga-state -p minga-scheduler -p minga-net -p minga-api -p minga-node`, 653 passed, 0 failed.
- `clippy.log`: clean with `-D warnings`.
- Not run here: `cargo test --workspace` (merge gate) and the daemon-backed CI job.
