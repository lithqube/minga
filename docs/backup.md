# Backup and restore

Minga keeps two kinds of state. **Desired state** (services, networks, volumes, secret and config names) lives in Raft and can be exported to one JSON file with `minga state export`. **Everything else** (secret values, volume data, TLS files, the event bus) is on disk and you copy it yourself. A full restore needs both.

> **The export file is sensitive.** Service specs carry environment values, and people put passwords and API keys there. `minga state export -o FILE` writes the file with mode 0600. Keep it that way, store it encrypted, and do not paste it into tickets or chat. Without `-o` it goes to stdout, so redirecting it yourself (`> file`) gives the file your umask, not 0600.

## What the export holds

`minga state export` calls `GET /v1.41/state` on `minga api` and writes:

| Field | Content |
|---|---|
| `format_version` | Persisted-format version of the build that exported it (`minga_state::codec::FORMAT_VERSION`, 2 today) |
| `exported_at_unix` | Export time |
| `services` | Full service specs: image, replicas, env, ports, mounts, networks, restart and update policy, secret references, labels, healthcheck |
| `networks` | Overlay networks (name, driver, subnet) |
| `volumes` | Named volume records (name, driver), **not their data** |
| `secrets`, `configs` | Names and ids only |

Nodes and tasks are left out on purpose. Nodes register themselves when they start, and the scheduler places tasks for the restored services.

## What the export does not hold

| Not in the export | Where it is | How to copy it |
|---|---|---|
| Secret **values** | `--secret-store` on each node (default `/var/lib/minga/secrets`) | Copy the directory with permissions kept, e.g. `sudo tar -C /var/lib/minga -cpf secrets.tar secrets`. Same sensitivity as the export file, more so. |
| Named volume **data** | `<data-dir>/volumes` on the node that runs the container (default `/var/lib/minga/volumes`) | Stop the workload or snapshot the filesystem, then `tar` the directory. Databases need their own dump or a quiesced copy. |
| TLS material | Files you passed to `--raft-tls-ca/-cert/-key`, and `MINGA_NATS_TLS_*` | Copy the files. The CA key from `minga tls init-ca` is the one to keep offline. |
| NATS/JetStream data | `<data-dir>/nats` on the manager (or your external NATS cluster) | Not needed to restore desired state. Heartbeats and task status rebuild themselves; command and event history and queued commands are lost if you skip it. |
| Task logs | `<data-dir>/logs` | Copy if you need the history. |
| The API token, `/etc/minga/minga.env`, systemd units | Your host config | Back up with the rest of your host configuration. |

The export refers to secrets by id. Restore the values into `--secret-store` on the nodes that run those services.

## Export

```bash
export MINGA_API_TOKEN=...   # if the API requires a token
minga state export -o minga-state.json --api-url http://127.0.0.1:2375
```

It refuses to replace an existing file unless you pass `--force`. Run it from a cron job or timer and ship the file off the host.

## Import

```bash
minga state import minga-state.json --api-url http://127.0.0.1:2375
```

Import writes through Raft in dependency order: networks, secrets, configs, volumes, then services.

- If any service in the file already exists, nothing is written and the API answers **409**. Pass `--overwrite` to replace those services with the file's version. Networks, volumes, secrets and configs are always upserted.
- A file with a higher `format_version` than the running build is refused (use the newer binary). A lower one is refused too, because no migration exists yet. Both answer 400 and write nothing.
- Tasks are not imported. The scheduler places tasks for the restored specs. Not verified: how running tasks of a service replaced with `--overwrite` are rolled over; test that on a spare cluster first.

`GET` and `POST /v1.41/state` sit behind the same bearer token as every other API route. They will be admin-only once API roles exist ([ADR 0005](adr/0005-api-identity-and-roles.md), proposed).

## Restore on a fresh node

1. Install the same `minga` version (or newer, if the file's `format_version` matches). Check with `minga --version` and the `format_version` field of the file.
2. Restore host files: TLS files, `/etc/minga/minga.env`, and the secret store (`/var/lib/minga/secrets`) with its permissions.
3. Restore volume data into `<data-dir>/volumes` if you backed it up.
4. Start the processes as you did originally, with empty data directories. On one host that is the manager and `minga api` (see the [quick start](../README.md#quick-start-one-linux-host)), each with its own `--data-dir`. Wait until `minga api` is serving (`/readyz` returns 200) and a node has registered.
5. Import:
   ```bash
   minga state import minga-state.json --api-url http://127.0.0.1:2375
   ```
6. Check that tasks get placed: `curl -H "Authorization: Bearer $MINGA_API_TOKEN" http://127.0.0.1:2375/v1.41/tasks`.

A node that was in the old cluster and comes back with a stale `--data-dir` keeps its old Raft state. Restore into a fresh cluster, or clear the data directories on every voter first.

## Cold copy: the alternative

If you can stop the node, copy the Raft state itself. Every `minga` process that is a Raft voter has its own `--data-dir` holding `raft_log.redb` and `state.redb`: the manager's, and the one given to `minga api`. A single host has two.

1. Stop **both** processes (`SIGINT`, so they shut down cleanly; `systemctl stop` does this for the shipped unit).
2. Copy both data directories with permissions kept (`sudo tar -cpf ...`). Include `secrets`, `volumes` and `nats` if they live under them.
3. To restore, put the directories back on a host with the **same node ids, bind addresses and peer lists**, and start both processes.

A cold copy restores nodes, tasks and Raft membership exactly, but only for a `minga` build that reads the same data format (a newer format is refused, an older one has no migration). Do not copy the files of a running process: redb files copied mid-write may be unusable. Use `minga state export` for a backup without downtime, and the cold copy when you need an exact image or are moving a whole cluster.
