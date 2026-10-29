# Wave 10 execution checkpoint — 2026-10-27

This is a development checkpoint, **not** a production deployment or compliance attestation.

## Verified locally

- Planning artifact commit: `7452901` on `main` (local; not pushed by this execution).
- B/TLS: explicit CA and client identity configuration, TLS-only connector, separate NATS client/route configuration, real local `nats-server` integration tests. The built-in sidecar is now restricted to loopback when plaintext; remote use requires an independently configured TLS NATS server. Production PKI issuance, rotation, authorization and Talos packaging remain unverified.
- C/secrets: Compose `external` references propagate into task specifications; Compose `file:`/`environment:` and Docker API inline `Data` are rejected rather than silently discarded. Node-local resolver and OCI read-only mount generation have unit tests. Docker API references validate targets/options and declared metadata. This is **not** production-safe secret delivery yet: the mount path can be swapped after validation, there is no per-task staging, placement inventory, proven host namespace visibility, multi-node provisioning or rotation procedure.
- **Real-daemon container lifecycle is now GREEN.** `containerd_start_list_status_stop` passes against the live containerd daemon on the OrbStack Ubuntu 24.04 arm64 machine with `MINGA_REQUIRE_CONTAINERD=1` (no silent skip): **4 passed, 0 failed, exit 0**, repeated three consecutive times with no residue — no leftover task, container or snapshot after each run. The separate `containerd_client` suite is **6 passed, 0 failed** against the same daemon. `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test --workspace` each exited 0 in separate unpiped invocations on the macOS developer host.

## What the lifecycle work changed

The container path no longer needs a custom containerd client for the common case; the blockers found earlier are resolved:

- **Image resolution.** containerd's Images service has no Pull RPC, so Minga requires a preloaded, unpacked image in namespace `minga` and reads its OCI metadata through the Content service (`crates/minga-node/src/containerd/image.rs`). `Images.Get` returns a descriptor only, so the manifest, index, configuration and parent snapshot are resolved separately. Content reads are bounded and digest-verified.
- **Process configuration.** The OCI spec takes the image's `Entrypoint` + `Cmd` as process args, plus image env, working directory and user IDs, with explicit task environment overriding image values by key (`crates/minga-node/src/containerd/oci.rs`). This removed runc's `args must not be empty`.
- **Rootfs.** A writable snapshot is prepared from the image's unpacked parent and its mounts are passed to `Tasks.Create` (`crates/minga-node/src/containerd/runtime.rs`).
- **Cleanup ordering, a real bug.** `stop()` deleted the container even when task deletion failed. containerd refuses to delete a container that still has a task, and **a task whose container is gone can never be deleted again** — it is stranded in the namespace forever. The order is now task, then container, and a failed task delete aborts before the container is removed instead of being logged and ignored.
- **Shutdown timing, a second real bug.** After SIGTERM the code deleted the task without confirming the process had exited; containerd refused with `cannot delete a running process`. The stop path now waits for the stopped state, escalates to SIGKILL and waits again, and reports an error rather than leaking.
- **Restart hygiene.** `start()` clears a container, task or snapshot left by a previous attempt at the same task ID before creating new ones, so a failed start no longer blocks the next one.

The lifecycle test uses `docker.io/library/redis:8.10.2-alpine` rather than BusyBox: BusyBox's default command is `sh`, which exits immediately with closed stdin, so a BusyBox container can never be observed in the Running state. The earlier RED result was partly a bad assertion rather than only a runtime defect.

## Feasibility probes and blockers

- Talos QEMU: `talosctl cluster create qemu` required privilege; `sudo -n true` reported a password requirement. No Talos host PASS was achieved. No billable hcloud resources were provisioned.
- Separate Linux containerd probe: a privileged Linux/arm64 Docker container ran containerd 1.7.17 and runc 1.1.14. The first Task.Create attempt with overlayfs failed at rootfs mount (`invalid argument`). With the native snapshotter, Task.Create reached runc but failed while applying cgroups (`cgroup.procs: no such file or directory`). This nested Docker environment did not prove container execution or FIFO stdout/stderr behavior.
- After the user selected OrbStack for testing, `orb create --arch arm64 --memory 4G --cpus 2 --disk 20G ubuntu:24.04 minga-linux-probe` created a local Linux machine. Its containerd 2.2.1/runc 1.3.4 successfully executed a BusyBox container with separate stdout and stderr. A `ctr run --fifo-dir` probe emitted distinct `fifo-stdout` and `fifo-stderr` into separate redirected outputs, and live inspection found distinct stdout/stderr FIFO nodes. A bind mount with `bind,ro,nosuid,nodev,noexec` delivered an exact disposable sentinel inside a container and refused writes. **These are Linux/containerd CLI probes, not Minga runtime end-to-end tests or Talos host evidence.**
- OrbStack's `orb create` supports predefined Linux distributions, not custom Talos images. Its machines share the OrbStack kernel, and its documentation says nested KVM virtualization on Apple Silicon is unavailable. Therefore an OrbStack Ubuntu machine cannot satisfy the Talos host gate. The user chose to **keep Talos staging blocked and continue only with OrbStack Linux tests**; no hcloud provisioning is authorized.
- Minga Linux integration tests (running as root so the containerd socket is accessible): `containerd_client` completed 6/6 against the real daemon with no `SKIP`. A normal-user run also returned 6/6, but every case skipped on socket permissions and is **not** evidence. The 10 node secret/OCI unit tests completed on Linux without skips.
- Image provisioning remains **image-only**: the operator must preload and unpack the image in namespace `minga`. Minga still has no supported pull. A missing image, an incompatible platform or a missing unpacked snapshot must fail with a clear message.
- A/log capture: `ctr --fifo-dir` demonstrated Linux FIFO stream separation, but the raw Minga Task.Create destination syntax, reader lifecycle and real `minga logs` path were not validated; runtime still passes empty stdout/stderr destinations. Log replay, durable PubAck cursor, authorization and history/follow handoff remain open.
- Compose fidelity gaps found by source audit, all still open: `command`/`entrypoint` are parsed and dropped before reaching `TaskSpec`; `deploy.update_config` and `rollback_config` are parsed but never enforced, so there is no staged update and no automatic rollback; service labels are dropped, which removes Traefik's routing labels; published ports are recorded only as OCI annotations, and the network data plane is not applied. `minga deploy` translates and prints the stack and explicitly reports that cluster deployment is not implemented.

## Next gates

1. Prove the C secret path through Minga in a real Linux container: exact read-only bytes, the staging/rename race, a protected volume, node inventory, per-node provisioning and rotation.
2. Probe Minga's raw Task.Create FIFO syntax and stdout/stderr bytes, then implement bounded durable capture, acknowledged replay, task authorization and gap-free follow.
3. Carry `command`/`entrypoint` into `TaskSpec` and the OCI process, and make unsupported Compose semantics fail loudly rather than being silently dropped.
4. Implement service labels, container networking and host-port publication, then test Traefik v3 discovery against a live stack.
5. Build an update state machine for health-gated `start-first` rollout and automatic `failure_action: rollback`.
6. Publish corrected deployment documentation and separate FIPS, SOC 2, VS-NfD and NATO control-gap evidence only to the extent actually supported. None of these are certifications. **Talos/hcloud staging remains blocked by user choice** until separately authorized.
