# Wave 10 execution checkpoint — 2026-10-27

This is a development checkpoint, **not** a production deployment or compliance attestation.

## Verified locally

- Planning artifact commit: `7452901` on `main` (local; not pushed by this execution).
- B/TLS: explicit CA and client identity configuration, TLS-only connector, separate NATS client/route configuration, real local `nats-server` integration tests. The built-in sidecar is now restricted to loopback when plaintext; remote use requires an independently configured TLS NATS server. Production PKI issuance, rotation, authorization and Talos packaging remain unverified.
- C/secrets: Compose `external` references propagate into task specifications; Compose `file:`/`environment:` and Docker API inline `Data` are rejected rather than silently discarded. Node-local resolver and OCI read-only mount generation have unit tests. Docker API references validate targets/options and declared metadata. This is **not** production-safe secret delivery yet: the mount path can be swapped after validation, there is no per-task staging, placement inventory, proven host namespace visibility, multi-node provisioning or rotation procedure.
- Quality gate after these changes: `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test --workspace` each exited 0 in separate unpiped shell invocations. The first full test run caught a parallel NATS test port collision; the fixture was serialized and the full gate then passed.

## Feasibility probes and blockers

- Talos QEMU: `talosctl cluster create qemu` required privilege; `sudo -n true` reported a password requirement. No Talos host PASS was achieved. No billable hcloud resources were provisioned.
- Separate Linux containerd probe: a privileged Linux/arm64 Docker container ran containerd 1.7.17 and runc 1.1.14. The first Task.Create attempt with overlayfs failed at rootfs mount (`invalid argument`). With the native snapshotter, Task.Create reached runc but failed while applying cgroups (`cgroup.procs: no such file or directory`). This nested Docker environment did not prove container execution or FIFO stdout/stderr behavior.
- After the user selected OrbStack for testing, `orb create --arch arm64 --memory 4G --cpus 2 --disk 20G ubuntu:24.04 minga-linux-probe` created a local Linux machine. Its containerd 2.2.1/runc 1.3.4 successfully executed a BusyBox container with separate stdout and stderr. A `ctr run --fifo-dir` probe emitted distinct `fifo-stdout` and `fifo-stderr` into separate redirected outputs, and live inspection found distinct stdout/stderr FIFO nodes. A bind mount with `bind,ro,nosuid,nodev,noexec` delivered an exact disposable sentinel inside a container and refused writes. **These are Linux/containerd CLI probes, not Minga runtime end-to-end tests or Talos host evidence.**
- OrbStack's `orb create` supports predefined Linux distributions, not custom Talos images. Its machines share the OrbStack kernel, and its documentation says nested KVM virtualization on Apple Silicon is unavailable. Therefore an OrbStack Ubuntu machine cannot satisfy the Talos host gate; a separate Talos-capable hypervisor/host or authorized hcloud deployment is still needed.
- A/log capture: `ctr --fifo-dir` demonstrated Linux FIFO stream separation, but the raw Minga Task.Create destination syntax, reader lifecycle and real `minga logs` path were not validated; runtime still passes empty stdout/stderr destinations. Log replay, durable PubAck cursor, authorization and history/follow handoff remain open.

## Next gates

1. Obtain explicit approval for chargeable hcloud Talos staging **or** a usable privileged local Talos/Linux host with containerd, runc and working cgroups. Record an actual host PASS before staging claims.
2. Resolve C's staging/rename race, protected volume, node inventory, per-node provisioning and rotation; test exact read-only bytes inside a real Linux container.
3. On a working Linux host, probe FIFO syntax and stdout/stderr bytes, then implement bounded durable capture, acknowledged replay, task authorization and gap-free follow.
4. Run the unpiped Rust quality gate after code review; publish corrected deployment documentation and separate FIPS, SOC 2, VS-NfD and NATO control-gap evidence only to the extent actually supported. None of these are certifications.
