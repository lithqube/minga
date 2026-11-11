# Deploying a multi-service staging stack on Hetzner CAX21 (ARM64)

**Status:** Validated on a single Hetzner CAX21 node (Ampere Altra ARM64, Ubuntu 24.04 LTS). All 8 services reach `RUNNING`. Validation required `feat/staging-compat` fixes — see [PR #16](https://github.com/lithqube/minga/pull/16).

**Version:** Minga `feat/staging-compat` (targeting v1.0.2)

> **What this covers.** A real-world staging deploy of a Nuxt BFF, an AI service, a NATS workers process, NATS JetStream, an auth server (ZITADEL), Postgres, a secrets agent, and Traefik for TLS — all on one CAX21 node using only containerd and Minga. No Docker daemon. Private images from GHCR.

---

## Why Hetzner CAX21 for staging?

| Property | Benefit |
|----------|---------|
| **ARM64 (Ampere Altra)** | Better single-thread perf per euro than x86 shared-core at this tier; ideal for HTTP-proxy + embedding-math workloads |
| **~€8/mo (4 vCPU / 8 GB)** | Full 8-service staging stack for less than the cost of a CI runner |
| **Ubuntu 24.04 LTS** | 5-year LTS; containerd ships in `apt`; `ufw` works out of the box |
| **Hetzner cloud-init** | A provisioning script can run on first boot via `runcmd` — zero manual steps |

---

## Stack topology

```
Hetzner CAX21 (Ubuntu 24.04 LTS, ARM64)
  Minga v1.0.2 (single binary)
    traefik:v3         :80/:443   TLS termination, Swarm-label routing
    bff:3000           internal   Nuxt BFF (private GHCR image, amd64+arm64)
    workers            internal   NATS consumer (same image as bff)
    ai-service:8000    internal   FastAPI AI service (private GHCR image, amd64+arm64)
    nats:4222          internal   JetStream, file-backed
    zitadel:8080       internal   Auth server (uid 1000)
    db:5432            internal   Postgres 16
    secrets-agent      internal   Renders /opt/app/secrets/secrets.env every 60s
```

Traefik discovers routes from Swarm labels via `minga api` at `:2375`.
Only ports 22, 80, and 443 are open to the internet (ufw).

---

## Prerequisites

1. Ubuntu 24.04 LTS on the VPS
2. Minga v1.0.2 installed (`install.sh --with-systemd`)
3. Containerd (`apt install containerd`)
4. `ufw` active, only 22/80/443 open

```bash
curl -fsSL https://github.com/lithqube/minga/releases/latest/download/install.sh | sh --with-systemd
```

---

## Single-node configuration

Two processes on one host: `minga manager` (scheduler + supervisor) and `minga api` (Docker Engine API). Both are Raft voters and must start together.

**`/etc/minga/minga.env`** (manager):
```
MINGA_ROLE=manager
MINGA_NODE_ID=1
MINGA_BIND=127.0.0.1:7001
MINGA_EXTRA_ARGS=--peer 2=127.0.0.1:7002 --metrics-bind 127.0.0.1:9100
```

> The `minga.service` installed by `install.sh` already passes `--data-dir`, `--runtime containerd` and `--containerd-socket` — do not repeat them in `MINGA_EXTRA_ARGS`.

**`/etc/systemd/system/minga-api.service`** (second process):
```ini
[Unit]
Description=Minga Docker Engine API
After=network.target minga.service

[Service]
ExecStart=/usr/local/bin/minga api \
  --node-id 2 --bind 127.0.0.1:7002 \
  --peer 1=127.0.0.1:7001 \
  --data-dir /var/lib/minga-api \
  --api-bind 0.0.0.0:2375
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

---

## Compose file highlights

### Traefik as reverse proxy

Traefik v3's Swarm provider points at the Minga API. Use the host's gateway IP directly (Minga v1.0.2+ supports `extra_hosts: ["host.docker.internal:host-gateway"]`; on v1.0.1, substitute the IP from `ip route | awk '/default/ {print $3}'`):

```yaml
traefik:
  image: traefik:v3
  command:
    - "--providers.swarm.endpoint=http://172.31.1.1:2375"
    - "--providers.swarm.network=myapp-network"
    - "--entrypoints.web.address=:80"
    - "--entrypoints.websecure.address=:443"
    - "--certificatesresolvers.le.acme.email=${ACME_EMAIL}"
    - "--certificatesresolvers.le.acme.storage=/letsencrypt/acme.json"
    - "--certificatesresolvers.le.acme.httpchallenge.entrypoint=web"
  ports:
    - "80:80"
    - "443:443"
  volumes:
    - traefik-certs:/letsencrypt
```

Services opt in with labels:
```yaml
bff:
  image: ghcr.io/your-org/your-bff:${VERSION}
  labels:
    - "traefik.enable=true"
    - "traefik.http.routers.bff.rule=Host(`staging.example.com`)"
    - "traefik.http.routers.bff.entrypoints=websecure"
    - "traefik.http.routers.bff.tls.certresolver=le"
    - "traefik.http.services.bff.loadbalancer.server.port=3000"
```

### Private GHCR images

Pre-pull before `minga deploy` using `--user`:

```bash
ctr -n minga images pull \
  --user "github-username:ghp_your_classic_pat" \
  ghcr.io/your-org/your-image:tag
```

Minga v1.0.2+ reads `~/.docker/config.json` automatically on pull (inline `auth` base64 format). Configure it once:

```bash
mkdir -p ~/.docker
printf '{"auths":{"ghcr.io":{"auth":"%s"}}}' \
  "$(echo -n 'github-username:ghp_token' | base64 -w0)" \
  > ~/.docker/config.json
```

### Named users (uid override)

ZITADEL and Python services often run as named users. Add `user: "UID"` to override:

```yaml
zitadel:
  image: ghcr.io/zitadel/zitadel:latest
  user: "1000"

ai-service:
  image: ghcr.io/your-org/your-ai:latest
  user: "10001"   # match the uid from the image Dockerfile
```

> Named string users require Minga v1.0.2+.

### NATS JetStream

```yaml
nats:
  image: nats:2-alpine
  command: ["-js", "--store_dir", "/data/jetstream", "-m", "8222", "-n", "staging"]
  volumes:
    - nats-data:/data/jetstream
```

---

## Deploying

```bash
# Validate first
MINGA_API_URL=http://localhost:2375 MINGA_API_TOKEN=$TOKEN \
  minga validate -c compose.yaml

# Deploy
MINGA_API_URL=http://localhost:2375 MINGA_API_TOKEN=$TOKEN \
  minga deploy -c compose.yaml mystack
```

---

## Issues found during first real-world deploy

All five compose-compatibility issues below were found by deploying this stack on a fresh CAX21 node. All are fixed in Minga v1.0.2 (PR #16).

| Issue | Symptom | Fix |
|-------|---------|-----|
| `env_file` rejected | `error: env_file is part of the Compose spec but not supported yet` | PR #16: load files, merge into environment |
| `extra_hosts` rejected | Same | PR #16: parse + resolve `host-gateway` via `/proc/net/route` |
| Named `user:` rejected | `named users are not supported; set a numeric user:` | PR #16: resolve name against image `/etc/passwd` |
| Private image 401 | `ctr: failed to fetch anonymous token: 401 Unauthorized` | PR #16: read `~/.docker/config.json` inline `auth` on pull |
| Relative bind mount fails | `no such file or directory: ./docker/staging/zitadel.yaml` | PR #16: join against compose file parent directory |
| File bind mount `EEXIST` | `create bind source: File exists (os error 17)` | Workaround: ensure no prior mount at that path; fix planned for v1.0.2 |
| Stale task `AlreadyExists` | `task already exists` on redeploy after Raft state wipe | Workaround: `ctr -n minga tasks delete --force <id>` then restart containerd |

---

## Checklist before `minga deploy`

- [ ] DNS records pointing to VPS IP
- [ ] ufw: 22, 80, 443 open; NATS (4222) not exposed externally
- [ ] Secrets populated (env vars or secrets-agent)
- [ ] GHCR credentials in `~/.docker/config.json` (or pre-pull with `--user`)
- [ ] `minga validate` returns no errors
- [ ] `curl http://localhost:9100/readyz` returns `OK`
