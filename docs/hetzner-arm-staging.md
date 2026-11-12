# Deploying a multi-service staging stack on Hetzner CAX21 (ARM64)

**Status:** Validated on a single Hetzner CAX21 node (Ampere Altra ARM64, Ubuntu 24.04 LTS). All 8 services reach `RUNNING`. Validation required `feat/staging-compat` fixes — see [PR #16](https://github.com/lithqube/minga/pull/16).

**Version:** Minga v1.1.0 (first validated with v1.0.2 on `feat/staging-compat`; not yet re-run on v1.1.0)

> **Not yet re-validated:** the API token for Traefik (Basic auth), the node-side `host-gateway` address, the `DOCKER_CONFIG` setup and named users were changed after that run. They are covered by unit tests, and named users also by a daemon-backed test, but not yet by a redeploy of this stack.

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
  Minga v1.1.0 (single binary)
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
2. Minga v1.1.0 installed (`install.sh --with-systemd`)
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
# MINGA_API_TOKEN: without it the API is unauthenticated, and any container
# on the host could deploy privileged containers through it.
EnvironmentFile=/etc/minga/minga-api.env
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

**`/etc/minga/minga-api.env`** (root-owned, mode 0600):

```bash
sudo install -m 0600 /dev/null /etc/minga/minga-api.env
echo "MINGA_API_TOKEN=$(openssl rand -hex 32)" | sudo tee /etc/minga/minga-api.env >/dev/null
```

The API listens on every interface so containers can reach it through their bridge gateway. ufw keeps 2375 closed to the internet. If ufw also filters bridge traffic on your host, allow the Minga bridge range to reach the API: `sudo ufw allow from 10.100.0.0/16 to any port 2375 proto tcp`.

---

## Compose file highlights

### Traefik as reverse proxy

Traefik v3's Swarm provider points at the Minga API on the host. `host-gateway` resolves on the node to the host's address on Traefik's bridge (Minga v1.0.2+).

Traefik can't send a bearer token, but it can send Basic auth, and Minga accepts the API token as the Basic-auth password (any username). Put the credentials in an env file kept out of git, mode 0600:

```bash
# traefik.env
TRAEFIK_PROVIDERS_SWARM_USERNAME=traefik
TRAEFIK_PROVIDERS_SWARM_PASSWORD=<the MINGA_API_TOKEN value>
```

```yaml
traefik:
  image: traefik:v3
  env_file: ./traefik.env
  extra_hosts:
    - "host.docker.internal:host-gateway"
  command:
    - "--providers.swarm.endpoint=http://host.docker.internal:2375"
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

> Minga has one token and no read-only role yet, so Traefik holds full API access. Treat the Traefik container like the API itself. `env_file` values are stored in the service spec, the same as `environment`.

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

Minga v1.0.2+ reads the Docker credential file on every pull: a task's automatic pull and `minga image pull`. v1.0.2 reads inline `auth` entries only; v1.1.0 also reads credential helpers (`credHelpers`, then `credsStore`), which must then be on the manager's `PATH`, and needs containerd 1.7+ for credentialed pulls. The manager runs as root, so point it at a root-owned file explicitly instead of relying on `HOME`:

```bash
sudo install -d -m 0700 /etc/minga/docker
read -rs GHCR_TOKEN   # paste the token; it stays out of shell history
printf '{"auths":{"ghcr.io":{"auth":"%s"}}}' \
  "$(printf '%s' "github-username:$GHCR_TOKEN" | base64 -w0)" \
  | sudo install -m 0600 /dev/stdin /etc/minga/docker/config.json
unset GHCR_TOKEN
echo 'DOCKER_CONFIG=/etc/minga/docker' | sudo tee -a /etc/minga/minga.env
```

Then pre-pull, or let the first deploy pull:

```bash
sudo DOCKER_CONFIG=/etc/minga/docker minga image pull ghcr.io/your-org/your-image:tag
```

Docker Hub entries written by `docker login` (key `https://index.docker.io/v1/`) match `docker.io` images. On v1.0.2, `ctr` takes the credentials on its command line, so other local users can see them in `ps` while a pull runs. From v1.1.0 the pull goes through containerd's Transfer API and the credential never appears on a command line.

With `minga-ctl registry login`, point the context at the same file, because minga-ctl's default is `/root/.docker/config.json`: `minga-ctl context add staging --ssh <user>@<node> --remote-token-file /etc/minga/api-token --registry-auth-file /etc/minga/docker/config.json`. The SSH user needs read and write access to that file.

### Named users

`user:` takes a name or a uid, with an optional group (`user[:group]`), as in Docker. Names are resolved from the image's own `/etc/passwd` and `/etc/group` when the task starts. A name with no group takes the user's primary group.

```yaml
zitadel:
  image: ghcr.io/zitadel/zitadel:latest
  user: "zitadel"

ai-service:
  image: ghcr.io/your-org/your-ai:latest
  user: "app:app"
```

> Requires Minga v1.0.2+. Supplementary groups from `/etc/group` are applied from v1.1.0.

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

The issues below were found by deploying this stack on a fresh CAX21 node. The compose-compatibility ones are fixed for Minga v1.0.2.

| Issue | Symptom | Fix |
|-------|---------|-----|
| `env_file` rejected | `error: env_file is part of the Compose spec but not supported yet` | PR #16: load files, merge into environment |
| `extra_hosts` rejected | Same | PR #16: parse and write a per-task `/etc/hosts`. `host-gateway` now resolves on the node to the bridge address; PR #16 used the default route, which on Hetzner is the upstream router. |
| Named `user:` rejected | `named users are not supported; set a numeric user:` | Resolved against the image's `/etc/passwd` and `/etc/group` at task start. PR #16 only improved the error. |
| String `command:` breaks | A folded (`>`) or plain string `command` runs as one argv entry | Split with shell quoting, as Compose does |
| Private image 401 | `ctr: failed to fetch anonymous token: 401 Unauthorized` | PR #16: read the Docker credential file on auto-pull; now also `minga image pull`, `DOCKER_CONFIG`, and Docker Hub's `https://index.docker.io/v1/` key |
| `minga image pull` fails on containerd 2.2 | `ctr: unknown command "unpack"` | Pull only; `ctr images pull` unpacks |
| Traefik gets 401 from the API | API token can't be sent by the Swarm provider | API accepts the token as the Basic-auth password |
| Relative bind mount fails | `no such file or directory: ./docker/staging/zitadel.yaml` | PR #16: join against compose file parent directory |
| File bind mount `EEXIST` | `create bind source: File exists (os error 17)` | Workaround: ensure no prior mount at that path; fix planned for v1.0.2 |
| Stale task `AlreadyExists` | `task already exists` on redeploy after Raft state wipe | Workaround: `ctr -n minga tasks delete --force <id>` then restart containerd |

---

## Checklist before `minga deploy`

- [ ] DNS records pointing to VPS IP
- [ ] ufw: 22, 80, 443 open; NATS (4222) and the API (2375) not exposed externally
- [ ] `MINGA_API_TOKEN` set in `/etc/minga/minga-api.env` (0600) and Traefik's `traefik.env`
- [ ] Secrets populated (env vars or secrets-agent)
- [ ] GHCR credentials in `/etc/minga/docker/config.json`, `DOCKER_CONFIG` set in `minga.env`
- [ ] `minga validate` returns no errors
- [ ] `curl http://localhost:9100/readyz` returns `OK`
