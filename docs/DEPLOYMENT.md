# Deployment

Target: **k3s on Raspberry Pi 5, arm64 only.** Cilium CNI + Cilium Gateway-API. nfs-synology
CSI. Vault + External Secrets Operator. **ArgoCD owns all rollouts.**

---

## ⚠️ ARM64 gotchas — each of these costs an evening if you hit it cold

### 1. Never use Alpine for Python services

`scikit-learn`, `xgboost`, `lightgbm`, and `duckdb` have **zero musllinux aarch64 wheels**.
On Alpine/ARM those become source builds requiring a full cmake toolchain, and compiling
scipy or sklearn on a Pi is an hours-long, frequently-OOMing exercise.

```dockerfile
FROM python:3.13-slim-bookworm     # ✅
FROM python:3.13-alpine            # ❌ silently catastrophic on arm64
```

### 2. `polars` is now a shim package

`polars` ships as a pure-Python wheel (`py3-none-any`) that depends on
**`polars-runtime-32`**, which carries the actual binary. Private mirrors or vendored wheel
caches that mirror only `polars` fail confusingly on ARM. Mirror both.

### 3. cgroup memory must be enabled or k8s limits are decorative

On Pi OS, **without this line k8s memory limits do not enforce** — a "capped" pod OOMs the
whole node instead. This is the classic Pi + k3s failure and it presents as random node
death, not as an OOMKilled pod.

```
# /boot/firmware/cmdline.txt — append to the SINGLE existing line, then reboot
cgroup_memory=1 cgroup_enable=memory
```

Verify: `cat /proc/cgroups | grep memory` → enabled column must be `1`.

### 4. pgvector from source defaults to `-march=native`

Produces `Illegal instruction` crashes when the build host CPU differs from the run host.
Use the official multi-arch image, or build with `make OPTFLAGS=""`. (We may not need
pgvector at all — only if we add semantic search over news.)

### 5. pandas 3.0 is a breaking major

Released 2026-07-22. Every tutorial assumes 2.x. We're Polars-native anyway, but pin
deliberately if a transitive dep drags pandas in.

### 6. Bun on arm64

First-class support, glibc ≥2.17 required (Pi OS Bookworm ships 2.36 — fine). The
baseline-vs-modern build split is **x64-only**, so the "which build" problem doesn't exist
here. ⚠️ **Real gotcha: lockfiles generated on x86 can poison ARM installs** — npm optional
deps may try to pull `@oven/bun-linux-x64-baseline`. Regenerate the lockfile on ARM or pin
platform-correct deps. Use `oven/bun:debian` or `:slim`, **not `:alpine`** if the image
shares a base with anything Python.

### 7. Verify the architecture

`uname -m` must return `aarch64`. 32-bit armhf has essentially no wheel coverage.

---

## Cluster layout

| Node | RAM | Role |
|---|---|---|
| pi-1 | 8 GB | k3s server, Cilium Gateway |
| pi-2 | **16 GB** | Postgres + Redis |
| pi-3 | 8 GB | API + frontend + scheduler |
| pi-4 | **16 GB** | Simulation workers |

**Budget math:** k3s agent is ~268 MB, server-with-workload ~1.6 GB (measured on Pi 4B;
Pi 5 is faster but similar memory). On 8 GB that leaves ~7.3 GB usable. A 4 GB node can
host the control plane and nothing else — **do not put Postgres on a 4 GB node.**

Set explicit memory limits on sim workers. The realistic OOM cause is a vectorized
`(n_sims × n_players)` array, not steady-state load — see [`ENGINE.md`](ENGINE.md) §10 on
chunking.

---

## Storage

```
nfs-synology CSI
  ├── pvc-ffh-postgres        Postgres data
  └── pvc-ffh-lake            Parquet lake (RWX — many readers)
```

**Postgres on NFS is officially supported, with two required options:**

| Where | Option | Why |
|---|---|---|
| Client mount | **`hard`** | The only firm PostgreSQL requirement. On network trouble processes hang instead of corrupting. |
| Client mount | `noatime` | Pointless writes otherwise |
| **NFS server export** | **`sync`** | Without it an `fsync` on the client isn't guaranteed to reach permanent storage — equivalent to running with `fsync off` |

`async` on the *client* is fine; Postgres issues its own `fsync` at the right times.

⚠️ An fsync failure makes Postgres **PANIC**. With `hard` that's the correct trade
(hang over corrupt), but monitor for it.

**Never place a SQLite or `.duckdb` file on the NFS PVC.** DuckDB reads Parquet read-only —
that's plain file I/O with no lock contention, so many reader pods are safe. A DuckDB
*database file* on NFS with concurrent access is not.

`shared_buffers` 1–2 GB on the 16 GB node. Do not let Postgres swap; don't put swap on NFS.

---

## Secrets — Vault + ESO

**No secret is ever committed, templated into a manifest, or read from a `.env` in git.**

| Secret | Vault path |
|---|---|
| Anthropic API key | `kv/ffh/llm/anthropic` |
| OpenAI API key | `kv/ffh/llm/openai` |
| Postgres credentials | `kv/ffh/db/postgres` |
| ESPN `espn_s2` + `SWID` cookies | `kv/ffh/platform/espn` |
| Yahoo OAuth client + refresh token | `kv/ffh/platform/yahoo` |
| API bearer token | `kv/ffh/api/token` |

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: ffh-llm-keys
spec:
  refreshInterval: 1h
  secretStoreRef: {name: vault-backend, kind: ClusterSecretStore}
  target: {name: ffh-llm-keys}
  dataFrom:
    - extract: {key: kv/ffh/llm}
```

⚠️ **ESPN cookies expire** and must be re-extracted from browser devtools periodically.
The app must surface "ESPN auth expired" clearly rather than degrading into empty
responses that look like an empty league. Yahoo refresh tokens roll on use — persist the
rotated token back to Vault or the next refresh fails.

---

## Ingress — Cilium Gateway-API

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: ffh
spec:
  parentRefs: [{name: cilium-gateway, namespace: gateway}]
  hostnames: ["ffh.homelab.local"]
  rules:
    - matches: [{path: {type: PathPrefix, value: /api}}]
      backendRefs: [{name: ffh-api, port: 8000}]
    - matches: [{path: {type: PathPrefix, value: /}}]
      backendRefs: [{name: ffh-frontend, port: 3000}]
```

⚠️ **The draft WebSocket needs a long timeout.** A draft runs 1–3 hours; a default
30–60s idle timeout will drop the socket mid-draft. Configure the Gateway listener
accordingly and implement client reconnect-with-state-replay regardless (see
[`API.md`](API.md)) — belt and braces, because losing the board at pick 1.03 is the
worst possible failure.

---

## ArgoCD — GitOps, no exceptions

```
deploy/
  base/                  kustomize base — deployments, services, PVCs, ESO
  overlays/homelab/      arm64 nodeSelectors, resource limits, hostnames
  argocd/                Application manifests (app-of-apps)
```

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata: {name: ffh, namespace: argocd}
spec:
  project: default
  source:
    repoURL: https://github.com/<user>/FantasyFootballHelper
    targetRevision: main
    path: deploy/overlays/homelab
  destination: {server: https://kubernetes.default.svc, namespace: ffh}
  syncPolicy:
    automated: {prune: true, selfHeal: true}
    syncOptions: [CreateNamespace=true]
```

**Never `kubectl apply` by hand.** `selfHeal: true` will revert it and you'll waste an hour
confused. Change git, let Argo reconcile.

⚠️ **Freeze auto-sync during the live draft.** A reconcile that restarts the API pod
mid-draft is a self-inflicted outage at the worst moment. Set `automated: null` on draft
day, or add a maintenance-window annotation.

---

## Scheduled jobs

k8s `CronJob`, not in-process schedulers — they survive pod restarts.

| Job | Schedule (ET) | Purpose |
|---|---|---|
| `ingest-nflverse` | Daily 06:00, +hourly Sun/Mon in season | pbp, stats, snaps, charting |
| `ingest-odds` | Every 30 min in season | nflverse games.csv + ESPN live odds |
| `ingest-players` | Daily 05:00 | Sleeper players blob (5 MB, ≤1×/day) + crosswalk refresh |
| `ingest-injuries` | Every 2h Wed–Sun | Sleeper + ESPN injury status |
| `ingest-weather` | Every 6h, hourly within 24h of kickoff | Open-Meteo forecasts |
| `ingest-market` | Daily 07:00 | FantasyCalc, DynastyProcess ECR, ADP |
| `compute-projections` | Daily 08:00 | Full projection rebuild |
| `weekly-lineup` | **Tue 09:00** | Lineup analysis + debate (Batch API) |
| `weekly-waiver` | **Tue 10:00** | Waiver + FAAB (Batch API) |
| `season-sim` | Daily 09:00 in season | Playoff odds, all-play, schedule luck |

Lineup and waiver run Tuesday so recommendations are ready well before the Thursday lock,
and Batch API's 24h turnaround is irrelevant at that cadence (50% cost saving).

All ingest jobs are **idempotent and watermarked** via `ingest_runs`. Use `If-None-Match`
and treat 304 as `skipped_not_modified` — on The Odds API a 304 costs zero credits.

---

## CI — GitHub Actions

**Build `linux/arm64` only.** The images only ever run on Pis; a multi-arch manifest is
pure overhead.

```yaml
jobs:
  build:
    runs-on: ubuntu-24.04-arm     # native arm64, free, GA for private repos since Jan 2026
```

⚠️ **Do not use QEMU emulation** (`docker/setup-qemu-action`). It's 10–40× slower and flaky
for exactly the Rust and native code in this stack. Native ARM runners are free — use them.

Pipeline: `ruff check` + `ruff format --check` → `pytest` (incl. the engine-purity and
crosswalk tests) → `bun test` → build + push to GHCR → Argo reconciles.

**CI does not deploy.** It builds and pushes an image and updates the tag in `deploy/`.
ArgoCD does the rest.

---

## Observability

Minimum viable, but do it — a silent wrong number is this system's worst failure mode:

- `/metrics` Prometheus endpoint on the API
- Alert on: ingest job failure, **crosswalk unmatched count > 0**, LLM error rate, LLM
  spend above a monthly threshold, Postgres fsync errors, node memory pressure
- Structured JSON logs with a request/debate correlation ID

**The crosswalk alert is the important one.** Everything else fails loudly; that one fails
as a plausible-looking recommendation computed on an incomplete roster.
