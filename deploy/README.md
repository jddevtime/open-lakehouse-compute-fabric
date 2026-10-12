<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (c) 2026 Justin Dodrill -->
# Installing and deploying OLCF

Pick the route that fits the machine. Every route ends with the same server (`olcf serve`) and the same CLI (`olcf`).

| Route | For | Needs on the machine | Notes |
| --- | --- | --- | --- |
| [One-line installer](#1-one-line-installer-macos-linux-windows) | a laptop or workstation | `sh`, `curl` (macOS/Linux) or PowerShell (Windows) | no Xcode tools, no Python, no Java, no git, no admin rights |
| [Docker Desktop / Docker Engine](#2-docker-compose) | a laptop (Docker Desktop) or a server | Docker with Compose v2 | equally first-class; nothing is installed on the host |
| [Kubernetes (Helm)](#3-kubernetes-helm-chart) | a cluster | Helm 3.12+, an image registry | control plane x N, PostgreSQL, worker StatefulSet |
| [systemd / launchd / Task Scheduler](#4-run-as-a-service-without-containers) | bare metal, an always-on Mac or PC | the one-line install | hardened unit files |
| [Offline bundle](#5-air-gapped-install-offline-bundle) | machines without internet | a way to carry a directory | one tarball, verified checksums |

## 1. One-line installer (macOS, Linux, Windows)

macOS and Linux (installs the newest source of the `main` branch from GitHub; `--ref v0.1.0` pins a tag once one exists):

```sh
curl -fsSL https://raw.githubusercontent.com/jddevtime/open-lakehouse-compute-fabric/main/scripts/install.sh | sh
```

Windows (PowerShell 5.1 or 7; per user, no administrator rights):

```powershell
# Windows PowerShell 5.1 or 7, per user. Install uv, then OLCF from the GitHub source archive (every dependency is a wheel; nothing is compiled).
# The PowerShell installer scripts\install.ps1 is for a downloaded wheel or an offline bundle.
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
uv tool install --python 3.13 "olcf @ https://github.com/jddevtime/open-lakehouse-compute-fabric/archive/refs/heads/main.tar.gz"
olcf quickstart
```

Release downloads, once a release exists, are `https://github.com/jddevtime/open-lakehouse-compute-fabric/releases/latest/download/install.sh` (`install.ps1` likewise). Each release also carries `SHA256SUMS`, `*.sigstore.json` signatures (cosign keyless), an SBOM and
`THIRD_PARTY_NOTICES.md`. To verify a download before running it:

```sh
sha256sum -c SHA256SUMS --ignore-missing
cosign verify-blob --bundle install.sh.sigstore.json --certificate-identity-regexp 'https://github.com/jddevtime/open-lakehouse-compute-fabric/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com install.sh
```

From a checkout or a downloaded release (no network address involved):

```sh
sh scripts/install.sh --wheel dist/olcf-0.1.0-py3-none-any.whl          # --help lists every option
sh scripts/install.sh --dry-run                                          # shows what would happen, changes nothing
```

What it does, in order, and what it never does:

1. Downloads **uv** (a single static binary; SHA-256 verified before it is run) into `<prefix>/bin`.
2. `uv python install 3.13` provides a private Python under `<prefix>/python` (it never looks at the system Python).
3. Creates one virtual environment per OLCF version and installs **wheels only** (`--only-binary :all:`): nothing is ever compiled, so no
   compiler is needed.
4. `olcf init` creates the data directory, keys and the first administrator (the password is shown once). It also writes `environment = "development"` to
   `config.toml`, which is what lets this installation trust the development key that signs the bundled Spark runtime manifests; that is why it runs **before** the runtime step
   (with `--no-init` on a fresh data directory the runtime step is refused as "not trusted" unless `OLCF_RUNTIME_TRUST_DIR` points at your own key directory).
5. `olcf runtime install --with-jdk` installs the Spark runtime and, only if the machine has no Java 17/21, a checksum-verified
   **Eclipse Temurin JDK 21** under the data directory. Skip it with `--no-runtime`; SQL on DuckDB, lakehouses and the UI work without it.

It **never runs `git`, `python3`, `java`, `clang` or `make`** and never executes `/usr/bin/python3`, `/usr/bin/java` or `/usr/bin/git`.
On a Mac without the Xcode Command Line Tools those three are stubs that pop up the installer dialog; the scripts detect that
(`xcode-select -p`) and ignore them. `JAVA_HOME` or a JDK that OLCF downloaded itself is used instead.

Defaults: prefix `~/.local/share/olcf` (Linux/macOS) or `%LOCALAPPDATA%\olcf` (Windows), data `~/.olcf`. Re-running is safe; a newer version is
installed next to the old one, the database is backed up and migrated, and `current` switches only after a smoke test.

```sh
olcf serve                  # or: olcf quickstart  (init + serve + open the UI)
olcf doctor                 # prerequisites and configuration check, with remediation hints
olcf upgrade --version 0.2.0
olcf upgrade --rollback     # back to the previous version (a database backup is taken before every upgrade)
sh scripts/uninstall.sh     # keeps your data unless you ask: --remove-metadata --remove-secrets --remove-storage ... --purge, --dry-run
```

The uninstaller removes only what it recognises as OLCF, lists everything before deleting, and treats keys, metadata, lakehouse files, logs,
runtimes and backups as separate categories.

## 2. Docker Compose

Works the same on Docker Desktop (macOS, Windows, Linux) and Docker Engine. Run from the repository root.

```sh
sh deploy/scripts/gen-env.sh                 # writes deploy/.env (mode 0600) with fresh random secrets; never commit it
docker compose -f deploy/docker-compose.yml --profile allinone up -d --build
docker compose -f deploy/docker-compose.yml logs olcf | grep -i password    # the generated first-administrator password (shown once)
```

* `allinone`: one container, SQLite, an embedded Spark worker; published on `127.0.0.1:8470` only.
* `cluster`: control plane with built-in TLS, PostgreSQL, MinIO (S3), and two workers that enrol themselves with single-use tokens
  minted by a one-shot `enroll` service.
* `tls` (add to either): a Caddy reverse proxy with automatic or internal-CA certificates. `deploy/nginx/nginx.conf` is an equivalent
  nginx configuration if you prefer to run your own.

Every OLCF container runs as uid 10001, with a read-only root filesystem, all capabilities dropped, `no-new-privileges` and a tmpfs for
`/tmp`. State lives only in the named volumes. Secrets come from `.env` or `*_FILE` files and never appear on a command line.
The Spark runtime is installed after the first start (`docker compose exec olcf olcf runtime install`) or baked into the image (below).
The images and the Compose file default to `OLCF_ENVIRONMENT=production`, so the development-signed runtime manifests shipped with OLCF are **refused** at run time until you
trust your own signing key (see [runtime trust](#runtime-trust-in-production)); for a local try-out put `OLCF_ENVIRONMENT=development` in `deploy/.env` (Compose passes it to
every container).

**Baking a Spark runtime into the image** (`BAKE_RUNTIME`, `OLCF_BAKE_RUNTIME` in `.env`; the build is `deploy/docker/bake-runtime.sh` in a throwaway stage, the final image is always
production). Two modes, chosen by whether `RUNTIME_TRUST_DIR` (build argument; a directory inside the build context, default `deploy/runtime-trust`, shipped empty) holds a `*.pub` key:

| Mode | Build | What happens |
| --- | --- | --- |
| Development try-out | `docker build -f deploy/Dockerfile --build-arg BAKE_RUNTIME=olcf-rt-2.0.0 -t olcf:dev .` | The bundled, development-signed manifest is installed with `OLCF_ENVIRONMENT=development` set **for that one install in the build stage only**. The seed is marked as development-signed; a production container logs a warning and does not offer it. Run the container with `OLCF_ENVIRONMENT=development` to use it. |
| Operator-signed (production) | put `ops.pub` and your re-signed `olcf-rt-2.0.0.json` in a directory `my-trust/` of the build context, then `docker build -f deploy/Dockerfile --build-arg RUNTIME_TRUST_DIR=my-trust --build-arg BAKE_RUNTIME=olcf-rt-2.0.0.json -t olcf:prod .` | The runtime is installed in production mode with `OLCF_RUNTIME_TRUST_DIR` pointing at your keys, so only your signature is accepted. The keys are copied into the final image at `/etc/olcf/runtime-trust`, and the entrypoint exports `OLCF_RUNTIME_TRUST_DIR` to it (an `OLCF_RUNTIME_TRUST_DIR` you set yourself wins). Only public keys belong there. |

`deploy/Dockerfile.worker` takes the same `RUNTIME_TRUST_DIR` build argument (it bakes no runtime: the control plane installs one on the worker, which then verifies the manifest
against these keys); Compose passes `OLCF_RUNTIME_TRUST_BUILD_DIR` to both builds. A bundled id together with operator keys, or a custom manifest without its key, fails the build with a message
instead of producing an image that refuses its own runtime. Later `olcf runtime install` runs inside a running container use the baked keys too (or a directory you mount and name with `OLCF_RUNTIME_TRUST_DIR`).
**These Dockerfiles, the build-stage scripts and the Compose changes are covered by static checks and by executing the scripts (`bake-runtime.sh`, both entrypoints) with a stub `olcf`, plus one run against the real CLI.**

`docker compose -f deploy/docker-compose.yml --profile cluster config -q` validates the file for a profile without starting anything.

## 3. Kubernetes (Helm chart)

```sh
docker build -f deploy/Dockerfile        -t registry.example.com/olcf:0.1.0 .
docker build -f deploy/Dockerfile.worker -t registry.example.com/olcf-worker:0.1.0 .
docker push registry.example.com/olcf:0.1.0 && docker push registry.example.com/olcf-worker:0.1.0

# lab (bundled single-pod PostgreSQL, no ingress):
helm install olcf deploy/helm/olcf -n olcf --create-namespace \
  --set image.repository=registry.example.com/olcf --set workerImage.repository=registry.example.com/olcf-worker \
  -f deploy/helm/olcf/ci/lab-values.yaml

# production shape: external PostgreSQL, ingress with TLS, 3 control-plane replicas, Kubernetes compute provider
helm install olcf deploy/helm/olcf -n olcf --create-namespace -f my-values.yaml      # start from deploy/helm/olcf/ci/production-values.yaml
helm test olcf -n olcf
```

What the chart creates: control-plane Deployment (replicas, PodDisruptionBudget, probes), Service, optional Ingress, PVC for the data
directory (kept on uninstall), ConfigMap and Secret (keys are generated once and kept across upgrades; or point `secrets.existingSecret`
at your own), optional bundled PostgreSQL, a worker StatefulSet with a headless Service, NetworkPolicies, ServiceAccount and the RBAC the
Kubernetes compute provider needs (secrets, services, pods and pod logs in the compute namespace), and a post-install/upgrade hook that mints one
single-use enrolment token per worker pod into a Secret. All values are validated by `values.schema.json`. Pods satisfy the **restricted**
Pod Security Standard (the Kubernetes provider's job pods too: non-root uid 10001, no privilege escalation, no capabilities, `RuntimeDefault` seccomp).

The Spark port ranges are one value, `spark.ports.*` (defaults 7077-7092 master, 18080-18095 master UI, 41200-41299 worker daemons, 41300-41399
worker UI, 4040-4056 driver UI, 41000-41099 driver, 41100-41199 block manager). The same value configures the control plane
(`OLCF_COMPUTE__*`) and the NetworkPolicy, which allows the control plane to reach the workers and the workers to reach each other on those ranges and nothing else
([ports](../docs/reference/ports-and-networking.md#helm-sparkports-and-the-network-policy)). Job pods of the Kubernetes provider: the job Secret is mounted read-only at
`/opt/olcf/input`, the runner stages it into a writable `emptyDir` at `/opt/olcf/work`; a pod that ends without a result fails with `result_missing`, and log gaps are
reported explicitly ([Kubernetes](../docs/operations/kubernetes.md#the-kubernetes-compute-provider)).

Operational notes:

* More than one control-plane replica needs PostgreSQL and a `ReadWriteMany` volume (the built-in CA lives in the data directory). Install
  with one replica first, then scale up, to avoid replicas initialising the volume at the same time.
* Set `controlPlane.publicUrl` to the external https URL; workers connect there (`workers.insecureInCluster=true` is for labs only).
* To re-enrol a worker, delete its `<pod>.token` key from the `<release>-enroll` Secret and run `helm upgrade`.
* `helm uninstall` keeps the data PVC and the Secrets (they hold your keys); delete them by hand when you are sure.
* Livy / Fabric providers: a submission whose response was lost is reconciled by name (`olcf-<job>-a<attempt>`) and ends as `submission_adopted`, `submission_not_created` (retried) or `submission_unresolved` (not retried automatically); see [Kubernetes](../docs/operations/kubernetes.md#livy-and-fabric-remote-spark-over-http).

### Runtime trust in production

The chart sets `OLCF_ENVIRONMENT=production`. A production installation trusts only the runtime-signing keys in a trust directory you provide; the development key whose private
half is in the source tree is not trusted, so the shipped manifests must be re-signed with your own key (`python -m olcf.runtime keygen|trust|sign`, see [hardening](../docs/operations/hardening.md#6-supply-chain)).
Give the pods the public keys with one value:

```sh
kubectl -n olcf create configmap olcf-runtime-trust --from-file=ops.pub=./ops.pub        # public keys only; a Secret works the same way
helm upgrade --install olcf deploy/helm/olcf -n olcf -f my-values.yaml --set runtimeTrust.existingConfigMap=olcf-runtime-trust     # or runtimeTrust.existingSecret=...
```

`runtimeTrust.existingSecret` **or** `runtimeTrust.existingConfigMap` (not both; keys are file names ending in `.pub`) is mounted read-only at `runtimeTrust.mountPath`
(default `/etc/olcf/runtime-trust`) in the control-plane pods and in every worker pod, and `OLCF_RUNTIME_TRUST_DIR` is set to that path on both. Nothing is mounted when neither is set.
After rotating a key, restart the pods (`kubectl rollout restart deployment/<release> statefulset/<release>-worker`). Also available for anything else you need in the pods (a corporate CA bundle, a proxy):
`controlPlane.extraEnv|extraVolumes|extraVolumeMounts` and `workers.extraEnv|extraVolumes|extraVolumeMounts`. `olcf health` reports the state as `runtime-trust`.
`runtime-trust` is a critical check: when it is `down` (production that explicitly trusts the public development key, for example `OLCF_RUNTIME_ALLOW_DEV_KEY=1`), `GET /v1/health/ready` answers `503`. The chart's
readiness probe calls that endpoint, so the pod stays running (liveness and startup use `GET /v1/health`, which stays `200`) but is removed from the Service until the configuration is fixed. The container
`HEALTHCHECK` (Docker, Compose) probes liveness only and is unaffected; put a load balancer's health check on `/v1/health/ready`.
The Docker images can also carry the keys themselves (build argument `RUNTIME_TRUST_DIR`, see [Docker Compose](#2-docker-compose)); an `OLCF_RUNTIME_TRUST_DIR` set by the chart wins.

## 4. Run as a service without containers

Install first (route 1). Then:

| Platform | Files | Command |
| --- | --- | --- |
| Linux, hardened system service | `deploy/systemd/olcf.service`, `olcf-worker.service`, `olcf.sysusers`, `olcf.tmpfiles`, `*.env.example` | see the header of each unit |
| Linux/macOS/Windows, per user | generated for you | `olcf service install` / `olcf service uninstall` |
| macOS LaunchAgent template | `deploy/launchd/org.olcf.server.plist`, `org.olcf.worker.plist` | replace the `__PLACEHOLDERS__`, then `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/<file>` |
| Windows Task Scheduler | `deploy/windows/install-service.ps1`, `olcf-server-task.xml` | `.\install-service.ps1 -Role server` (no admin rights) |

macOS substitution example:

```sh
sed -e "s#__PREFIX__#$HOME/.local/share/olcf#g" -e "s#__DATA_DIR__#$HOME/.olcf#g" -e "s#__HOME__#$HOME#g" \
  deploy/launchd/org.olcf.server.plist > ~/Library/LaunchAgents/org.olcf.server.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/org.olcf.server.plist
```

Windows has no built-in service host for a Python program that needs no extra software, so OLCF uses a per-user scheduled task (restart on
failure, start at logon). For an unattended Windows server, run OLCF in Docker or on Linux.

## 5. Air-gapped install (offline bundle)

On a machine **with** internet (any OS with `sh`, `curl`, `tar` and `uv`; the target platform may differ):

```sh
bash scripts/build_release.sh                                         # produces dist/olcf-<v>-py3-none-any.whl
sh scripts/make_offline_bundle.sh --wheel dist/olcf-0.1.0-py3-none-any.whl --platform macos-arm64 --out olcf-bundle
tar -czf olcf-bundle.tar.gz olcf-bundle
```

Bundle layout: `uv` (static binary), `wheels/` (OLCF and every dependency), `python/<tag>/<archive>` (python-build-standalone, used through
`UV_PYTHON_INSTALL_MIRROR`), optional `runtime/*.olcfrt` (a prebuilt Spark runtime), `install.sh`, `uninstall.sh`, `MANIFEST.sha256`.

On the target (no network is used at all):

```sh
tar -xzf olcf-bundle.tar.gz && (cd olcf-bundle && shasum -a 256 -c MANIFEST.sha256)
sh olcf-bundle/install.sh --offline-bundle olcf-bundle
```

## Secrets and keys

* `OLCF_MASTER_KEY` (base64, 32 bytes) encrypts every stored credential. Losing it makes stored secrets and encrypted backups unreadable: keep
  an offline copy. Generate one with `openssl rand -base64 32`.
* `OLCF_JWT_SIGNING_KEY` signs login tokens. Replicas of one control plane must share both keys (the chart does this for you).
* Prefer `*_FILE` variables or a secret manager over plain environment variables where your platform supports it. Nothing in `deploy/` bakes a
  secret into an image.

## What the tests and checks cover

* `install.sh`, `uninstall.sh`, `make_offline_bundle.sh`: executed end to end on Linux, including a Mac without developer tools (`/usr/bin` installer
  stubs that fail the run if executed) and a fully offline install with an unreachable network proxy.
* `docker-compose.yml`: `docker compose config` validates each profile.
* The Helm chart: rendered by `deploy/scripts/render_chart.py`, a small Go-template subset interpreter written for this repository (it supports `add`, `add1` and `sub` for the port
  ranges); every rendered object is parsed as YAML, the values are checked against `values.schema.json`, and the tests assert that the NetworkPolicy covers the first and last port
  of every Spark range and that the ConfigMap round-trips into the control plane's settings. The test cases that need `kubeconform` and `helm` run where those tools are installed,
  and CI (`container.yml`) is configured to run `helm lint` and `helm template`. Before you deploy, run `helm lint`, `helm template` and `kubeconform` against your own values,
  and check the NetworkPolicy against your cluster's CNI.
* The Kubernetes provider's pod layout and completion contract: protocol tests against a Kubernetes API server and kubelet written for the tests.
* `entrypoint.sh`, the healthcheck and the systemd/launchd/Task Scheduler files: syntax and structure checked by `tests/tooling`.
* `install.ps1`, `uninstall.ps1` and `windows/install-service.ps1`: reviewed by reading and structure-checked. The systemd units are checked with `systemd-analyze verify` in CI.
