# Kubernetes

OLCF can run on Kubernetes in two separate ways that are often combined:

1. **The platform runs on Kubernetes.** The Helm chart in `deploy/helm/olcf` deploys the control plane, PostgreSQL (external, or a bundled single pod for labs) and a StatefulSet of OLCF workers that run Spark in local or standalone mode.
2. **Spark jobs run as Kubernetes pods.** The `kubernetes` compute provider starts a driver pod per job and lets Spark create executor pods; OLCF tracks the driver pod, reads its logs and proxies its UI.

> **Test coverage.** The chart is rendered by a small Go-template interpreter written for this repository (`deploy/scripts/render_chart.py`) and the values are checked against `values.schema.json`; the tests assert that the NetworkPolicy opens exactly the Spark port ranges the ConfigMap gives the control plane. The `container.yml` workflow is configured to run `helm lint --strict` and `helm template` and to compare them with the bundled renderer. The Kubernetes provider is covered by protocol tests: a Kubernetes API server and kubelet written for the tests (it runs the real runner, with Secret volumes forced read-only), a `kubectl` stub and a `spark-submit` stub. Before you deploy, run `helm lint`, `helm template` and `kubeconform` against your own values, and check the NetworkPolicy against your cluster's CNI.

## The chart

```sh
docker build -f deploy/Dockerfile        -t registry.example.com/olcf:0.1.0 .
docker build -f deploy/Dockerfile.worker -t registry.example.com/olcf-worker:0.1.0 .
docker push registry.example.com/olcf:0.1.0 && docker push registry.example.com/olcf-worker:0.1.0

# lab: bundled single-pod PostgreSQL, no ingress
helm install olcf deploy/helm/olcf -n olcf --create-namespace \
  --set image.repository=registry.example.com/olcf --set workerImage.repository=registry.example.com/olcf-worker \
  -f deploy/helm/olcf/ci/lab-values.yaml

# production shape: external PostgreSQL, ingress with TLS, three control-plane replicas, Kubernetes provider
helm install olcf deploy/helm/olcf -n olcf --create-namespace -f my-values.yaml   # start from deploy/helm/olcf/ci/production-values.yaml
helm test olcf -n olcf
```

The chart creates: the control-plane Deployment (`controlPlane.replicaCount`, a PodDisruptionBudget, probes), a Service and optional Ingress, a PVC for the data directory (kept on uninstall), a ConfigMap and Secret (keys are generated once and kept across upgrades, or supply `secrets.existingSecret` with `master-key`, `jwt-signing-key` and `admin-password`), optional bundled PostgreSQL, a worker StatefulSet with a headless Service, NetworkPolicies, a ServiceAccount, the RBAC the Kubernetes provider needs, and a post-install and post-upgrade hook that mints one single-use enrollment token per worker pod into a Secret. Pods run as uid 10001, non-root, read-only root file system, all capabilities dropped, seccomp `RuntimeDefault` (the restricted Pod Security Standard).

Values you will set:

| Value | Meaning |
|---|---|
| `controlPlane.publicUrl` | the external https URL. Required in practice: workers connect there, and enrolment commands and OIDC redirects use it (`workers.insecureInCluster=true` is for labs) |
| `controlPlane.replicaCount` | more than 1 needs PostgreSQL and a `ReadWriteMany` volume (the built-in CA lives in `/data/pki`). Install with one replica first, then scale up |
| `controlPlane.trustedProxies` | reverse-proxy hops to trust for `X-Forwarded-*` (1 for an ingress controller) |
| `database.external.url` / `existingSecret` | `postgresql+psycopg://user:pass@host:5432/olcf` |
| `ingress.*` | host, class, TLS secret; allow large bodies and long read timeouts (`proxy-body-size: 512m`, `proxy-read-timeout: "3600"` on ingress-nginx) because of uploads and WebSockets |
| `workers.replicas`, `workers.pool`, `workers.labels`, `workers.maxJobs`, `workers.resources` | the worker StatefulSet; `workers.advertiseHost` defaults to the pod IP, which is what executors use to reach the driver |
| `spark.ports.<role>.{start,end}` | the Spark port ranges (`masterRpc`, `masterUi`, `workerRpc`, `workerUi`, `driverUi`, `driver`, `blockManager`), the single source for the ConfigMap and the NetworkPolicy; defaults 7077-7092, 18080-18095, 41200-41299, 41300-41399, 4040-4056, 41000-41099, 41100-41199 ([ports](../reference/ports-and-networking.md#helm-sparkports-and-the-network-policy)) |
| `networkPolicy.*` | defaults allow ingress from anywhere and all egress; tighten with `allowIngressFromAnywhere=false`, `ingressFrom`, `monitoring.ingressFrom`, `egressAllowAll=false` (then DNS, the database, HTTPS, the Kubernetes API server when `kubernetesProvider.enabled`, the workers' Spark ranges, `monitoring.egressTo` and `egress.extra`); `workerIngressFrom` admits an external driver to the Spark ports |
| `kubernetesProvider.enabled`, `.namespace` | create RBAC for the Kubernetes compute provider in that namespace |

To re-enroll a worker, delete its `<pod>.token` key from the `<release>-enroll` Secret and run `helm upgrade`. `helm uninstall` keeps the data PVC and the Secrets because they hold your keys; delete them by hand when you are sure.

The control-plane readiness probe calls `GET /v1/health/ready`; liveness and startup call `GET /v1/health`. A production release that explicitly trusts the public development runtime key (`OLCF_RUNTIME_ALLOW_DEV_KEY=1` through `controlPlane.extraEnv`) therefore stays `Running` but `0/1 Ready` and receives no traffic (readiness `503`) until you remove that setting or add `runtimeTrust.existingSecret|existingConfigMap`; `olcf health` (or `GET /v1/admin/health`) shows the `runtime-trust` reason.

Install the Spark runtime on the workers after the first start (`olcf runtime install --remote`). **In production this needs a trust anchor**: the chart sets `OLCF_ENVIRONMENT=production`, so the development-signed manifests shipped with OLCF are refused ("not trusted"). Sign the manifests with your own key ([hardening, section 6](hardening.md#6-supply-chain)) and give the control plane and the workers a trust directory with its public key through the chart: create a ConfigMap (or Secret) whose keys are file names ending in `.pub` and set `runtimeTrust.existingConfigMap` (or `runtimeTrust.existingSecret`; not both). The chart mounts it read-only at `runtimeTrust.mountPath` (default `/etc/olcf/runtime-trust`) in the control-plane pods and in every worker pod and sets `OLCF_RUNTIME_TRUST_DIR` on both; with neither value set nothing is mounted. Restart the pods after rotating a key. For other needs (a corporate CA bundle, proxy variables) there are `controlPlane.extraEnv|extraVolumes|extraVolumeMounts` and `workers.extraEnv|extraVolumes|extraVolumeMounts`. Images built with the `RUNTIME_TRUST_DIR` build argument carry the keys themselves ([deploy README](../../deploy/README.md#2-docker-compose)). `BAKE_RUNTIME` (Dockerfile build argument) installs a runtime during the image build: with only the bundled id it is a development-signed try-out that a production pod does not offer; for production bake an operator-signed manifest with `RUNTIME_TRUST_DIR`. The chart templates are tested by rendering with and without these values; they were not installed on a cluster.

## The Kubernetes compute provider

With the provider enabled, a job whose provider is `kubernetes` runs its Spark driver and executors as pods. By default (`submit_mode: driver_pod`) the controller creates a Secret that holds the job specification and source (including the job's resolved storage credentials and its derived callback token), a headless Service for the driver and a driver Pod that runs the OLCF runner inside the runtime image in Spark client mode against `k8s://https://kubernetes.default.svc`; Spark creates the executor pods, which are owned by the driver pod and removed with it. The alternative `spark_submit` mode runs `spark-submit --deploy-mode cluster`; its entrypoint is expected to call the runner with `--workdir`, which is not tested. The provider needs a service account allowed to manage pods, services, secrets and pod logs in the compute namespace (the chart creates it), an image containing the runtime venv with `olcf` installed (`--no-deps`), and network reachability between the pods and the control plane. Cluster access uses the `kubernetes` Python client when installed, otherwise `kubectl`. Spark settings the platform owns (`spark.kubernetes.*`, master, authentication) cannot be set by user code. See [compute model](../concepts/compute-model.md#providers).

### Pod layout and secret staging

| Mount | Kind | Mode | Purpose |
|---|---|---|---|
| `/opt/olcf/input` | Secret `input` (the job's Secret, `job.json` and the source) | read-only, file mode `0440` | what the runner reads. Kubernetes always mounts Secret volumes read-only |
| `/opt/olcf/work` | `emptyDir` `work` (optional size limit) | read-write | the runner's working directory: `--workdir` copies the input here with owner-only permissions, writes `result.json`, logs and artifacts, and **deletes the staged `job.json`** when the run ends |
| `/tmp` | `emptyDir` | read-write | scratch space, so `read_only_root_filesystem: true` is possible (an option of the provider, off by default) |

Command: `python -m olcf.runtime_lib.runner --workdir /opt/olcf/work /opt/olcf/input/job.json` (`runner_command` in the provider configuration substitutes `{input_dir}` and `{work_dir}`). The pod runs as a non-root user (`runAsNonRoot`, `runAsUser` 10001 by default, `fsGroup`, no privilege escalation, all capabilities dropped, `RuntimeDefault` seccomp). The Secret and the headless Service are deleted when the attempt is cleaned up or cancelled (the pod itself is kept for log retrieval until retention); the Secret holds credentials, so keep read access to Secrets in the compute namespace narrow. Check on your cluster: file ownership under a non-root UID and `fsGroup`, `readOnlyRootFilesystem: true`, and the image contract.

### Completion, `result_missing` and log gaps

The pod's result reaches the controller by the **runner contract**: after the runner persisted `result.json` it uploads the declared artifacts, prints one `@@OLCF@@` `result` line followed by `runner_finished`, and writes a compact record (at most 3.5 KiB) to `/dev/termination-log`. The controller never reports success for a pod that ended without delivering the result: it re-reads the retained log and the termination record for `compute.remote_result_grace_seconds` (120) and then fails the attempt with `failure.details.code = "result_missing"` (category `provider`). Because the work may have run, such an attempt is retried only when the job is `idempotent`; before running it again read the pod's log and check the target. Details: [compute model](../concepts/compute-model.md#remote-providers-and-the-result-contract).

Logs are read incrementally through a timestamp cursor and drained before the job finishes (`compute.remote_log_pages_per_poll`, `compute.remote_drain_timeout_seconds`). If the kubelet rotated or truncated the container log and the cursor's position is gone, or a burst of output exceeded the read window, the job log shows an explicit gap note and the job gets a `log_gap` event; lines in a gap are lost, the result is not (it travels in the `result` event and the termination record). A job that writes many megabytes within a single second makes each page re-read the burst (up to a 64 MiB window), which is quadratic in the burst size; keep chatty output below that or log to artifacts.

### Livy and Fabric (remote Spark over HTTP)

The `external` provider with `livy_url`, and the `fabric` provider, talk to a Livy server. Sessions and batches are named `olcf-<job_id>-a<attempt>`. A request whose response was lost is never repeated blindly: the attempt is reconciled by name, with these outcomes in the job's events and failure codes: `submission_adopted` (the object exists, the job carries on), `submission_not_created` (absent for the quiesce window, default 60 s: failed and retried), `submission_unresolved` (unknown after 900 s: failed, **not** retried automatically; check the Livy server and run the job again yourself). The two windows are the provider configuration keys `submission_quiesce_seconds` and `submission_reconcile_seconds`. Batches take `source.main_class` and `source.args`, and the job's validated Spark configuration is sent as the session or batch `conf`. This logic is covered by protocol tests of the Livy API.

## Running without Helm

Compose profiles `allinone` and `cluster` in `deploy/docker-compose.yml` give a single-host equivalent (control plane, PostgreSQL, MinIO, two workers, optional Caddy); `deploy/README.md` documents them. Kubernetes manifests for the control plane are otherwise not provided.
