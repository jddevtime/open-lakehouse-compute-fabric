# Running a cluster and the Compute control panel

This guide is the operator's view of compute: adding machines, installing runtimes, grouping workers, watching and draining them, and running a Spark cluster. The model (providers, scheduler, job lifecycle) is in the [compute model](../concepts/compute-model.md); the networking in [ports and networking](../reference/ports-and-networking.md).

## One machine is already a cluster

`olcf serve` starts the API, the scheduler and, by default, an **embedded worker** on the same machine. Nothing else is needed for a laptop or a single server.

* **Without a Spark runtime** the worker already offers the built-in `olcf-host-python` runtime (the worker's own Python, no Spark). Python-engine notebooks and jobs, SQL on DuckDB, dashboards and the AI-agent endpoint work; Spark notebooks, Spark SQL and Spark jobs wait in the queue with the reason `no_runtime` ([why a job is queued](../concepts/compute-model.md#why-is-my-job-still-queued)).
* **With a Spark runtime** installed, Spark jobs run on the same machine in local mode or in a one-machine Spark Standalone cluster.

```bash
olcf runtime install --with-jdk        # downloads, verifies and smoke-tests a Spark runtime
olcf compute smoke                     # a real Spark job: driver, executors, shuffle, Delta write
```

`olcf compute smoke` submits a Spark job, so it needs a ready Spark runtime on an online worker. Without one the request is refused at once with `409 no_spark_runtime` (exit code 6) and a remediation; nothing is queued. The test is asynchronous: the command starts it (`202` with an id and a poll URL), follows it until it ends and prints which nodes hosted executors. See [Self-tests](#self-tests) for the options and exit codes.

## The Compute page

UI: **Operate, Compute**. Five tabs:

| Tab | Shows | Does |
|---|---|---|
| Machines | every worker as a card or a table: state (healthy, degraded, down), pool, scheduling (active, cordoned, draining), cores and memory in use, disk, CPU, component lights (agent link, Java, Python, Spark daemons), installed runtimes, last heartbeat | Add a machine; per machine: test connectivity, cordon, uncordon, drain, edit labels, move to a pool, delete the node record, revoke access |
| Spark clusters | standalone clusters per runtime family: master address, workers, running applications and where their executors run | open the Spark UI through the authenticated proxy; stop a cluster |
| Control plane | the API replicas, which one is the leader, versions, uptime | |
| Queues and pools | queues (priority, paused, queued and running counts, oldest wait) and worker pools | create, pause and resume queues; create pools |
| Runtimes | installed runtimes per machine, with state and verification (the same cards as the [Runtimes page](#the-runtimes-page)) | install, verify, repair, retire, roll back on selected machines |

On a Machines card or table row the **Runtimes** badges show each runtime reported by that machine: green when `ready` and verified, red otherwise; the built-in runtime is labelled `host-python` and its tooltip says "Python only, no Spark". The Spark clusters tab is empty until the first Spark job. **Start cluster** is disabled, and the empty state offers **Install a runtime** instead of **Run the smoke test**, until some machine has a ready, verified runtime *with* Spark (the host-Python runtime does not count). On the Home page the checklist step "Install a Spark runtime (optional)" is likewise ticked only by a runtime with Spark.

Reservation: every node keeps `compute.reserve_cores` (default 1) cores free for the system and the agent. The setting is live: changing it under **Administration, Settings** (or `olcf settings set compute.reserve_cores 0`) takes effect within a few seconds without a restart; it accepts whole numbers from 0 to 64.

Everything has a CLI equivalent: `olcf compute topology|queues|clusters|smoke|test`, `olcf worker ...`, `olcf runtime ...`.

## Adding a machine

A machine joins by running the worker agent, which connects **outward** to the control plane; it needs no inbound firewall rules to the control plane, but the machines of one Spark cluster must reach each other ([ports](../reference/ports-and-networking.md)). Requirements: Python 3.11 or newer, and for Spark a JDK 17 or 21 (the runtime installer can fetch one).

In the UI choose **Add a machine**, give a node name, pool, labels and the token lifetime (10 minutes, 1 hour or 24 hours), and copy the command for Linux, macOS, Windows or Docker. The dialog waits and shows the machine when it connects. On the command line:

```bash
# on the control plane (needs compute.manage)
olcf worker enroll-token --name spark-node-3 --pool batch --label site=berlin --ttl 3600

# on the new machine
python3 -m venv ~/.olcf-worker-venv
~/.olcf-worker-venv/bin/pip install olcf
~/.olcf-worker-venv/bin/olcf-worker run --url https://olcf.example.com --token olcf_enr_... \
    --name spark-node-3 --pool batch --labels site=berlin
```

Use the installer script or an offline bundle instead of `pip install olcf` when the package is not on an index you can reach ([deploy README](../../deploy/README.md)). With a private CA add `--ca-pem ca.crt`.

Enrollment exchanges the single-use token for a worker token and, with mTLS, a client certificate generated from a key that never leaves the machine ([TLS and mTLS](tls-and-mtls.md)). The agent then keeps a WebSocket open, reports its inventory every few seconds, and runs whatever the scheduler sends. Useful agent options:

| Option | Meaning |
|---|---|
| `--max-cores`, `--max-memory-mb`, `--max-jobs` | advertise less than the machine has |
| `--advertise-host` | the address other nodes use to reach this one (Spark executors and the driver); set it when the host name does not resolve elsewhere |
| `--state-dir` | where the agent keeps state, default `~/.olcf-worker` |
| `--sandbox-command` | wrap every job in a sandbox ([security model](../concepts/security-model.md#code-execution)) |
| `--runtime-trust-dir` | directory of trusted runtime-signing public keys |
| `--keep-daemons-on-exit` / `--stop-children-on-exit` | what happens when the agent stops. By default the Spark master and worker JVMs the agent started are stopped (a bounded wait, then their process groups are killed) and running job processes are left to be adopted by the next agent, except for the embedded worker, which stops them. `--keep-daemons-on-exit` leaves the JVMs running for a rolling restart; `--stop-children-on-exit` also stops job processes. SIGTERM, SIGINT and SIGHUP (SIGBREAK on Windows) all run this graceful shutdown |
| `--no-host-python` | do not offer the built-in `olcf-host-python` runtime on this machine (also `host_python = false` in `agent.toml`) |
| `--maven-repository`, `--pip-index-url`, `--pip-find-links` | package sources for runtime installs on this machine |
| `--daemon-memory` | heap of the Spark master and worker JVMs (default `512m`) |
| `--config` | TOML file with the same settings (default `<state-dir>/agent.toml`) |

Run the agent as a service: `deploy/systemd/olcf-worker.service` (Linux), `deploy/launchd/org.olcf.worker.plist` (macOS), or `deploy/windows/install-service.ps1 -Role worker` (Windows scheduled task).

On the machine itself, `olcf-worker status` shows its state and `olcf-worker leave` takes it out of the fleet. After it connects, `olcf worker test <worker>` asks the agent to run a self-test.

## Runtimes

A runtime is a complete Python, PySpark, jar and optional JDK environment. Install it where jobs will run:

```bash
olcf runtime list                         # catalogue, with state on this machine
olcf runtime install olcf-rt-2.0.0 --remote --pool batch    # ask the control plane to install on workers
olcf runtime verify olcf-rt-2.0.0 --deep --remote           # files, hashes, imports and a Spark smoke test
olcf runtime matrix                       # the compatibility matrix
olcf runtime rollback olcf-rt-2.0.0       # re-activate the previous runtime of the family
olcf runtime retire olcf-rt-2.0.0 --purge
```

Installs download and verify checksummed Python packages, jars and (with `--with-jdk`) a Temurin JDK; they take minutes and need free disk space on the machine (the project did not measure the installed size). For networks without Internet: `olcf runtime export-bundle` and `import-bundle`, or `--index-url` and `--find-links` ([runbook](../operations/runbook.md#air-gapped-sites)). Manifests are signed; read [hardening](../operations/hardening.md#6-supply-chain) before production.

### The Runtimes page

UI: **Operate, Runtimes**, tab *Catalog* (the other tabs are *Compatibility* and *Offline bundles*). Each runtime is a card with its title, id, family, version facts, a signed or unsigned badge and "Installed on N of M nodes", which expands into a per-node table (state, verified, fingerprint, detail, last update).

The built-in runtime appears as a card titled **Host Python (no Spark)** with the id `olcf-host-python` and the family `host-python`. It differs from the others in what the card shows:

* a green **Built in** badge ("Part of every worker: nothing to install, verify or retire") instead of the Install, Verify and More-actions buttons, and no Signed or Unsigned badge;
* a warning badge **No Spark** ("Runs Python only: Spark, Spark SQL and Delta on Spark need a Spark runtime"), and a Python badge with the worker's Python version, but no Spark, Delta, Iceberg, Java or Scala badge;
* the line reads "Available on N of M nodes" (not "Installed on"), or "Not offered by any online node yet" before a worker has reported it. In the expanded table each node's *Detail* is one line such as `Python 3.13.16 (CPython) on linux-x86_64; duckdb 1.5.6, deltalake 1.6.6, pyarrow 25.0.1, pandas 3.0.6, matplotlib 3.11.2; no Spark`, and its fingerprint is the node's own ([compute model](../concepts/compute-model.md#the-host-python-runtime-no-spark)). A node whose Python lacks `ipykernel`, `jupyter_client` or `nbformat` shows the runtime as failed with that module's name.

The same entry is in `olcf runtime list` (`SPARK` column `no Spark`; add `--remote` for the controller's catalog), `GET /v1/runtimes` (`builtin: true`, `no_spark: true`, `engines: ["python"]`) and is left out of the compatibility matrix.

### The job page while a job waits

A job that is queued shows a banner at the top of its page (**Operate, Jobs**, then the job), refreshed every two seconds:

* once the scheduler has evaluated it, an amber banner **"Waiting in the queue: <Reason>"** (for example *No runtime*, *No node*, *Resources*, *Quota*, *Paused*), the sentence for this job as its body, and a line **What to do:** with the generic fix for the reason;
* before that (and for jobs that only have the plain text), a blue banner "Waiting in the queue" with the raw wait reason.

For a job whose runtime is `olcf-host-python`, in any state, a second blue banner reads **"Running on the host Python runtime"**: "This job uses the worker's own Python (no Spark). Install a Spark runtime to use PySpark or Spark SQL." The *Runtime* tile shows the runtime id, and the *Timeline* tab lists the `runtime_fallback` event (when a Python-engine job was moved to the host runtime because the Spark runtime it would have used is not installed) and one `queue_reason` event per change of reason, each with its sentence. A failed job shows its failure panel as before.

## Pools, labels and placement

* **Pools** group workers (`default` always exists): `olcf worker pool create batch`, `olcf worker pool move <worker> batch`. A job can ask for a pool (`olcf job submit --pool batch`).
* **Labels** are free `key=value` pairs (`olcf worker label <worker> gpu=true zone=a`; the command replaces all labels). A job can require labels, a node or a provider through `policy.placement` (`pool`, `labels` where all must match, `node`, `provider`) in its submission; the CLI offers `--pool` only.
* **Queues** have a priority and can be paused. `olcf job submit --queue <name> --priority 0-100`. Aging adds priority to waiting jobs (`compute.aging_interval_seconds`).

## Maintenance

| Goal | Command |
|---|---|
| Stop sending new work, let running jobs finish | `olcf worker cordon <worker> --reason "kernel update"`; `olcf worker uncordon <worker>` |
| Cordon, wait for running jobs to finish, then release the node | `olcf worker drain <worker>` |
| A machine is gone for good | `olcf worker revoke <worker>` then `olcf worker remove <worker>` (the node must be offline or revoked) |
| A worker needs a new identity | revoke it, then enroll again with a new token |
| Move load between pools | `olcf worker pool move` |

A job on a worker that disappears waits `compute.worker_lost_grace_seconds` (180 s) for it to return before it is failed and the retry rules apply.

## Spark clusters

With the default provider (`compute.default_provider = auto`) the scheduler starts a Spark Standalone master and workers on demand when an eligible node can host them, and otherwise falls back to local mode with the reason recorded in the job. Clusters appear under **Spark clusters** and in `olcf compute clusters`. Executors run on the machines of the pool that fit; the Spark UI of the master and of applications is reachable only through the control plane's authenticated proxy. A job can pin a provider with `policy.placement.provider` in its submission (API and SDK); the default for all jobs is `compute.default_provider` in **Administration, Settings** (`auto`, `standalone`, `local`, or a provider name).

Interactive notebook sessions use `compute.session_provider` (default `local`: the Spark driver lives in the kernel's node). Per-job layers of Python packages exist only on the node that built them, so executors on other nodes cannot import them; put such packages in the runtime ([compute model](../concepts/compute-model.md#runtimes)).

The Spark standalone path is exercised by tests with several agents on one host and by a real Spark job. Use `olcf compute smoke --executors 2` on your hardware before relying on it.

## Self-tests

* `olcf compute test`: submits a tiny job to every pool and checks it returns.
* `olcf compute smoke [--runtime ID] [--partitions N] [--executors N] [--pool NAME] [--wait|--no-wait] [--timeout SECONDS] [--server-timeout SECONDS] [--status ID] [--cancel ID]`: a real Spark job with shuffle and a Delta write (needs a ready Spark runtime on an online worker, see above). It is **asynchronous**: `POST /v1/compute/spark-smoke` (permission `compute.manage`) answers `202` with the job id, a `poll_url` (`GET /v1/compute/spark-smoke/{id}`), `phase`, `wait_reason` and `deadline_at`, and the command polls it. `--server-timeout` (10 to 1800 s, default 240) is the deadline on the controller; when it passes the controller cancels the test and reports `timed_out`. `--timeout` (default 600 s) is only how long this command waits. `--no-wait` prints the id and returns; `--status ID` shows a test started earlier and, with `--wait` (the default), follows it to its end; `--cancel ID` stops one (`POST /v1/compute/spark-smoke/{id}/cancel`, idempotent). Only one smoke test runs at a time.

  | Exit code | Meaning |
  |---|---|
  | 0 | the test passed (also `--no-wait`, `--status ID --no-wait` of a test that is still running, and `--cancel`) |
  | 6 | `409`: `no_spark_runtime` (no online worker has a ready Spark runtime; install one with `olcf runtime install --remote`) or `smoke_already_running` (the error carries the running id; follow it with `--status ID --wait` or stop it with `--cancel ID`) |
  | 9 | this command stopped waiting (`--timeout`), as for every `--wait` in the [CLI exit-code table](cli.md#exit-codes); the test keeps running until `--server-timeout` (follow it with `--status ID --wait`, stop it with `--cancel ID`) |
  | 10 | the test finished and did not pass (failed, or timed out on the controller) |
  | 3, 4, 8 | not authenticated, permission denied, server unreachable |

  It uses the same session bootstrap as every job, so the master is whatever the planner chose: `spark://...` on a Standalone cluster, `local[n]` when it fell back to local mode. The work is spread over at least twice the cores in partitions and every task reports its host, process and executor. The final document carries, besides `ok`, `finished`, `timed_out`, `state`, `message`, `failure`, `master_is_cluster`, `partitions`, `total_cores`, `executors`, `executors_registered`, `single_address` and `nodes_with_executors` (worker, host, address, cores, tasks, executors per node). `distributed` is true only when the master is a cluster and at least two agents hosted executors; `single_address` is true when those agents share one address (one machine) and the note says so; a local fallback gives `distributed: false` with the reason.
* `olcf worker test <worker>` and the **Test connectivity** action: agent link, DNS, TCP, TLS and HTTP to the control plane, disk space and clock skew against the controller.
* `olcf doctor --server`: all server health checks.
