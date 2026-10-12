# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Compute-plane commands: ``worker``, ``compute``, ``runtime``, ``fabric``, ``sample``, ``mcp``.

``runtime install|verify|repair|retire|rollback|export-bundle|import-bundle`` operate on *this machine* (the embedded worker's runtime
store, through :mod:`olcf.runtime`) and therefore work without a running server; add ``--remote`` to ask the control plane to do the same on
workers instead.  Nothing here executes ``java`` / ``python3`` from ``/usr/bin`` on macOS.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import click

from olcf.cli import hostsafe
from olcf.cli.commands.lifecycle import (
    embedded_state_dir,
    find_install_info,
    install_runtime_local,
    local_trust_store,
)
from olcf.cli.common import (
    confirm,
    load_json_arg,
    parse_kv,
    pass_state,
    yes_option,
)
from olcf.cli.state import EXIT_JOB_FAILED, EXIT_TIMEOUT, State


# --------------------------------------------------------------------------------------------- worker
@click.group("worker", help="Workers: machines that run Spark and Python for jobs.")
def worker_group() -> None:
    pass


WORKER_COLS = [("ID", "id"), ("NAME", "name", 24), ("STATE", "state"), ("SCHEDULING", "scheduling"), ("POOL", "pool"), ("CORES", "resources.cores"),
               ("MEM MB", "resources.memory_mb"), ("OS", "os"), ("AGENT", "agent_version"), ("SEEN", "last_seen_at")]


@worker_group.command("list")
@click.option("--state", "st", default=None)
@click.option("--pool", default=None)
@pass_state
def worker_list(state: State, st: str | None, pool: str | None) -> None:
    rows = state.client.compute.list_workers(state=st, pool=pool)
    state.out.table(rows if isinstance(rows, list) else rows.get("items", []), WORKER_COLS, empty="no workers")


@worker_group.command("get")
@click.argument("worker")
@pass_state
def worker_get(state: State, worker: str) -> None:
    state.out.emit(state.client.compute.get_worker(_worker_id(state, worker)), None)


def _worker_id(state: State, ref: str) -> str:
    if ref.startswith("wrk_"):
        return ref
    rows = state.client.compute.list_workers()
    for w in (rows if isinstance(rows, list) else rows.get("items", [])):
        if w.get("name") == ref:
            return w["id"]
    raise click.ClickException(f"no worker named '{ref}' (see `olcf worker list`)")


@worker_group.command("enroll-token", help="Create a one-time token to enroll another machine as a worker.  Shown once.")
@click.option("--name", default=None, help="Label for the token / worker.")
@click.option("--ttl", "ttl_seconds", type=int, default=3600, show_default=True, help="Seconds until the token expires.")
@click.option("--pool", default=None, help="Pool the worker joins.")
@click.option("--label", "labels", multiple=True, help="Worker label k=v (placement); repeatable.")
@pass_state
def worker_enroll_token(state: State, name: str | None, ttl_seconds: int, pool: str | None, labels: tuple[str, ...]) -> None:
    doc = state.client.compute.create_enrollment_token(name=name, ttl_seconds=ttl_seconds, pool=pool, labels={k: str(v) for k, v in parse_kv(labels).items()} or None)
    url = state.client.base_url

    def human(d: dict[str, Any]) -> None:
        o = state.out
        o.success(f"enrollment token created (valid {ttl_seconds // 60} min, single use)")
        print(d.get("token") or d.get("enrollment_token"), file=o.out)
        o.info("\nOn the machine that should become a worker (needs Python 3.11+ or the OLCF installer):")
        o.info(f"  olcf-worker run --url {url} --token <token above>")
        o.info("Then check it with:  olcf worker list")

    state.out.emit(doc, human, reveal=("token", "enrollment_token"))


def _worker_action(name: str, help_: str, with_reason: bool = True) -> None:
    @worker_group.command(name, help=help_)
    @click.argument("worker")
    @click.option("--reason", default="", help="Recorded in the audit log.")
    @pass_state
    def cmd(state: State, worker: str, reason: str) -> None:
        wid = _worker_id(state, worker)
        res = getattr(state.client.compute, name.replace("-", "_"))(wid, reason)
        state.out.emit(res, lambda d: state.out.success(f"{worker}: {d.get('state', name)}"))


_worker_action("cordon", "Stop sending new work to a worker (running jobs continue).")
_worker_action("uncordon", "Allow new work on a cordoned worker again.")
_worker_action("drain", "Cordon, wait for running jobs to finish, then idle.")
_worker_action("revoke", "Permanently revoke a worker's credentials (it must re-enroll).")


@worker_group.command("test", help="Ask the worker to run a self-test (Python, Java, ports, disk, runtime).")
@click.argument("worker")
@pass_state
def worker_test(state: State, worker: str) -> None:
    res = state.client.compute.test_worker(_worker_id(state, worker))
    state.out.emit(res, lambda d: state.out.table(d.get("steps") or d.get("checks") or [d], [("CHECK", "name"), ("OK", "ok"), ("DETAIL", "detail", 70)]))
    if res.get("ok") is False:
        raise SystemExit(1)


@worker_group.command("renew", help="Make a connected worker renew its machine certificate and worker token now (agents also do it on their own inside the renewal window).")
@click.argument("worker")
@click.option("--cert/--no-cert", "certificate", default=True, show_default=True, help="Renew the machine certificate.")
@click.option("--token/--no-token", default=True, show_default=True, help="Rotate the worker token.")
@pass_state
def worker_renew(state: State, worker: str, certificate: bool, token: bool) -> None:
    res = state.client.compute.renew_worker(_worker_id(state, worker), certificate=certificate, token=token)

    def human(d: dict[str, Any]) -> None:
        o, w = state.out, d.get("worker") or {}
        renewed = d.get("renewed") or []
        o.success(f"{worker}: renewed {' and '.join(renewed) if renewed else 'nothing'}")
        cred = w.get("credentials") or {}
        if w.get("cert_expires_at"):
            o.info(f"  certificate expires {w['cert_expires_at']}" + (f" ({cred['cert_days_left']:.0f} days)" if cred.get("cert_days_left") is not None else ""))
        o.info(f"  token {'expires ' + w['token_expires_at'] if w.get('token_expires_at') else 'does not expire'}")
        if cred.get("rotation_pending"):
            o.info(f"  the superseded credentials stay valid until the node reconnects with the new ones (at the latest {cred.get('rotation_grace_until')})")

    state.out.emit(res, human)


@worker_group.command("remove", help="Delete a worker record (it must be offline or revoked).")
@click.argument("worker")
@yes_option
@pass_state
def worker_remove(state: State, worker: str, yes: bool) -> None:
    confirm(state, f"Remove worker '{worker}'?", yes)
    state.client.compute.delete_worker(_worker_id(state, worker))
    state.out.emit({"removed": worker}, lambda d: state.out.success(f"removed {d['removed']}"))


@worker_group.command("label", help="Replace a worker's labels: olcf worker label NAME gpu=true zone=a")
@click.argument("worker")
@click.argument("labels", nargs=-1)
@pass_state
def worker_label(state: State, worker: str, labels: tuple[str, ...]) -> None:
    state.out.emit(state.client.compute.set_worker_labels(_worker_id(state, worker), {k: str(v) for k, v in parse_kv(labels).items()}), None)


@worker_group.group("pool", help="Worker pools.")
def pool_group() -> None:
    pass


@pool_group.command("list")
@pass_state
def pool_list(state: State) -> None:
    rows = state.client.compute.list_pools()
    state.out.table(rows if isinstance(rows, list) else rows.get("items", []), [("NAME", "name"), ("DESCRIPTION", "description", 40), ("WORKERS", "worker_count"), ("QUEUE", "default_queue")])


@pool_group.command("create")
@click.argument("name")
@click.option("--description", default="")
@pass_state
def pool_create(state: State, name: str, description: str) -> None:
    state.out.emit(state.client.compute.create_pool(name=name, description=description), lambda d: state.out.success(f"created pool {name}"))


@pool_group.command("move", help="Put a worker into a pool.")
@click.argument("worker")
@click.argument("pool")
@pass_state
def pool_move(state: State, worker: str, pool: str) -> None:
    state.out.emit(state.client.compute.set_worker_pool(_worker_id(state, worker), pool), lambda d: state.out.success(f"{worker} is now in pool {pool}"))


# --------------------------------------------------------------------------------------------- compute
@click.group("compute", help="Cluster-wide compute: topology, self-tests, queues, clusters.")
def compute_group() -> None:
    pass


@compute_group.command("topology", help="Workers, pools, queues and runtimes at a glance.")
@pass_state
def compute_topology(state: State) -> None:
    topo = state.client.compute.topology()

    def human(t: dict[str, Any]) -> None:
        state.out.info(state.out.style("workers", bold=True))
        s = t.get("summary") or {}
        state.out.kv([("nodes", f"{s.get('nodes_online', 0)} online of {s.get('nodes_total', 0)}"), ("cores", f"{s.get('cores_used', 0)} / {s.get('cores_total', 0)} in use"),
                      ("memory MB", f"{s.get('memory_used_mb', 0)} / {s.get('memory_total_mb', 0)} in use"), ("jobs", f"{s.get('jobs_running', 0)} running, {s.get('jobs_queued', 0)} queued")])
        state.out.info("\n" + state.out.style("nodes", bold=True))
        state.out.table(t.get("nodes") or [], [("NAME", "name", 24), ("STATE", "state"), ("POOL", "pool"), ("CORES", "resources.cores"), ("USED", "resources.cores_allocated"),
                                               ("CPU %", "metrics.cpu_percent"), ("RUNTIMES", "runtimes")], empty="  none yet")
        if t.get("queues"):
            state.out.info("\n" + state.out.style("queues", bold=True))
            state.out.table(t["queues"], [("NAME", "name"), ("QUEUED", "depth"), ("RUNNING", "running"), ("PAUSED", "paused"), ("OLDEST WAIT S", "oldest_wait_seconds")])
        if t.get("clusters"):
            state.out.info("\n" + state.out.style("spark clusters", bold=True))
            state.out.table(t["clusters"], [("ID", "id"), ("STATE", "state"), ("RUNTIME", "runtime_id"), ("MASTER", "master.state")])

    state.out.emit(topo, human)


@compute_group.command("test", help="End-to-end test of scheduling: submit a tiny job to every pool and check it comes back.")
@pass_state
def compute_test(state: State) -> None:
    res = state.client.compute.test()
    state.out.emit(res, lambda d: state.out.table(d.get("steps") or d.get("checks") or [d], [("STEP", "name"), ("OK", "ok"), ("MS", "ms"), ("DETAIL", "detail", 70)]))
    if res.get("ok") is False:
        raise SystemExit(1)


@compute_group.command("smoke", help="Run a real distributed Spark smoke job (needs a ready Spark runtime on an online worker) and report which nodes hosted executors.")
@click.option("--runtime", "runtime_id", default=None, help="Runtime id to use (default: the scheduler's choice).")
@click.option("--partitions", type=int, default=None)
@click.option("--executors", type=int, default=None)
@click.option("--pool", default=None, help="Only use workers of this pool.")
@click.option("--wait/--no-wait", default=True, show_default=True, help="Follow the test to its end (exit code reflects the result) or print its id and return.")
@click.option("--timeout", type=float, default=600.0, show_default=True, help="How long this command waits (seconds), then it exits 9; the test itself keeps running until --server-timeout.")
@click.option("--server-timeout", "server_timeout", type=int, default=None, help="Deadline of the test on the controller, 10-1800 s (default 240); the job is cancelled when it passes.")
@click.option("--status", "status_id", default=None, metavar="ID", help="Show (and with --wait follow) a smoke test that was started earlier.")
@click.option("--cancel", "cancel_id", default=None, metavar="ID", help="Stop a running smoke test.")
@pass_state
def compute_smoke(state: State, runtime_id: str | None, partitions: int | None, executors: int | None, pool: str | None, wait: bool, timeout: float, server_timeout: int | None,
                  status_id: str | None, cancel_id: str | None) -> None:
    c = state.client
    if cancel_id:
        state.out.emit(c.compute.spark_smoke_cancel(cancel_id), lambda d: state.out.info(f"{d['id']}: {d['message']}"))
        return
    if status_id:
        doc = c.compute.spark_smoke_status(status_id)
    else:
        doc = c.compute.spark_smoke(runtime_id=runtime_id, partitions=partitions, executors=executors, timeout_seconds=server_timeout, pool=pool)
        if not wait:
            state.out.emit(doc, lambda d: state.out.info(f"Spark smoke test {d['id']} started (deadline {d.get('deadline_at')}); follow it with: olcf compute smoke --status {d['id']} --wait"))
            return
        state.out.step(f"Spark smoke test {doc['id']} started (deadline {doc.get('deadline_at')})")
    if not doc.get("finished") and (wait or not status_id):
        seen: list[str] = []

        def progress(d: dict[str, Any]) -> None:
            key = f"{d.get('phase')}|{d.get('wait_reason')}"
            if key not in seen:
                seen.append(key)
                state.out.detail(f"{d.get('phase')}: {d.get('message')}")

        progress(doc)
        try:
            doc = c.compute.spark_smoke_wait(doc["id"], timeout=timeout, interval=1.5, on_progress=progress)
        except TimeoutError as exc:
            state.out.error(str(exc), f"olcf compute smoke --status {doc['id']} --wait   (or --cancel {doc['id']})")
            raise SystemExit(EXIT_TIMEOUT) from None  # 9: the client stopped waiting; the test keeps running (the CLI table: 11 means a missing prerequisite)
    state.out.emit(doc, lambda d: _render_smoke(state, d))
    if not doc.get("finished"):
        return
    if doc.get("ok") is not True:
        raise SystemExit(EXIT_JOB_FAILED)


def _render_smoke(state: State, d: dict[str, Any]) -> None:
    out = state.out
    if not d.get("finished"):
        out.info(f"Spark smoke test {d['id']}: {d.get('phase')} - {d.get('message')}")
        return
    verdict = "passed" if d.get("ok") else ("timed out" if d.get("timed_out") else "failed")
    out.info(f"Spark smoke test {d['id']} {verdict}: {d.get('message')}")
    out.kv([("state", d.get("state")), ("runtime", d.get("runtime_id")), ("provider", d.get("provider")), ("master", d.get("master")), ("application", d.get("application_id")),
            ("tasks", d.get("tasks")), ("sum", d.get("sum")), ("distributed", d.get("distributed")), ("elapsed", f"{d.get('elapsed_seconds')} s")])
    nodes = d.get("nodes_with_executors") or []
    if nodes:
        out.table(nodes, [("NODE", "name"), ("HOST", "hostname"), ("CORES", "cores"), ("TASKS", "tasks")])
    if d.get("failure"):
        out.warn(f"failure: {(d['failure'] or {}).get('message', d['failure'])}")
    if d.get("note"):
        out.warn(str(d["note"]))


@compute_group.command("queues")
@pass_state
def compute_queues(state: State) -> None:
    rows = state.client.compute.list_queues()
    state.out.table(rows if isinstance(rows, list) else rows.get("items", []), [("NAME", "name"), ("PAUSED", "paused"), ("PRIORITY", "priority"), ("QUEUED", "queued"), ("RUNNING", "running")])


@compute_group.command("clusters")
@pass_state
def compute_clusters(state: State) -> None:
    rows = state.client.compute.list_clusters()
    state.out.table(rows if isinstance(rows, list) else rows.get("items", []), [("ID", "id"), ("STATE", "state"), ("RUNTIME", "runtime_id"), ("PROVIDER", "provider"), ("MASTER", "master_url", 40)])


# --------------------------------------------------------------------------------------------- runtime
@click.group("runtime", help="Spark runtimes (Python + PySpark + JARs + JDK), installed per machine and verified by a real smoke test.")
def runtime_group() -> None:
    pass


def _manager(state: State, *, with_jdk: bool = False, uv_python: bool = True, index_url: str | None = None, find_links: tuple[str, ...] = ()) -> Any:
    from olcf.runtime.manager import ManagerConfig, RuntimeManager

    return RuntimeManager(ManagerConfig(state_dir=embedded_state_dir(state.data_path()), with_jdk=with_jdk, allow_uv_python_install=uv_python, pip_index_url=index_url,
                                        pip_find_links=list(find_links), trust=local_trust_store(state)))  # host_python stays on: `runtime list`/`verify` show the built-in olcf-host-python


@runtime_group.command("list", help="Runtimes available (bundled manifests) and installed on this machine; --remote asks the server.")
@click.option("--remote", is_flag=True)
@pass_state
def runtime_list(state: State, remote: bool) -> None:
    if remote:
        rows = state.client.runtimes.list()
        state.out.table(rows if isinstance(rows, list) else rows.get("items", []), [("ID", "id"), ("FAMILY", "family"), ("SPARK", "spark"), ("PYTHON", "python"), ("STATE", "state"), ("WORKERS", "worker_count")])
        return
    mgr = _manager(state)
    avail = [{"id": m.id, "family": m.family, "spark": m.spark_version, "python": m.python_series, "java": m.data.get("java", {}).get("major")} for m in mgr.available_manifests()]
    inst = {r["id"]: r for r in mgr.list()}
    for a in avail:
        a["state"] = inst.get(a["id"], {}).get("state", "not installed")
        a["active"] = bool(inst.get(a["id"], {}).get("active"))
    host = inst.get("olcf-host-python")
    if host:  # the built-in Python-only runtime: always there, nothing to install
        avail.append({"id": host["id"], "family": host["family"], "spark": "no Spark", "python": host.get("python"), "java": "-", "state": host["state"], "active": False,
                      "detail": host.get("detail")})
    doc = {"state_dir": str(mgr.cfg.state_dir), "runtimes": avail}
    state.out.emit(doc, lambda _d: state.out.table(avail, [("ID", "id"), ("FAMILY", "family"), ("SPARK", "spark"), ("PYTHON", "python"), ("JAVA", "java"), ("STATE", "state"), ("ACTIVE", "active")]))
    jdks = hostsafe.find_jdks([embedded_state_dir(state.data_path())])
    if not state.out.json_mode:
        state.out.info(state.out.style("JDK: " + (", ".join(f"{j.major} ({j.source})" for j in jdks) or "none found - `olcf runtime install --with-jdk` downloads one"), dim=True))


@runtime_group.command("install", help="Install a runtime.  Downloads (checksum-verified) Python, PySpark, JARs and optionally a JDK, then runs a Spark smoke test.")
@click.argument("runtime", required=False)
@click.option("--with-jdk/--no-jdk", default=False, help="Download Eclipse Temurin when no suitable Java is installed (verified SHA-256).")
@click.option("--force", is_flag=True, help="Reinstall even if verified.")
@click.option("--no-uv-python", is_flag=True, help="Do not let `uv` download a missing Python.")
@click.option("--index-url", default=None, help="Private PyPI mirror.")
@click.option("--find-links", multiple=True, help="Directory/URL of wheels (offline installs).")
@click.option("--bundle", type=click.Path(exists=True, dir_okay=False), default=None, help="Offline bundle from `runtime export-bundle` (no network used).")
@click.option("--remote", is_flag=True, help="Ask the control plane to install on workers instead.")
@click.option("--pool", default=None, help="With --remote: only workers of this pool.")
@click.option("--worker", "workers", multiple=True, help="With --remote: only these workers.")
@pass_state
def runtime_install(state: State, runtime: str | None, with_jdk: bool, force: bool, no_uv_python: bool, index_url: str | None, find_links: tuple[str, ...], bundle: str | None,
                    remote: bool, pool: str | None, workers: tuple[str, ...]) -> None:
    if remote:
        if not runtime:
            raise click.UsageError("--remote needs a runtime id")
        res = state.client.runtimes.install(runtime, worker_ids=[_worker_id(state, w) for w in workers] or None, pool=pool, force=force)
        state.out.emit(res, lambda d: state.out.success("installation requested on the selected workers (watch: olcf runtime list --remote)"))
        return
    msg = install_runtime_local(state, runtime, with_jdk=with_jdk, force=force, index_url=index_url, find_links=find_links, uv_python=not no_uv_python, bundle=bundle)
    state.out.emit({"result": msg, "state_dir": str(embedded_state_dir(state.data_path()))}, lambda d: state.out.success(d["result"]))


def _local_action(name: str, help_: str, **extra: Any) -> None:
    @runtime_group.command(name, help=help_)
    @click.argument("runtime")
    @click.option("--remote", is_flag=True, help="Do it on workers through the control plane.")
    @click.option("--pool", default=None)
    @click.option("--deep", is_flag=True, help="(verify) rerun the Spark smoke test.") if name == "verify" else (lambda f: f)
    @click.option("--purge", is_flag=True, help="(retire) delete the files too.") if name == "retire" else (lambda f: f)
    @pass_state
    def cmd(state: State, runtime: str, remote: bool, pool: str | None, deep: bool = False, purge: bool = False) -> None:
        if remote:
            fn = getattr(state.client.runtimes, name)
            state.out.emit(fn(runtime, pool=pool), lambda d: state.out.success(f"{name} requested on workers"))
            return
        mgr = _manager(state)
        if name == "verify":
            res = mgr.verify(runtime, deep=deep, progress=state.out.progress)
        elif name == "repair":
            res = mgr.repair(runtime, progress=state.out.progress)
        elif name == "retire":
            res = mgr.retire(runtime, purge=purge)
        else:
            res = mgr.rollback(runtime)
        state.out.emit(res, lambda d: state.out.success(f"{name} {runtime}: " + str(d.get("state") or ("ok" if d.get("ok", True) else "failed"))))
        if res.get("ok") is False:
            raise SystemExit(1)


_local_action("verify", "Check an installed runtime (files, hashes, imports; --deep reruns the Spark smoke test).")
_local_action("repair", "Repair a damaged runtime from its recorded lock (transactional).")
_local_action("retire", "Stop offering a runtime to new jobs (--purge deletes it).")
_local_action("rollback", "Re-activate the previous runtime of a family.")


@runtime_group.command("export-bundle", help="Package a runtime (wheels + JARs [+ JDK]) for offline machines.")
@click.argument("runtime")
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), required=True)
@pass_state
def runtime_export(state: State, runtime: str, out_file: str) -> None:
    res = _manager(state).export_bundle(runtime, out_file, progress=state.out.progress)
    state.out.emit(res, lambda d: state.out.success(f"wrote {out_file}"))


@runtime_group.command("import-bundle", help="Install a runtime from an offline bundle (no network).")
@click.argument("bundle", type=click.Path(exists=True, dir_okay=False))
@click.option("--force", is_flag=True)
@pass_state
def runtime_import(state: State, bundle: str, force: bool) -> None:
    res = _manager(state).import_bundle(bundle, progress=state.out.progress, force=force)
    state.out.emit(res, lambda d: state.out.success(f"installed {d.get('runtime_id')}"))


@runtime_group.command("matrix", help="Fabric runtime compatibility matrix known to the server.")
@pass_state
def runtime_matrix(state: State) -> None:
    state.out.emit(state.client.runtimes.matrix(), None)


# --------------------------------------------------------------------------------------------- fabric
@click.group("fabric", help="Microsoft Fabric compute extension: run Fabric notebooks on your own Spark.")
def fabric_group() -> None:
    pass


@fabric_group.command("status", help="Integration settings and the readiness checklist.")
@pass_state
def fabric_status(state: State) -> None:
    doc = state.client.fabric.status()

    def human(d: dict[str, Any]) -> None:
        state.out.kv([(k, v) for k, v in d.items() if not isinstance(v, (dict, list))])
        for key in ("checklist", "readiness", "checks"):
            if d.get(key):
                rows = d[key] if isinstance(d[key], list) else [{"name": k, "ok": v} for k, v in d[key].items()]
                rows = [{**r, "name": r.get("name") or r.get("title") or r.get("id")} if isinstance(r, dict) else r for r in rows]
                state.out.table(rows, [("CHECK", "name"), ("OK", "ok"), ("DETAIL", "detail", 70)])

    state.out.emit(doc, human)


@fabric_group.command("test", help="Stepwise diagnostics: DNS, TLS, proxy, Entra token, Fabric API, OneLake, inbound path.")
@pass_state
def fabric_test(state: State) -> None:
    res = state.client.fabric.test()
    steps = res.get("steps") or []
    state.out.emit(res, lambda d: state.out.table(steps, [("STEP", "name"), ("OK", "ok"), ("MS", "ms"), ("DETAIL", "detail", 60), ("FIX", "remediation", 60)]))
    if any(not s.get("ok") for s in steps):
        raise SystemExit(1)


@fabric_group.command("configure", help="Replace the integration configuration from a JSON file (tenants, audiences, workspace mappings ...).")
@click.argument("file", type=click.Path(dir_okay=False, allow_dash=True))
@pass_state
def fabric_configure(state: State, file: str) -> None:
    state.out.emit(state.client.fabric.configure(load_json_arg(file)), lambda d: state.out.success("configuration saved"))


@fabric_group.command("snippet", help="The cell to paste into a Fabric notebook.")
@click.option("--auth", type=click.Choice(["keyvault", "token", "client_credentials", "notebookutils"]), default="keyvault")
@click.option("--fabric-workspace", default=None, help="Fabric workspace id (shows how it is mapped) or an OLCF workspace name.")
@pass_state
def fabric_snippet(state: State, auth: str, fabric_workspace: str | None) -> None:
    doc = state.client.fabric.snippet(workspace=fabric_workspace, auth=auth)
    code = doc.get("code") or doc.get("snippet")
    if state.out.json_mode or not code:
        state.out.emit(doc, None)
    else:
        print(code, file=state.out.out)
        for note in doc.get("notes") or []:
            state.out.info("# " + str(note))


@fabric_group.command("submissions", help="Notebooks submitted from Fabric.")
@click.option("--state", "st", default=None)
@click.option("--limit", type=int, default=20)
@pass_state
def fabric_submissions(state: State, st: str | None, limit: int) -> None:
    res = state.client.fabric.submissions(limit=limit, state=st)
    rows = res if isinstance(res, list) else res.get("items", [])
    state.out.table(rows, [("ID", "id"), ("STATE", "state"), ("NOTEBOOK", "notebook_name", 28), ("FABRIC WORKSPACE", "fabric_workspace_id"), ("JOB", "job_id"), ("RECEIVED", "received_at")])


@fabric_group.command("analyze", help="Check a notebook for Fabric-only features before moving it (no job is created).")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--format", "fmt", type=click.Choice(["ipynb", "fabric_git_source", "py"]), default=None)
@click.option("--lakehouse", type=click.Choice(["local", "remote"]), default=None, help="What-if: where the default lakehouse lives.")
@pass_state
def fabric_analyze(state: State, file: str, fmt: str | None, lakehouse: str | None) -> None:
    text = Path(file).read_text(encoding="utf-8")
    res = state.client.fabric.analyze(text, format=fmt or {".ipynb": "ipynb", ".py": "py"}.get(Path(file).suffix.lower()), lakehouse=lakehouse)

    def human(d: dict[str, Any]) -> None:
        state.out.kv([(k, v) for k, v in d.items() if not isinstance(v, (dict, list))])
        rows = d.get("findings") or d.get("issues") or []
        state.out.table(rows, [("CLASS", "class"), ("RULE", "rule"), ("CELL", "cell"), ("LINE", "line"), ("MESSAGE", "message", 60), ("FIX", "remediation", 60)], empty="no compatibility findings")

    state.out.emit(res, human)
    if res.get("blockers") or res.get("compatible") is False:
        raise SystemExit(7)


# --------------------------------------------------------------------------------------------- sample
@click.group("sample", help="The built-in sample workspace (data, notebooks, pipeline, SQL).")
def sample_group() -> None:
    pass


@sample_group.command("install")
@click.option("--reinstall", is_flag=True)
@click.option("--size", type=click.Choice(["small", "standard", "large"]), default="standard")
@pass_state
def sample_install(state: State, reinstall: bool, size: str) -> None:
    res = state.client.sample.install(reinstall=reinstall, size=size)
    state.out.emit(res, lambda d: state.out.success(f"sample workspace ready: {d.get('workspace', d.get('workspace_name', 'sample'))}"))


@sample_group.command("status")
@pass_state
def sample_status(state: State) -> None:
    state.out.emit(state.client.sample.status(), lambda d: state.out.kv([(k, v) for k, v in d.items() if not isinstance(v, (dict, list))]))


# --------------------------------------------------------------------------------------------- mcp
@click.group("mcp", help="The AI-agent (MCP) endpoint: Claude Desktop, Claude Code and other MCP clients.")
def mcp_group() -> None:
    pass


@mcp_group.command("info", help="What the endpoint offers (tools, resources, prompts) and recent calls.")
@pass_state
def mcp_info(state: State) -> None:
    state.out.emit(state.client.mcp.info(), None)


def launcher_command() -> list[str]:
    """Absolute command that starts the CLI, stable across upgrades when installed by the installer."""
    info = find_install_info()
    if info and info.get("prefix"):
        exe = Path(info["prefix"]) / "current" / "venv" / ("Scripts" if os.name == "nt" else "bin") / ("olcf.exe" if os.name == "nt" else "olcf")
        if exe.exists():
            return [str(exe)]
    return [sys.executable, "-m", "olcf.cli.main"]


@mcp_group.command("config", help="Print a ready-to-paste client configuration with a freshly created, narrowly scoped token.")
@click.option("--client", "client_kind", type=click.Choice(["claude-desktop", "claude-code", "generic"]), default="generic", show_default=True)
@click.option("--write", "allow_write", is_flag=True, help="Allow the agent to submit and cancel jobs (default: read-only + SQL).")
@click.option("--days", type=int, default=90, show_default=True, help="Token lifetime.")
@click.option("--name", default=None, help="Token name (default mcp-<client>).")
@pass_state
def mcp_config(state: State, client_kind: str, allow_write: bool, days: int, name: str | None) -> None:
    c = state.client
    scope = "mcp:write" if allow_write else "mcp"
    tok = c.identity.create_token(name or f"mcp-{client_kind}", scopes=[scope], ttl_days=days)
    url = c.base_url.rstrip("/") + "/mcp"
    ca = state.resolve_connection().get("ca_bundle")
    secret = tok["token"]
    if client_kind == "claude-desktop":
        env = {"OLCF_MCP_URL": url, "OLCF_MCP_TOKEN": secret, **({"SSL_CERT_FILE": ca} if ca else {})}
        cmd = launcher_command()
        cfg: Any = {"mcpServers": {"olcf": {"command": cmd[0], "args": [*cmd[1:], "mcp", "stdio"], "env": env}}}
        hint = "Merge this into claude_desktop_config.json (Claude Desktop > Settings > Developer > Edit Config), then restart Claude Desktop."
    elif client_kind == "claude-code":
        cfg = {"mcpServers": {"olcf": {"type": "http", "url": url, "headers": {"Authorization": f"Bearer {secret}"}}}}
        hint = f"Or add it with:  claude mcp add --transport http olcf {url} --header \"Authorization: Bearer {secret}\""
    else:
        cfg = {"url": url, "transport": "streamable-http", "headers": {"Authorization": f"Bearer {secret}"}, "stdio_command": [*launcher_command(), "mcp", "stdio"],
               "stdio_env": {"OLCF_MCP_URL": url, "OLCF_MCP_TOKEN": secret}}
        hint = "Use the URL + header for clients with Streamable HTTP, or the stdio command (with the two environment variables) for stdio-only clients."
    doc = {"client": client_kind, "scope": scope, "token_id": tok.get("id"), "expires_at": tok.get("expires_at"), "config": cfg, "note": hint}

    def human(d: dict[str, Any]) -> None:
        print(json.dumps(d["config"], indent=2), file=state.out.out)
        state.out.info(f"\nToken {d['token_id']} (scope {d['scope']}, expires {d['expires_at']}) is inside the configuration above; revoke it any time with `olcf token revoke {d['token_id']}`.")
        state.out.info(d["note"])

    state.out.emit(doc, human, reveal=("token", "Authorization", "OLCF_MCP_TOKEN", "config"))


@mcp_group.command("stdio", help="Bridge for stdio-only MCP clients (reads OLCF_MCP_URL / OLCF_MCP_TOKEN, falls back to the current context).")
@click.option("--url", default=None)
@click.option("--verbose", "bridge_verbose", is_flag=True)
@pass_state
def mcp_stdio(state: State, url: str | None, bridge_verbose: bool) -> None:
    from olcf.mcp import stdio

    conn = state.resolve_connection()
    target = url or os.environ.get("OLCF_MCP_URL") or conn["url"].rstrip("/") + "/mcp"
    if not os.environ.get("OLCF_MCP_TOKEN") and conn.get("token"):
        os.environ["OLCF_MCP_TOKEN"] = conn["token"]
    argv = ["--url", target]
    if conn.get("ca_bundle"):
        argv += ["--ca-file", conn["ca_bundle"]]
    if bridge_verbose:
        argv.append("--verbose")
    raise SystemExit(stdio.main(argv))


def register(group: click.Group) -> None:
    for cmd in (worker_group, compute_group, runtime_group, fabric_group, sample_group, mcp_group):
        group.add_command(cmd)
