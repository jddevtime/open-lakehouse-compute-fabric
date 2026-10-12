# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf-worker`` command line (CONTRACTS section 11).

``olcf-worker run --url URL [--token T] [--name N] [--state-dir D] [--ca-pem FILE] [--labels k=v,k=v] [--pool P] [--embedded]``
    Enrol (first start, needs ``--url`` and a single-use token) and run the agent until SIGINT/SIGTERM. With an identity in the state
    directory the token is optional (and ignored unless ``--re-enroll``). The token may also come from ``$OLCF_WORKER_TOKEN`` (preferred:
    command lines are visible to other users). Exit code 0 on clean shutdown, 2 on configuration/enrolment errors.
    More options: ``--config FILE`` (TOML, default ``<state-dir>/agent.toml``), ``--max-jobs/--max-cores/--max-memory-mb`` (advertised capacity),
    ``--advertise-host``, ``--sandbox-command "bwrap ..."`` (see :mod:`olcf.worker.sandbox`), ``--runtime-trust-dir``, ``--maven-repository`` (repeatable),
    ``--pip-index-url``, ``--pip-find-links``, ``--stop-children-on-exit``, ``--insecure-ws``, ``--json-logs``, ``--log-level``, ``--enroll-only``.

``olcf-worker status [--state-dir D] [--json]``
    What this node is doing: identity (redacted), whether the agent runs, connection state, jobs, Spark daemons, kernels and runtimes. Reads
    only local state files (``agent-status.json`` is refreshed by the agent every heartbeat).

``olcf-worker leave [--state-dir D] [--yes] [--keep-running-jobs] [--purge] [--purge-runtimes]``
    Take this machine out of the fleet: stop the agent, stop the job processes and Spark daemons it started, delete its identity (token,
    certificate, private key). Without ``--yes`` it only prints what it would do. The controller has no "leave" endpoint: afterwards revoke the
    node in the control plane (the command prints the worker id) so its token and certificate are invalidated server-side.
"""

from __future__ import annotations

import argparse
import contextlib
import faulthandler
import json
import logging
import os
import shlex
import signal
import sys
from pathlib import Path
from typing import Any

import psutil

from olcf.core.errors import OlcfError
from olcf.core.logging import configure_logging
from olcf.runtime.manager import ManagerConfig, RuntimeManager
from olcf.version import WORKER_PROTOCOL_VERSION, __version__
from olcf.worker import identity, inventory, procutil
from olcf.worker.agent import Agent, wipe_state
from olcf.worker.config import DEFAULT_STATE_DIR, AgentConfig, parse_labels

log = logging.getLogger("olcf.worker")


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="olcf-worker", description="OLCF worker agent")
    ap.add_argument("--version", action="version", version=f"olcf-worker {__version__} (protocol {WORKER_PROTOCOL_VERSION})")
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="enrol (if needed) and run the agent")
    run.add_argument("--url", default=os.environ.get("OLCF_WORKER_URL"), help="control plane base URL, e.g. https://olcf.example.com")
    run.add_argument("--token", default=None, help="single-use enrolment token (or $OLCF_WORKER_TOKEN)")
    run.add_argument("--name", default=None, help="node name (default: hostname)")
    run.add_argument("--state-dir", default=None, help=f"agent state directory (default {DEFAULT_STATE_DIR})")
    run.add_argument("--ca-pem", default=None, help="CA certificate (PEM) to pin for https/wss")
    run.add_argument("--labels", default=None, help="node labels k=v,k=v")
    run.add_argument("--pool", default=None)
    run.add_argument("--embedded", action="store_true", help="run as the control plane's embedded worker (stops children on exit)")
    run.add_argument("--config", default=None, help="TOML file with agent settings (default <state-dir>/agent.toml)")
    run.add_argument("--max-jobs", type=int, default=None)
    run.add_argument("--max-cores", type=int, default=None)
    run.add_argument("--max-memory-mb", type=int, default=None)
    run.add_argument("--advertise-host", default=None)
    run.add_argument("--sandbox-command", default=None, help='command prefix wrapped around jobs, e.g. "bwrap --unshare-net ..."')
    run.add_argument("--runtime-trust-dir", default=None, help="directory of trusted runtime-signing public keys (*.pub)")
    run.add_argument("--maven-repository", action="append", default=None, help="Maven repository URL (repeatable)")
    run.add_argument("--pip-index-url", default=None)
    run.add_argument("--pip-find-links", action="append", default=None)
    run.add_argument("--stop-children-on-exit", dest="stop_children", action="store_true", default=None)
    run.add_argument("--keep-daemons-on-exit", dest="keep_daemons", action="store_true", default=None,
                     help="leave Spark master/worker JVMs running when the agent exits (rolling restarts; the next agent adopts them). Default: stop them")
    run.add_argument("--no-host-python", dest="no_host_python", action="store_true", default=None,
                     help="do not offer the built-in olcf-host-python runtime (Python jobs on this machine's own interpreter, no Spark)")
    run.add_argument("--insecure-ws", action="store_true", default=None, help="allow an unencrypted ws:// channel to a remote controller (lab only)")
    run.add_argument("--re-enroll", action="store_true", help="enrol again even if an identity exists (needs a new token)")
    run.add_argument("--enroll-only", action="store_true", help="enrol, write the identity and exit")
    run.add_argument("--daemon-memory", default=None, help="heap of Spark master/worker JVMs, default 512m")
    run.add_argument("--json-logs", action="store_true")
    run.add_argument("--log-level", default=None)

    st = sub.add_parser("status", help="show this node's state")
    st.add_argument("--state-dir", default=None)
    st.add_argument("--json", action="store_true")

    lv = sub.add_parser("leave", help="take this machine out of the fleet")
    lv.add_argument("--state-dir", default=None)
    lv.add_argument("--yes", action="store_true", help="really do it (default: only print the plan)")
    lv.add_argument("--keep-running-jobs", action="store_true", help="do not terminate job processes and daemons")
    lv.add_argument("--purge", action="store_true", help="also delete work directories, logs, daemon/kernel dirs and caches")
    lv.add_argument("--purge-runtimes", action="store_true", help="also delete installed runtimes (implies --purge)")
    lv.add_argument("--stop-only", action="store_true", help="only stop the running agent; keep the identity")
    return ap


def _state_dir(arg: str | None) -> Path:
    return Path(arg or os.environ.get("OLCF_WORKER_STATE_DIR") or DEFAULT_STATE_DIR).expanduser().resolve()


def _config_from_args(a: argparse.Namespace) -> AgentConfig:
    sd = _state_dir(a.state_dir)
    cfg_file = Path(a.config).expanduser() if a.config else sd / "agent.toml"
    overrides: dict[str, Any] = {
        "url": a.url, "token": a.token or os.environ.get("OLCF_WORKER_TOKEN"), "name": a.name, "state_dir": sd, "ca_pem": a.ca_pem, "pool": a.pool,
        "embedded": True if a.embedded else None, "max_jobs": a.max_jobs, "max_cores": a.max_cores, "max_memory_mb": a.max_memory_mb, "advertise_host": a.advertise_host,
        "runtime_trust_dir": a.runtime_trust_dir, "pip_index_url": a.pip_index_url, "stop_children_on_exit": a.stop_children, "keep_daemons_on_exit": True if a.keep_daemons else None, "insecure_ws": a.insecure_ws,
        "daemon_memory": a.daemon_memory, "log_level": a.log_level, "host_python": False if a.no_host_python else None,
    }
    if a.labels is not None:
        overrides["labels"] = parse_labels(a.labels)
    if a.sandbox_command:
        overrides["sandbox_command"] = shlex.split(a.sandbox_command)
    if a.maven_repository:
        overrides["maven_repositories"] = list(a.maven_repository)
    if a.pip_find_links:
        overrides["pip_find_links"] = list(a.pip_find_links)
    return AgentConfig.from_file(cfg_file, **overrides).resolve()


def _arm_hang_dump() -> None:
    """Diagnostics: ``OLCF_WORKER_FAULTHANDLER_SECONDS=N`` writes the stack of every thread to stderr once, N seconds after start (finds where a start-up hangs)."""
    raw = os.environ.get("OLCF_WORKER_FAULTHANDLER_SECONDS", "").strip()
    if not raw:
        return
    try:
        seconds = float(raw)
    except ValueError:
        return
    if seconds > 0:
        faulthandler.dump_traceback_later(seconds, repeat=False, file=sys.stderr)


def cmd_run(a: argparse.Namespace) -> int:
    try:
        cfg = _config_from_args(a)
    except (ValueError, OSError) as exc:
        print(f"olcf-worker: invalid configuration: {exc}", file=sys.stderr)
        return 2
    cfg.ensure_dirs()
    configure_logging(cfg.log_level, json_logs=a.json_logs, log_dir=cfg.path("logs"), name="agent")
    _arm_hang_dump()
    ident = identity.load_identity(cfg.state_dir)
    try:
        if ident is None or a.re_enroll:
            inv = inventory.collect_inventory(cfg.state_dir, labels=cfg.labels, pool=cfg.pool, advertise_host=cfg.advertise_host)
            ident = identity.enroll(cfg, inv, attempts=5)
            cfg.token = None
            os.environ.pop("OLCF_WORKER_TOKEN", None)
        elif cfg.token:
            log.info("this node is already enrolled as %s; the --token is ignored (use --re-enroll to enrol again)", ident.worker_id)
        if a.enroll_only:
            print(json.dumps(ident.redacted(), indent=2))
            return 0
        assert ident is not None
        return Agent(cfg, ident).run()
    except OlcfError as exc:
        print(f"olcf-worker: {exc.message}" + (f"\n  -> {exc.remediation}" if getattr(exc, "remediation", None) else ""), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 0


# ------------------------------------------------------------------------------------------------ status
def _read_json(p: Path) -> Any:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def collect_status(state_dir: Path) -> dict[str, Any]:
    ident = identity.load_identity(state_dir)
    pf = procutil.read_pidfile(state_dir / "agent.pid")
    proc = procutil.process_matches(pf.get("pid"), pf.get("create_time")) if pf else None
    live = _read_json(state_dir / "agent-status.json") or {}
    st: dict[str, Any] = {
        "state_dir": str(state_dir), "agent_version": __version__, "protocol": WORKER_PROTOCOL_VERSION, "enrolled": ident is not None, "identity": ident.redacted() if ident else None,
        "agent_running": proc is not None, "agent_pid": proc.pid if proc else None, "connected": bool(live.get("connected")) and proc is not None,
        "last_hello": live.get("last_hello"), "last_error": live.get("last_error"), "draining": live.get("draining"), "clock_skew_seconds": live.get("clock_skew_seconds"),
        "credentials": {**(live.get("credentials") or {}), "certificate": identity.cert_info(state_dir)},
    }
    jobs: list[dict[str, Any]] = []
    for d in sorted((state_dir / "work").glob("*/agent-job.json")) if (state_dir / "work").is_dir() else []:
        rec = _read_json(d) or {}
        alive = procutil.process_matches(rec.get("pid"), rec.get("pid_create_time")) is not None if rec.get("state") not in ("succeeded", "failed", "cancelled", "timed_out") else False
        jobs.append({"job_id": rec.get("job_id"), "attempt": rec.get("attempt"), "state": rec.get("state"), "pid": rec.get("pid"), "process_alive": alive, "started_at": rec.get("started_at"),
                     "finished_at": rec.get("finished_at")})
    st["jobs"] = jobs
    daemons: list[dict[str, Any]] = []
    for d in sorted((state_dir / "daemons").glob("*/daemon.json")) if (state_dir / "daemons").is_dir() else []:
        rec = _read_json(d) or {}
        daemons.append({"role": rec.get("role"), "runtime_id": rec.get("runtime_id"), "pid": rec.get("pid"), "url": rec.get("url"), "state": rec.get("state"),
                        "process_alive": procutil.process_matches(rec.get("pid"), rec.get("create_time")) is not None})
    st["daemons"] = daemons
    try:
        st["runtimes"] = RuntimeManager(ManagerConfig(state_dir=state_dir)).list() if (state_dir / "runtimes").is_dir() else []
    except Exception as exc:  # noqa: BLE001
        st["runtimes"] = [{"error": str(exc)}]
    return st


def cmd_status(a: argparse.Namespace) -> int:
    st = collect_status(_state_dir(a.state_dir))
    if a.json:
        print(json.dumps(st, indent=2, default=str))
        return 0
    i = st["identity"]
    print(f"state dir : {st['state_dir']}")
    print(f"enrolled  : {'yes, worker ' + i['worker_id'] + ' (' + i.get('name', '') + ') at ' + i.get('url', '') if i else 'no'}")
    print(f"agent     : {'running, pid ' + str(st['agent_pid']) if st['agent_running'] else 'not running'}" + (", connected to the control plane" if st["connected"] else ""))
    if st["last_error"]:
        print(f"last error: {st['last_error']}")
    cert = (st.get("credentials") or {}).get("certificate")
    if cert:
        print(f"certificate: expires {cert['not_after']} (serial {cert['serial'][:12]})")
    cred_err = (st.get("credentials") or {}).get("last_error")
    if cred_err:
        print(f"renewal   : last attempt failed: {cred_err}")
    if st["draining"]:
        print("scheduling: DRAINING (no new work is accepted)")
    for r in st["runtimes"]:
        print(f"runtime   : {r.get('id', '?')} [{r.get('state', r.get('error'))}]" + (" active" if r.get("active") else ""))
    for d in st["daemons"]:
        print(f"daemon    : spark {d['role']} ({d['runtime_id']}) pid {d['pid']} {d['state']} alive={d['process_alive']} {d['url'] or ''}")
    running = [j for j in st["jobs"] if j["process_alive"]]
    print(f"jobs      : {len(running)} running, {len(st['jobs'])} on disk")
    for j in running:
        print(f"            {j['job_id']} attempt {j['attempt']} pid {j['pid']} since {j['started_at']}")
    return 0 if (st["enrolled"]) else 1


# ------------------------------------------------------------------------------------------------ leave
def _signal_pid(pid: int, create_time: float | None, *, grace: float = 30.0) -> bool:
    p = procutil.process_matches(pid, create_time)
    if p is None:
        return True
    with contextlib.suppress(psutil.Error):
        p.terminate() if procutil.IS_WINDOWS else os.kill(pid, signal.SIGTERM)
    return procutil.wait_pid(pid, create_time, timeout=grace)


def cmd_leave(a: argparse.Namespace) -> int:
    sd = _state_dir(a.state_dir)
    st = collect_status(sd)
    plan: list[str] = []
    if st["agent_running"]:
        plan.append(f"stop the agent (pid {st['agent_pid']})")
    if not a.stop_only and not a.keep_running_jobs:
        plan += [f"terminate job {j['job_id']} (pid {j['pid']})" for j in st["jobs"] if j["process_alive"]]
        plan += [f"stop Spark {d['role']} of runtime {d['runtime_id']} (pid {d['pid']})" for d in st["daemons"] if d["process_alive"]]
    if not a.stop_only:
        plan.append("delete identity.json, client.key, client.crt, ca.pem")
        if a.purge or a.purge_runtimes:
            plan.append("delete work/, logs/, daemons/, kernels/, cache/, diagnostics/" + (" and runtimes/" if a.purge_runtimes else ""))
    if not plan:
        print("nothing to do")
        return 0
    print("plan:\n  - " + "\n  - ".join(plan))
    if not a.yes:
        print("\n(dry run: pass --yes to execute)")
        return 0
    if st["agent_running"]:
        pf = procutil.read_pidfile(sd / "agent.pid") or {}
        if not _signal_pid(int(pf["pid"]), pf.get("create_time")):
            print("the agent did not stop within 30 s", file=sys.stderr)
            return 1
    if not a.stop_only:
        if not a.keep_running_jobs:
            for d in sorted((sd / "work").glob("*/agent-job.json")) if (sd / "work").is_dir() else []:
                rec = _read_json(d) or {}
                if rec.get("pid"):
                    procutil.terminate_tree(int(rec["pid"]), grace=10.0, create_time=rec.get("pid_create_time"))
            for d in sorted((sd / "daemons").glob("*/daemon.json")) if (sd / "daemons").is_dir() else []:
                rec = _read_json(d) or {}
                if rec.get("pid"):
                    procutil.terminate_tree(int(rec["pid"]), grace=10.0, create_time=rec.get("create_time"))
        identity.delete_identity(sd)
        if a.purge or a.purge_runtimes:
            wipe_state(sd, keep_runtimes=not a.purge_runtimes)
        print(f"left the fleet. Revoke worker {st['identity']['worker_id'] if st['identity'] else '(unknown)'} in the control plane to invalidate its token and certificate.")
    return 0


def main(argv: list[str] | None = None) -> int:
    a = _build_parser().parse_args(argv)
    return {"run": cmd_run, "status": cmd_status, "leave": cmd_leave}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
