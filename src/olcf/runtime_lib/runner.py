# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job runner: ``<runtime>/venv/bin/python -m olcf.runtime_lib.runner <workdir>/job.json`` (CONTRACTS sections 4.3, 11).

Reads ``job.json``, prepares the environment, builds Spark lazily (:mod:`olcf.runtime_lib.spark`), runs the job by ``kind``:

``notebook`` / ``capsule``   executed cell by cell in a real kernel (:mod:`olcf.runtime_lib.executor`), ``output.ipynb`` written
``spark_job``                a PySpark application file in-process, or a jar through ``spark-submit``
``sql``                      Spark SQL statements; the last result set becomes the exit value
``script``                   a python file in-process (``py``) or a shell script (``script``)

and writes ``result.json`` next to the spec: ``{state, exit_value, failure, spark, artifacts, metrics}``. Progress goes to stdout as
``@@OLCF@@ {...}`` lines. Exit codes: 0 succeeded, 1 failed, 124 timed out, 143 cancelled. Failures are classified with
:class:`olcf.core.errors.FailureCategory` (:mod:`olcf.runtime_lib.failures`).

Cancellation (SIGTERM/SIGINT/SIGBREAK) stops the kernel and Spark session and still writes ``result.json``; the timeout
(``limits.timeout_seconds``) is enforced by a watchdog thread that escalates to a hard exit if the main thread is stuck.

Remote mode (``--workdir DIR`` and/or ``--remote``; Kubernetes pods, where there is no agent to collect ``result.json``)
    ``python -m olcf.runtime_lib.runner --workdir /opt/olcf/work /opt/olcf/input/job.json``. The input directory (a read-only Secret volume) is *staged* into the
    writable ``DIR`` and the job runs there. After ``result.json`` is persisted the runner completes the one remote contract (CONTRACTS, PROV addenda): it uploads the
    declared artifacts (``output.ipynb`` ...) with the job token, emits **exactly one** ``@@OLCF@@ {"event":"result","job_id","attempt","result":{...}}`` line (the result
    document, size-capped at ``limits.result_event_bytes`` with ``result_ref`` pointing at the uploaded complete ``result.json`` when it had to be shortened) and leaves a
    compact copy in the termination log (``OLCF_TERMINATION_LOG``, default ``/dev/termination-log``) as a secondary channel that survives log rotation.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from olcf.core.errors import FailureCategory, Unsupported
from olcf.runtime_lib import bootstrap, failures, paths
from olcf.runtime_lib.bootstrap import emit_event
from olcf.runtime_lib.logbuf import truncate_text

EXIT_OK, EXIT_FAILED, EXIT_TIMEOUT, EXIT_CANCELLED = 0, 1, 124, 143
#: Largest ``result`` event line the runner writes (a log line must stay small enough for every log pipeline); larger results are shortened and referenced.
DEFAULT_RESULT_EVENT_BYTES = 192 * 1024
#: The kubelet keeps at most 4096 bytes of a termination message; stay well below.
TERMINATION_MESSAGE_BYTES = 3584


class JobCancelled(BaseException):  # noqa: N818 - signal-like control flow, must not be swallowed by `except Exception`
    """Raised in the main thread when the agent asks the job to stop (SIGTERM) or the watchdog fires."""

    def __init__(self, reason: str = "cancelled") -> None:
        super().__init__(reason)
        self.reason = reason


class _State:
    """Process-wide run state shared by signal handlers, the watchdog and the main flow."""

    def __init__(self) -> None:
        self.reason: str | None = None  # None | "cancelled" | "timeout"
        self.finalised = False
        self.lock = threading.Lock()
        self.executor: Any = None
        self.spark: Any = None
        self.child: subprocess.Popen[bytes] | None = None
        self.started = time.monotonic()
        self.remote = False  # remote contract: upload artifacts, emit the ``result`` event, write the termination record
        self.staged_spec: Path | None = None


STATE = _State()


# --------------------------------------------------------------------------------------------- result handling
def _write_json_atomic(path: Path, doc: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(doc, default=str), encoding="utf-8")
    os.replace(tmp, path)


def _limit_value(value: Any, max_bytes: int) -> Any:
    """JSON-safe exit value, truncated (visibly) when its encoding exceeds ``max_bytes``."""
    try:
        enc = json.dumps(value, default=str)
    except (TypeError, ValueError):
        enc = json.dumps(str(value))
    if len(enc.encode("utf-8")) <= max_bytes:
        return json.loads(enc)
    preview, _ = truncate_text(enc, max(1024, min(max_bytes, 64 * 1024)))
    return {"_truncated": True, "original_bytes": len(enc.encode("utf-8")), "preview": preview}


def _read_spark_app(workdir: Path) -> dict[str, Any] | None:
    p = workdir / "spark-app.json"
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def _metrics(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    m: dict[str, Any] = {"duration_seconds": round(time.monotonic() - STATE.started, 3), "python": platform.python_version()}
    try:
        import resource

        m["max_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 if sys.platform != "darwin" else 1024 * 1024), 1)
    except ImportError:  # Windows
        pass
    m.update(extra or {})
    return m


def dependency_fingerprint(spec: dict[str, Any]) -> dict[str, Any] | None:
    """What this run executed on: the runtime (id + fingerprint) and the environment layer (layer hash, environment lock hash / id / version) -
    recorded in ``result.json`` and therefore in the job record, so a result can be tied to the exact dependency set."""
    rt = spec.get("runtime") or {}
    if not rt:
        return None
    layer = rt.get("layer") or {}
    out: dict[str, Any] = {"runtime_id": rt.get("id"), "runtime_fingerprint": rt.get("fingerprint")}
    hp = rt.get("host_python")
    if isinstance(hp, dict):  # the worker's own interpreter: record what it was (Python, platform, library versions) next to the fingerprint
        out["host_python"] = {k: hp.get(k) for k in ("python", "version", "packages", "detail", "no_spark") if hp.get(k) is not None}
    if layer or rt.get("environment"):
        env = rt.get("environment") or {}
        out["layer_key"] = layer.get("key")
        out["environment_lock_hash"] = layer.get("lock_hash") or env.get("lock_hash")
        out["environment"] = layer.get("environment") or {k: env.get(k) for k in ("item_id", "name", "version") if env.get(k) is not None} or None
        out["jars"] = [Path(u).name for u in layer.get("jars") or []]
    return out


def finish(workdir: Path, spec: dict[str, Any], *, state: str, exit_value: Any = None, failure: dict[str, Any] | None = None,
           artifacts: list[dict[str, Any]] | None = None, metrics: dict[str, Any] | None = None) -> None:
    """Write ``result.json`` exactly once (idempotent: the watchdog and the main flow may race). In remote mode the result is then delivered (see the module docstring)."""
    with STATE.lock:
        if STATE.finalised:
            return
        STATE.finalised = True
    limits = spec.get("limits") or {}
    maxb = int(limits.get("max_output_bytes") or 50 * 1024 * 1024)
    arts = [dict(a) for a in artifacts or []]
    if STATE.remote:
        _publish_artifacts(workdir, spec, arts)  # before result.json: the persisted document records what was delivered
    doc = {"state": state, "exit_value": _limit_value(exit_value, maxb), "failure": failure, "spark": _read_spark_app(workdir),
           "artifacts": arts, "metrics": _metrics(metrics), "dependencies": dependency_fingerprint(spec)}
    persisted = True
    try:
        _write_json_atomic(workdir / "result.json", doc)
    except OSError as exc:
        if not STATE.remote:
            raise
        persisted = False
        print(f"olcf-runner: cannot persist result.json in {workdir}: {exc}", file=sys.stderr, flush=True)
    if STATE.remote:
        _deliver_result(workdir, spec, doc, persisted=persisted)
    emit_event("runner_finished", state=state, category=(failure or {}).get("category"))


# --------------------------------------------------------------------------------------------- remote completion contract
def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _upload(spec: dict[str, Any], name: str, path: Path, media_type: str | None) -> dict[str, Any]:
    """POST one file to ``/v1/jobs/{id}/artifacts/{name}`` with the job token. Returns ``{size, sha256, stored[, error]}``; never raises."""
    meta: dict[str, Any] = {"size": path.stat().st_size, "sha256": _sha256_file(path)}
    cb = spec.get("callbacks") or {}
    base, token = str(cb.get("api_url") or "").rstrip("/"), cb.get("job_token")
    if not base or not token:
        return {**meta, "stored": False, "error": "the job spec has no callbacks.api_url / job_token to upload to"}
    url = f"{base}/v1/jobs/{urllib.parse.quote(str(spec.get('job_id')), safe='')}/artifacts/{urllib.parse.quote(name, safe='')}"
    last = ""
    for attempt in range(3):
        try:
            with path.open("rb") as fh:
                req = urllib.request.Request(url, data=fh, method="POST", headers={  # noqa: S310 - the controller URL comes from the signed job spec
                    "Authorization": f"Bearer {token}", "Content-Type": media_type or "application/octet-stream", "Content-Length": str(meta["size"])})
                with urllib.request.urlopen(req, timeout=120) as resp:  # noqa: S310
                    resp.read()
            return {**meta, "stored": True}
        except urllib.error.HTTPError as exc:
            last = f"HTTP {exc.code}"
            if exc.code < 500 and exc.code not in (408, 429):
                break  # a refusal (auth, size, name) will not change on retry
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            last = f"{type(exc).__name__}: {exc}"[:200]
        time.sleep(1.5 * (attempt + 1))
    return {**meta, "stored": False, "error": last}


def _publish_artifacts(workdir: Path, spec: dict[str, Any], artifacts: list[dict[str, Any]]) -> None:
    for art in artifacts:
        name, rel = str(art.get("name") or ""), str(art.get("path") or art.get("name") or "")
        p = (workdir / rel).resolve()
        if not name or not p.is_file() or workdir.resolve() not in p.parents:
            art.update({"stored": False, "error": "the file does not exist in the work directory"})
            continue
        art.update(_upload(spec, name, p, art.get("media_type")))


def _shrink_result(doc: dict[str, Any], cap: int) -> dict[str, Any]:
    """A copy of ``doc`` whose JSON encoding fits in ``cap`` bytes: the exit value is cut (visibly, via ``_limit_value``), long failure texts are cut, optional
    sections go last. The caller adds ``result_ref``."""
    out = dict(doc)
    out["exit_value"] = _limit_value(doc.get("exit_value"), max(1024, cap // 3))
    if isinstance(out.get("failure"), dict):
        f = dict(out["failure"])
        for k in ("traceback", "message", "remediation"):
            if isinstance(f.get(k), str) and len(f[k]) > 8192:
                f[k] = truncate_text(f[k], 8192)[0]
        out["failure"] = f
    out["artifacts"] = [{k: a.get(k) for k in ("name", "path", "media_type", "size", "sha256", "stored", "error") if a.get(k) is not None} for a in (doc.get("artifacts") or [])[:64]]
    for key in ("dependencies", "spark", "metrics"):
        if len(json.dumps(out, default=str).encode("utf-8")) <= cap:
            break
        out[key] = None
    return out


def _deliver_result(workdir: Path, spec: dict[str, Any], doc: dict[str, Any], *, persisted: bool) -> None:
    """Emit the single ``result`` event (and the termination record). Runs after ``result.json`` was persisted; must never raise into the finishing path."""
    try:
        cap = int((spec.get("limits") or {}).get("result_event_bytes") or DEFAULT_RESULT_EVENT_BYTES)
        full = json.dumps(doc, default=str).encode("utf-8")
        event_doc: dict[str, Any] = doc
        if len(full) > cap:
            event_doc = _shrink_result(doc, cap)
            ref: dict[str, Any] = {"name": "result.json", "bytes": len(full), "sha256": hashlib.sha256(full).hexdigest(), "complete": False}
            if persisted:
                ref.update({k: v for k, v in _upload(spec, "result.json", workdir / "result.json", "application/json").items() if k in ("stored", "error")})
            event_doc["result_ref"] = ref
        emit_event("result", job_id=spec.get("job_id"), attempt=spec.get("attempt"), result=event_doc)
        _write_termination_record(spec, doc, len(full), hashlib.sha256(full).hexdigest())
    except Exception as exc:  # noqa: BLE001 - delivery problems are reported, the exit status still tells the story
        print(f"olcf-runner: result delivery failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)


def _write_termination_record(spec: dict[str, Any], doc: dict[str, Any], size: int, digest: str) -> None:
    """A compact JSON summary in ``OLCF_TERMINATION_LOG`` (Kubernetes: ``state.terminated.message``, at most 4 KiB). It is a *secondary* channel that does not depend on
    log volume or rotation: enough to know the outcome and the failure; the exit value is included only when it is tiny."""
    path = os.environ.get("OLCF_TERMINATION_LOG", "/dev/termination-log")
    fail = doc.get("failure") if isinstance(doc.get("failure"), dict) else None
    small: dict[str, Any] = {"state": doc.get("state"), "failure": ({k: (v[:600] if isinstance(v, str) else v) for k, v in fail.items() if k in ("category", "code", "message", "remediation")} if fail else None),
                             "artifacts": [str(a.get("name")) for a in (doc.get("artifacts") or [])][:16], "result_bytes": size, "result_sha256": digest}
    ev = doc.get("exit_value")
    if len(json.dumps(ev, default=str)) <= 1024:
        small["exit_value"] = ev
    else:
        small["partial"] = True
    rec = {"v": 1, "job_id": spec.get("job_id"), "attempt": spec.get("attempt"), "result": small}
    text = json.dumps(rec, default=str)
    if len(text.encode("utf-8")) > TERMINATION_MESSAGE_BYTES:
        small.pop("exit_value", None)
        small["partial"] = True
        small["artifacts"] = small["artifacts"][:4]
        if isinstance(small.get("failure"), dict):
            small["failure"]["message"] = str(small["failure"].get("message", ""))[:300]
            small["failure"].pop("remediation", None)
        text = json.dumps(rec, default=str)
    try:
        with open(path, "w", encoding="utf-8") as fh:  # noqa: PTH123
            fh.write(text)
    except OSError:
        pass  # not running under a kubelet (or the path is not writable): the event is the primary channel


def _cleanup_sensitive(workdir: Path) -> None:
    for name in ("kernel-spec.json", "kernel-spec.connection.json"):
        try:
            (workdir / name).unlink()
        except OSError:
            pass
    if STATE.staged_spec is not None:  # remote mode: the staged copy of job.json holds the job token and secrets; the input volume stays with the pod
        try:
            STATE.staged_spec.unlink()
        except OSError:
            pass


# --------------------------------------------------------------------------------------------- signals + watchdog
def _install_signals() -> None:
    def handler(signum: int, frame: Any) -> None:  # noqa: ARG001
        if STATE.reason is None:
            STATE.reason = "cancelled"
        raise JobCancelled(STATE.reason)

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):  # not main thread / unsupported
                pass


def _start_watchdog(workdir: Path, spec: dict[str, Any], timeout: float | None) -> None:
    if not timeout:
        return

    def watch() -> None:
        deadline = STATE.started + timeout
        while time.monotonic() < deadline:
            if STATE.finalised:
                return
            time.sleep(0.5)
        if STATE.finalised:
            return
        STATE.reason = "timeout"
        emit_event("timeout", seconds=timeout)
        ex = STATE.executor
        if ex is not None:
            ex.stop()
        child = STATE.child
        if child is not None and child.poll() is None:
            child.terminate()
        try:
            signal.raise_signal(signal.SIGTERM)
        except (ValueError, OSError):
            pass
        time.sleep(8)  # grace for the main thread to unwind (stop kernel / spark)
        if not STATE.finalised:
            finish(workdir, spec, state="failed", failure=failures.failure_dict(
                FailureCategory.TIMEOUT.value, f"job exceeded limits.timeout_seconds={timeout:g}", remediation="Increase limits.timeout_seconds or optimise the job."))
            _cleanup_sensitive(workdir)
            os._exit(EXIT_TIMEOUT)

    threading.Thread(target=watch, name="olcf-watchdog", daemon=True).start()


# --------------------------------------------------------------------------------------------- kinds
def _source_path(workdir: Path, spec: dict[str, Any]) -> Path:
    src = spec.get("source") or {}
    rel = src.get("path")
    if not rel:
        raise Unsupported("job spec has no source.path", remediation="CTRL must write the source next to job.json and reference it in source.path.")
    p = (workdir / rel).resolve()
    if not str(p).startswith(str(workdir.resolve()) + os.sep):
        raise Unsupported(f"source.path {rel!r} escapes the job directory")
    if not p.is_file():
        raise Unsupported(f"source file {rel!r} is missing in the job directory")
    return p


def _run_notebook(workdir: Path, spec: dict[str, Any]) -> tuple[str, Any, dict[str, Any] | None, list[dict[str, Any]], dict[str, Any]]:
    from olcf.runtime_lib import executor

    src = _source_path(workdir, spec)
    limits = spec.get("limits") or {}
    nb = executor.load_notebook(src, (spec.get("source") or {}).get("format"))
    ks = workdir / "kernel-spec.json"
    fd = os.open(ks, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump({"spec": spec, "options": {"workdir": str(workdir), "eager_spark": False}}, fh)
    cell_timeout = limits.get("cell_timeout_seconds")
    ex = executor.NotebookExecutor(spec, workdir, ks, max_output_bytes=int(limits.get("max_output_bytes") or 50 * 1024 * 1024),
                                   cell_timeout=float(cell_timeout) if cell_timeout else None)
    STATE.executor = ex
    try:
        res = ex.run(nb, spec.get("parameters") or {})
    finally:
        STATE.executor = None
    out_path = workdir / "output.ipynb"
    executor.write_notebook(res.notebook, out_path)
    artifacts = [{"name": "output.ipynb", "path": "output.ipynb", "media_type": "application/x-ipynb+json"}]
    return res.state, res.exit_value, res.failure, artifacts, {"cells_executed": res.cells_executed, "exit_called": res.exit_called,
                                                              "output_bytes": ex.budget.used, "output_truncated": ex.budget.truncated}


def _lazy_spark(spec: dict[str, Any]) -> Any:
    spark = bootstrap.LazySpark(spec)
    STATE.spark = spark
    return spark


def _run_python_file(workdir: Path, spec: dict[str, Any], *, args: list[str]) -> tuple[Any, bool]:
    """Execute a python file in-process with ``spark``/``notebookutils`` predefined. Returns (exit_value, exit_called)."""
    src = _source_path(workdir, spec)
    spark = _lazy_spark(spec)
    ns: dict[str, Any] = {"__name__": "__main__", "__file__": str(src), "__builtins__": __builtins__}
    ns.update(bootstrap.build_namespace(spec, spark))
    params = spec.get("parameters") or {}
    ns["PARAMETERS"] = params
    for k, v in params.items():
        if isinstance(k, str) and k.isidentifier():
            ns.setdefault(k, v)
    sys.argv = [str(src), *args]
    sys.path.insert(0, str(src.parent))
    code = compile(src.read_text(encoding="utf-8"), str(src), "exec")
    try:
        exec(code, ns)  # noqa: S102 - running user code is the job
    except bootstrap.NotebookExit as ex:
        return ex.value, True
    return ns.get("EXIT_VALUE"), False


def _run_shell(workdir: Path, spec: dict[str, Any]) -> None:
    src = _source_path(workdir, spec)
    if os.name == "nt":
        raise Unsupported("shell scripts are not supported on Windows workers", remediation="Use a python script (format py).")
    sh = shutil.which("bash") or shutil.which("sh")
    if not sh:
        raise Unsupported("no shell found on this worker")
    proc = subprocess.Popen([sh, str(src)], cwd=workdir)  # noqa: S603
    STATE.child = proc
    rc = proc.wait()
    STATE.child = None
    if rc != 0:
        raise RuntimeError(f"script exited with status {rc}")


def _run_jar(workdir: Path, spec: dict[str, Any]) -> None:
    """``spark-submit`` for jar applications; the Spark app id is scraped from its log output for the result."""
    from olcf.runtime_lib import spark as sp

    src = _source_path(workdir, spec)
    home = os.environ.get("SPARK_HOME")
    if not home:
        raise Unsupported("SPARK_HOME is not set; jar jobs need a runtime with a Spark distribution")
    exe = Path(home) / "bin" / ("spark-submit.cmd" if os.name == "nt" else "spark-submit")
    lock, manifest = sp.load_runtime_files()
    conf = sp.build_spark_conf(spec, lock=lock, manifest=manifest, runtime_root=sp.runtime_dir())
    sd = spec.get("source") or {}
    cmd = [str(exe)]
    if not sd.get("main_class"):
        raise Unsupported("jar jobs need source.main_class")
    cmd += ["--class", str(sd["main_class"]), "--master", conf.pop("spark.master"), "--name", conf.pop("spark.app.name")]
    for k, v in conf.items():
        cmd += ["--conf", f"{k}={v}"]
    cmd += [str(src), *[str(a) for a in (sd.get("args") or spec.get("args") or [])]]
    proc = subprocess.Popen(cmd, cwd=workdir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)  # noqa: S603
    STATE.child = proc
    app_re = re.compile(rb"(application_\d+_\d+|app-\d{14}-\d{4})")
    announced = False
    assert proc.stdout is not None
    for line in iter(proc.stdout.readline, b""):
        sys.stdout.buffer.write(line)
        sys.stdout.buffer.flush()
        if not announced:
            m = app_re.search(line)
            if m:
                announced = True
                info = {"application_id": m.group(1).decode(), "ui_url": None, "app_name": spec.get("name"), "master": spec.get("spark", {}).get("master")}
                (workdir / "spark-app.json").write_text(json.dumps(info))
                emit_event("spark_app_started", **info)
    rc = proc.wait()
    STATE.child = None
    if rc != 0:
        raise RuntimeError(f"spark-submit exited with status {rc}")


def _jsonable_rows(df: Any, limit: int) -> dict[str, Any]:
    rows = df.limit(limit + 1).collect()
    truncated = len(rows) > limit
    rows = rows[:limit]
    return {"columns": df.columns, "rows": [[_cell(v) for v in r] for r in rows], "row_count": len(rows), "truncated": truncated}


def _cell(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, (list, tuple)):
        return [_cell(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _cell(x) for k, x in v.items()}
    if hasattr(v, "asDict"):
        return {k: _cell(x) for k, x in v.asDict().items()}
    return str(v)


def _run_sql(workdir: Path, spec: dict[str, Any]) -> Any:
    from olcf.runtime_lib import magics

    src = _source_path(workdir, spec)
    spark = _lazy_spark(spec)
    spark.materialize()
    text = src.read_text(encoding="utf-8")
    params = spec.get("parameters") or {}
    last = None
    for stmt in magics.split_statements(text):
        last = spark.sql(stmt, args=params) if params and re.search(r":[A-Za-z_]\w*", stmt) else spark.sql(stmt)
    limit = int((spec.get("limits") or {}).get("max_rows") or 1000)
    if last is None or not last.columns:
        return None
    return _jsonable_rows(last, limit)


# --------------------------------------------------------------------------------------------- main
def run_job(workdir: Path, spec: dict[str, Any]) -> int:
    kind = spec.get("kind", "notebook")
    src_fmt = (spec.get("source") or {}).get("format", "ipynb")
    emit_event("runner_started", job_id=spec.get("job_id"), attempt=spec.get("attempt"), kind=kind, pid=os.getpid(), python=platform.python_version())
    state, exit_value, failure, artifacts, metrics = "succeeded", None, None, [], {}
    try:
        if kind in ("notebook", "capsule"):
            state, exit_value, failure, artifacts, metrics = _run_notebook(workdir, spec)
        elif kind == "sql":
            exit_value = _run_sql(workdir, spec)
        elif kind == "spark_job":
            if src_fmt == "jar":
                _run_jar(workdir, spec)
            elif src_fmt == "scala":
                raise Unsupported("Scala source jobs are not supported; compile to a jar (format jar)")
            else:
                exit_value, _ = _run_python_file(workdir, spec, args=[str(a) for a in ((spec.get("source") or {}).get("args") or spec.get("args") or [])])
        elif kind == "script":
            if src_fmt == "script":
                _run_shell(workdir, spec)
            else:
                exit_value, _ = _run_python_file(workdir, spec, args=[])
        else:
            raise Unsupported(f"unknown job kind {kind!r}")
    except JobCancelled as jc:
        STATE.reason = STATE.reason or jc.reason
        cat = FailureCategory.TIMEOUT if STATE.reason == "timeout" else FailureCategory.CANCELLATION
        failure = failures.failure_dict(cat.value, "job exceeded its timeout" if cat is FailureCategory.TIMEOUT else "job cancelled",
                                        remediation="Increase limits.timeout_seconds or optimise the job." if cat is FailureCategory.TIMEOUT else None)
        state = "failed"
    except SystemExit as se:
        if se.code not in (0, None):
            state, failure = "failed", failures.failure_dict(FailureCategory.USER_CODE.value, f"script called sys.exit({se.code!r})",
                                                                 remediation="Return normally (or exit with status 0) when the job succeeded.")
    except Exception as exc:  # noqa: BLE001 - every failure must be classified
        state, failure = "failed", failures.failure_from_exception(exc)
    finally:
        _stop_spark()
    if state == "succeeded" and failure is None and STATE.reason == "timeout":
        state, failure = "failed", failures.failure_dict(FailureCategory.TIMEOUT.value, "job exceeded its timeout")
    finish(workdir, spec, state=state, exit_value=exit_value, failure=failure, artifacts=artifacts, metrics=metrics)
    _cleanup_sensitive(workdir)
    if state == "succeeded":
        return EXIT_OK
    cat = (failure or {}).get("category")
    return EXIT_TIMEOUT if cat == FailureCategory.TIMEOUT.value else EXIT_CANCELLED if cat == FailureCategory.CANCELLATION.value else EXIT_FAILED


def _stop_spark() -> None:
    sp = STATE.spark
    if sp is not None and getattr(sp, "materialized", False):
        try:
            sp.stop()
        except BaseException:  # noqa: BLE001, S110 - best effort at shutdown, including a second SIGTERM
            pass


def stage_inputs(src_dir: Path, dest: Path) -> Path:
    """Copy the (read-only) input volume into the writable work directory, owner-only. Entries the kubelet adds to Secret volumes (``..data``, ``..<timestamp>``) are skipped;
    symbolic links are followed, so the files end up as ordinary files. Returns ``dest``."""
    dest.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(src_dir, followlinks=True):
        dirs[:] = [d for d in dirs if not d.startswith("..")]
        out_dir = dest / Path(root).relative_to(src_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        for name in files:
            if name.startswith(".."):
                continue
            target = out_dir / name
            shutil.copyfile(Path(root) / name, target)
            os.chmod(target, 0o600)
    return dest


def _parse_args(argv: list[str]) -> tuple[Path | None, bool, list[str]]:
    workdir: Path | None = None
    remote = False
    rest: list[str] = []
    it = iter(argv)
    for a in it:
        if a == "--workdir":
            nxt = next(it, None)
            if nxt is None:
                raise ValueError("--workdir needs a directory")
            workdir = Path(nxt)
        elif a.startswith("--workdir="):
            workdir = Path(a.split("=", 1)[1])
        elif a == "--remote":
            remote = True
        else:
            rest.append(a)
    return workdir, remote or workdir is not None, rest


def _early_failure(workdir: Path, message: str) -> int:
    """No readable job.json: still write a result (and, in remote mode, deliver it) so the controller does not have to guess."""
    spec = {"job_id": os.environ.get("OLCF_JOB_ID"), "attempt": int(os.environ.get("OLCF_ATTEMPT") or 0) or None, "limits": {}}
    try:
        finish(workdir, spec, state="failed", failure=failures.failure_dict(FailureCategory.INTERNAL.value, message))
    except OSError:
        pass
    return EXIT_FAILED


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    usage = "usage: python -m olcf.runtime_lib.runner [--workdir DIR] [--remote] <input-dir>/job.json"
    try:
        workdir_arg, remote, rest = _parse_args(argv)
    except ValueError as exc:
        print(f"{exc}\n{usage}", file=sys.stderr)
        return 2
    if len(rest) != 1:
        print(usage, file=sys.stderr)
        return 2
    job_path = Path(rest[0]).resolve()
    workdir = job_path.parent
    STATE.remote = remote
    if workdir_arg is not None and workdir_arg.resolve() != workdir:
        src_dir, workdir = workdir, workdir_arg.resolve()
        try:
            stage_inputs(src_dir, workdir)
        except OSError as exc:
            return _early_failure(workdir, f"cannot stage the job inputs from {src_dir} into {workdir}: {exc}")
        job_path = workdir / job_path.name
        STATE.staged_spec = job_path
    try:
        spec = bootstrap.load_job_spec(str(job_path))
    except (OSError, ValueError) as exc:
        if remote:
            return _early_failure(workdir, f"cannot read job.json: {exc}")
        _write_json_atomic(workdir / "result.json", {"state": "failed", "exit_value": None, "spark": None, "artifacts": [], "metrics": {},
                                                     "failure": failures.failure_dict(FailureCategory.INTERNAL.value, f"cannot read job.json: {exc}")})
        return EXIT_FAILED
    os.environ["OLCF_JOB_DIR"] = str(workdir)
    os.environ.setdefault("OLCF_JOB_ID", str(spec.get("job_id", "")))
    os.chdir(workdir)
    try:
        bootstrap.apply_job_env(spec)
        paths.configure(spec)
        _install_signals()
        limits = spec.get("limits") or {}
        _start_watchdog(workdir, spec, float(limits["timeout_seconds"]) if limits.get("timeout_seconds") else None)
        return run_job(workdir, spec)
    except BaseException as exc:  # noqa: BLE001 - last resort: still produce a result
        finish(workdir, spec, state="failed", failure=failures.failure_dict(FailureCategory.INTERNAL.value, f"runner crashed: {type(exc).__name__}: {exc}",
                                                                              traceback=traceback.format_exc()))
        _cleanup_sensitive(workdir)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
