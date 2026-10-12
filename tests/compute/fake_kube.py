# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""An in-memory Kubernetes for the provider tests: Secrets, Services and Pods, with a kubelet that behaves like the real one where the OLCF contract depends on it.

* **Volumes.** A Pod's ``secret`` volumes are materialised from the Secret object and are *always read-only*, whatever ``readOnly`` the mount says (the kubelet's
  behaviour); ``emptyDir`` volumes are writable scratch directories. Container paths in the command, arguments and environment are mapped to those directories.
* **Containers.** ``runner`` mode really executes the pod's command (``python -m ...``) in a subprocess through :mod:`tests.compute.kube_launcher`, which turns any
  write below a read-only mount into ``EROFS``. The container's stdout/stderr become the pod log (timestamped like ``kubectl logs --timestamps``); the termination
  message file becomes ``state.terminated.message``. Nothing is fabricated: if the runner does not emit a result, the log has none. ``scripted`` mode plays back
  ``log`` lines and an optional result event instead (for volume / rotation / missing-result scenarios).
* **Logs.** ``pod_log`` honours ``limit_bytes`` (which may cut a line in half), ``since_time`` (second granularity like ``metav1.Time``), ``timestamps`` and
  ``tail_lines``. ``log_retain_bytes`` rotates the oldest lines away for good; ``coarse_clock`` gives many lines the same timestamp.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from calendar import timegm
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from olcf.compute.errors import ProviderError
from olcf.compute.providers.kubernetes import AlreadyExists, KubeClient

LAUNCHER = Path(__file__).with_name("kube_launcher.py")
REPO = Path(__file__).resolve().parents[2]


def format_ts(ns: int) -> str:
    """RFC3339Nano like the kubelet writes it (trailing zeros of the fraction are trimmed)."""
    sec, frac = divmod(ns, 1_000_000_000)
    base = datetime.fromtimestamp(sec, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{frac:09d}".rstrip("0").rstrip(".") + "Z" if frac else base + "Z"


def parse_since(value: str) -> int:
    """``sinceTime`` -> epoch seconds (metav1.Time carries whole seconds)."""
    head = value.rstrip("Z").split(".")[0]
    return timegm(time.strptime(head, "%Y-%m-%dT%H:%M:%S"))


class _Pod:
    def __init__(self, key: tuple[str, str, str]) -> None:
        self.key = key
        self.lines: list[tuple[int, str]] = []  # (timestamp ns, text)
        self.rotated = 0
        self.polls = 0
        self.proc: subprocess.Popen[bytes] | None = None
        self.reader: threading.Thread | None = None
        self.root: Path | None = None
        self.mounts: dict[str, Path] = {}
        self.ro: list[Path] = []
        self.term_file: Path | None = None
        self.terminal: dict[str, Any] | None = None
        self.kill_reason: str | None = None
        self.last_ns = 0


class FakeKube(KubeClient):
    """See the module docstring. ``runner=True`` runs the pod command for real; otherwise the pod walks Pending -> Running -> Succeeded|Failed as it is read."""

    def __init__(self, *, pending_polls: int = 1, running_polls: int = 1, outcome: str = "succeed", log: list[str] | None = None, runner: bool = False,
                 emit_result: bool = False, result: dict[str, Any] | None = None, termination_message: str | None = None, log_retain_bytes: int | None = None,
                 coarse_clock: bool = False, runner_env: dict[str, str] | None = None, spread_ns: int = 1_000_000) -> None:
        self.objects: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.deleted: list[tuple[str, str, str, int | None]] = []
        self.created: list[dict[str, Any]] = []
        self.pending_polls, self.running_polls, self.outcome, self.runner = pending_polls, running_polls, outcome, runner
        self.log = log if log is not None else ["driver starting", "stage 1 done"]
        self.emit_result, self.result, self.termination_message = emit_result, result if result is not None else {"rows": 3}, termination_message
        self.log_retain_bytes, self.coarse_clock, self.runner_env = log_retain_bytes, coarse_clock, dict(runner_env or {})
        self.spread_ns = spread_ns  # scripted lines are stamped ``spread_ns`` apart, ending now (a realistic ~1000 lines/s) instead of all within one instant
        self.fail_create_kind: str | None = None
        self.unreachable = False
        self.log_calls: list[dict[str, Any]] = []
        self._pods: dict[tuple[str, str, str], _Pod] = {}
        self._lock = threading.RLock()

    # ---- helpers ------------------------------------------------------------------------------------------------
    def _check(self) -> None:
        if self.unreachable:
            raise ProviderError("kubernetes API unreachable")

    def _tick(self, p: _Pod) -> int:
        now = time.time_ns()
        if self.coarse_clock:
            now = (now // 1_000_000_000) * 1_000_000_000
        else:
            now = max(now, p.last_ns + 1)
        p.last_ns = now
        return now

    def append_log(self, name: str, text: str, *, namespace: str | None = None) -> None:
        """Append output to a pod's log the way a container would (test hook for scripted scenarios)."""
        with self._lock:
            key = next(k for k in self._pods if k[2] == name and (namespace is None or k[1] == namespace))
            self._append(self._pods[key], text.splitlines() or [""])

    def _append(self, p: _Pod, lines: list[str], *, spread: bool = False) -> None:
        with self._lock:
            spaced = spread and not self.coarse_clock and self.spread_ns > 0 and len(lines) > 1
            if spaced:
                p.last_ns = max(p.last_ns, time.time_ns() - len(lines) * self.spread_ns)
            for line in lines:
                if spaced:
                    p.last_ns += self.spread_ns
                    p.lines.append((p.last_ns, line))
                else:
                    p.lines.append((self._tick(p), line))
            if self.log_retain_bytes is not None:
                total = sum(len(t) + 1 for _, t in p.lines)
                while p.lines and total > self.log_retain_bytes:
                    total -= len(p.lines.pop(0)[1]) + 1
                    p.rotated += 1

    # ---- API ----------------------------------------------------------------------------------------------------
    def create(self, manifest: dict[str, Any]) -> dict[str, Any]:
        self._check()
        kind = manifest["kind"].lower()
        if self.fail_create_kind == kind:
            raise ProviderError(f"quota exceeded creating {kind}")
        key = (kind, manifest["metadata"].get("namespace", "default"), manifest["metadata"]["name"])
        with self._lock:
            if key in self.objects:
                raise AlreadyExists(f"{kind} {key[2]} already exists")
            obj = json.loads(json.dumps(manifest))
            if kind == "pod":
                obj["status"] = {"phase": "Pending"}
                self._pods[key] = _Pod(key)
            self.objects[key] = obj
            self.created.append(obj)
        return obj

    def get(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None:
        self._check()
        with self._lock:
            key = (kind.lower(), namespace, name)
            obj = self.objects.get(key)
            if obj is None:
                return None
            if key[0] == "pod":
                self._advance(key, obj)
            return json.loads(json.dumps(obj))

    def list(self, kind: str, namespace: str, selector: str) -> list[dict[str, Any]]:
        self._check()
        want = dict(p.split("=", 1) for p in selector.split(",") if "=" in p)
        with self._lock:
            return [json.loads(json.dumps(o)) for (k, ns, _), o in self.objects.items()
                    if k == kind.lower() and ns == namespace and all(((o["metadata"].get("labels") or {}).get(a) == b) for a, b in want.items())]

    def delete(self, kind: str, namespace: str, name: str, grace_seconds: int | None = None) -> None:
        self._check()
        with self._lock:
            key = (kind.lower(), namespace, name)
            self.deleted.append((key[0], namespace, name, grace_seconds))
            self.objects.pop(key, None)
            pod = self._pods.pop(key, None)
        if pod is not None:
            self._stop(pod, grace=grace_seconds if grace_seconds is not None else 30)

    def pod_log(self, namespace: str, pod: str, *, limit_bytes: int = 8388608, since_time: str | None = None, timestamps: bool = False, tail_lines: int | None = None) -> str:
        self._check()
        with self._lock:
            self.log_calls.append({"pod": pod, "limit_bytes": limit_bytes, "since_time": since_time, "timestamps": timestamps, "tail_lines": tail_lines})
            obj = self.objects.get(("pod", namespace, pod))
            p = self._pods.get(("pod", namespace, pod))
            if obj is None or p is None or obj["status"]["phase"] == "Pending":
                return ""
            self._advance(p.key, obj)
            lines = list(p.lines)
        if since_time:
            floor = parse_since(since_time)
            lines = [(ts, t) for ts, t in lines if ts // 1_000_000_000 >= floor]
        if tail_lines is not None:
            lines = lines[-int(tail_lines):] if tail_lines > 0 else []
        text = "".join((f"{format_ts(ts)} {t}\n" if timestamps else f"{t}\n") for ts, t in lines)
        raw = text.encode("utf-8")
        return raw[:limit_bytes].decode("utf-8", "ignore") if len(raw) > limit_bytes else text

    def version(self) -> str:
        self._check()
        return "v1.29.0-fake"

    def kinds(self, kind: str) -> list[str]:
        return sorted(n for (k, _, n) in self.objects if k == kind)

    def close(self) -> None:
        with self._lock:
            pods = list(self._pods.values())
            self._pods.clear()
        for p in pods:
            self._stop(p, grace=0)

    # ---- kubelet ------------------------------------------------------------------------------------------------
    def workdir(self, name: str, mount_path: str) -> Path:
        """Host directory behind a container mount path of a runner-mode pod (for assertions)."""
        p = next(p for k, p in self._pods.items() if k[2] == name)
        return p.mounts[mount_path]

    def kill(self, name: str, *, reason: str = "OOMKilled", exit_code: int = 137) -> None:
        """Kill the container from outside (OOM, eviction) without a termination message."""
        with self._lock:
            key = next(k for k in self._pods if k[2] == name)
            p = self._pods[key]
            p.kill_reason = reason
        if p.proc is not None and p.proc.poll() is None:
            p.proc.send_signal(signal.SIGKILL)
        elif p.terminal is None:
            p.terminal = {"exitCode": exit_code, "reason": reason}

    def _advance(self, key: tuple[str, str, str], obj: dict[str, Any]) -> None:
        p = self._pods[key]
        phase = obj["status"]["phase"]
        if phase not in ("Pending", "Running"):
            return
        p.polls += 1
        if p.polls > self.pending_polls and phase == "Pending":
            if self.runner and not self._mount(p, obj):
                return  # FailedMount: stays Pending
            obj["status"] = {"phase": "Running", "podIP": "10.0.0.7", "containerStatuses": [{"name": "driver", "state": {"running": {"startedAt": format_ts(time.time_ns())}}}]}
            obj["spec"]["nodeName"] = "node-1"
            if self.runner:
                self._spawn(p, obj)
            else:
                self._append(p, list(self.log), spread=True)
        if obj["status"]["phase"] == "Running":
            term = self._terminated(p, obj)
            if term is not None:
                ok = term["exitCode"] == 0
                obj["status"] = {"phase": "Succeeded" if ok else "Failed", "podIP": "10.0.0.7",
                                 "containerStatuses": [{"name": "driver", "state": {"terminated": {**term, "startedAt": format_ts(time.time_ns())}}}]}

    def _terminated(self, p: _Pod, obj: dict[str, Any]) -> dict[str, Any] | None:
        if self.runner:
            if p.terminal is None and p.proc is not None and p.proc.poll() is not None:
                if p.reader is not None:
                    p.reader.join(timeout=5)
                rc = p.proc.returncode
                msg = ""
                if p.term_file is not None and p.term_file.exists():
                    msg = p.term_file.read_text(errors="replace")[-4096:]
                p.terminal = {"exitCode": rc if rc >= 0 else 128 - rc, "reason": p.kill_reason or ("Completed" if rc == 0 else "Error")}
                if msg:
                    p.terminal["message"] = msg
            return p.terminal
        if p.polls <= self.pending_polls + self.running_polls or self.outcome == "hang":
            return None
        if p.terminal is None:
            ok = self.outcome == "succeed"
            labels = obj["metadata"].get("labels") or {}
            if self.emit_result:
                doc = {"state": "succeeded", **self.result} if ok else {"state": "failed", "failure": {"category": "user-code", "message": "NameError: x"}}
                self._append(p, ["@@OLCF@@ " + json.dumps({"event": "result", "job_id": labels.get("olcf.io/job-id"), "attempt": int(labels.get("olcf.io/attempt") or 1), "result": doc})])
            p.terminal = {"exitCode": 0 if ok else 1, "reason": "Completed" if ok else "Error"}
            if self.termination_message:
                p.terminal["message"] = self.termination_message
        return p.terminal

    def _mount(self, p: _Pod, obj: dict[str, Any]) -> bool:
        spec = obj["spec"]
        p.root = Path(tempfile.mkdtemp(prefix="olcf-fakekube-"))
        hosts: dict[str, tuple[Path, bool]] = {}
        for vol in spec.get("volumes") or []:
            d = p.root / "vol" / vol["name"]
            d.mkdir(parents=True)
            if "secret" in vol:
                sec = self.objects.get(("secret", p.key[1], vol["secret"]["secretName"]))
                if sec is None:
                    shutil.rmtree(p.root, ignore_errors=True)
                    return False
                data = {**{k: base64.b64decode(v).decode("utf-8", "replace") for k, v in (sec.get("data") or {}).items()}, **(sec.get("stringData") or {})}
                items = vol["secret"].get("items") or [{"key": k, "path": k} for k in data]
                mode = int(vol["secret"].get("defaultMode", 0o644))
                for it in items:
                    f = d / it["path"]
                    f.parent.mkdir(parents=True, exist_ok=True)
                    f.write_text(data[it["key"]], encoding="utf-8")
                    f.chmod(mode)
                hosts[vol["name"]] = (d, True)  # Secret volumes are read-only whatever the mount says
            else:
                hosts[vol["name"]] = (d, False)
        for mnt in (spec["containers"][0].get("volumeMounts") or []):
            host, ro = hosts[mnt["name"]]
            p.mounts[mnt["mountPath"]] = host
            if ro or mnt.get("readOnly"):
                p.ro.append(host)
        return True

    def _map(self, p: _Pod, value: str) -> str:
        for mp in sorted(p.mounts, key=len, reverse=True):
            if value == mp or value.startswith(mp.rstrip("/") + "/"):
                return str(p.mounts[mp]) + value[len(mp):]
            if mp in value:
                value = value.replace(mp, str(p.mounts[mp]))
        return value

    def _spawn(self, p: _Pod, obj: dict[str, Any]) -> None:
        assert p.root is not None
        c = obj["spec"]["containers"][0]
        cmd = [self._map(p, str(a)) for a in [*(c.get("command") or []), *(c.get("args") or [])]]
        if cmd[0] in ("python", "python3") and cmd[1:2] == ["-m"]:
            argv = [sys.executable, str(LAUNCHER)]
            for ro in p.ro:
                argv += ["--ro", str(ro)]
            argv += cmd[1:]
        else:
            argv = cmd
        p.term_file = p.root / "termination-log"
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8", "HOME": str(p.root / "home"), "TMPDIR": str(p.root / "tmp"),
               "PYTHONPATH": os.pathsep.join([str(REPO / "src"), str(REPO)]), "PYTHONUNBUFFERED": "1", "OLCF_TERMINATION_LOG": str(p.term_file)}
        (p.root / "home").mkdir()
        (p.root / "tmp").mkdir()
        for e in c.get("env") or []:
            env[e["name"]] = self._map(p, str(e.get("value", "")))
        env.update(self.runner_env)
        p.proc = subprocess.Popen(argv, cwd=str(p.root), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)  # noqa: S603

        def pump() -> None:
            assert p.proc is not None and p.proc.stdout is not None
            for raw in iter(p.proc.stdout.readline, b""):
                self._append(p, [raw.decode("utf-8", "replace").rstrip("\n")])

        p.reader = threading.Thread(target=pump, name="fakekube-log", daemon=True)
        p.reader.start()

    def _stop(self, p: _Pod, *, grace: float) -> None:
        proc = p.proc
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=max(grace, 0.1))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        if p.reader is not None:
            p.reader.join(timeout=5)
        if p.root is not None:
            shutil.rmtree(p.root, ignore_errors=True)
