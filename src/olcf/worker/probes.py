# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Node-side probes (CONTRACTS 4.2 ``probe``; 4.6 ``POST /v1/workers/{id}/test``).

``probe {checks: [{kind, name?, timeout?, ...}]}`` runs every check from *this node's* point of view and answers::

    {"ok": bool, "ms": float, "checks": [{"name", "kind", "ok", "detail", "ms", "data"?}]}

Kinds and their parameters

========== ==========================================================================================================
tcp        ``host``, ``port`` - TCP connect
http       ``url``, ``method``(GET), ``expect_status``([...] or <400), ``use_agent_ca`` (trust the agent's pinned CA), ``verify`` - for
           https the TLS version / certificate expiry are reported; ``data`` carries the parsed JSON body when it is JSON
dns        ``host``, ``expect_ip``? - name resolution (all addresses in ``data``)
storage    ``uri`` (+ ``endpoint``?, ``write``?) - ``file://``/local paths: exists/readable (+ create+delete a probe file when ``write``);
           ``abfs(s)://``, ``wasb(s)://``, ``s3://``/``s3a://``, ``gs://``, ``http(s)://``: DNS + TCP (+ HTTPS handshake) to the endpoint the
           URI maps to. Credentials are *not* sent to the agent, so this proves *reachability*, not authorisation.
disk       ``path``?, ``min_free_gb``?, ``min_free_percent``? - free space (default path: the state directory)
clock      ``max_skew_seconds`` (5), ``server_time``? - skew vs the controller (from frame timestamps observed by the agent)
runtime    ``runtime_id``, ``deep``? - ``RuntimeManager.verify`` (markers, manifest/lock, jar hashes, venv; deep = smoke test)
spark_master ``url`` (``http://host:8080/json``) or ``master_url`` (``spark://host:7077``) - master health JSON (``data``) or TCP reachability
========== ==========================================================================================================

Every check is bounded by its timeout (default 5 s), exceptions never escape (they become ``ok: false`` with an actionable detail), and
details are redacted. Nothing here writes outside a temporary probe file, and ``shell`` is never used.
"""

from __future__ import annotations

import os
import socket
import ssl
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import psutil

from olcf.core.redaction import redact_text
from olcf.runtime.errors import RuntimeManagerError
from olcf.runtime.manager import RuntimeManager

DEFAULT_TIMEOUT = 5.0


class ProbeSuite:
    """Runs probe checks. ``clock`` supplies the controller clock skew; ``ca_pem`` is the agent's pinned CA."""

    def __init__(self, *, state_dir: Path, runtimes: RuntimeManager | None = None, ca_pem: str | None = None, skew: Callable[[], float | None] = lambda: None) -> None:
        self.state_dir, self.runtimes, self.ca_pem, self.skew = state_dir, runtimes, ca_pem, skew
        self._kinds: dict[str, Callable[[dict[str, Any], float], tuple[bool, str, Any]]] = {
            "tcp": self._tcp, "http": self._http, "dns": self._dns, "storage": self._storage, "disk": self._disk, "clock": self._clock, "runtime": self._runtime,
            "spark_master": self._spark_master,
        }

    # ------------------------------------------------------------------ entry
    def run(self, checks: list[dict[str, Any]]) -> dict[str, Any]:
        t0 = time.monotonic()
        out = [self.run_one(c) for c in checks]
        return {"ok": all(c["ok"] for c in out), "ms": round((time.monotonic() - t0) * 1000, 1), "checks": out}

    def run_one(self, check: dict[str, Any]) -> dict[str, Any]:
        kind = str(check.get("kind") or "")
        name = str(check.get("name") or kind)
        fn = self._kinds.get(kind)
        t0 = time.monotonic()
        if fn is None:
            return {"name": name, "kind": kind, "ok": False, "detail": f"unknown probe kind {kind!r} (supported: {', '.join(sorted(self._kinds))})", "ms": 0.0}
        timeout = float(check.get("timeout") or DEFAULT_TIMEOUT)
        try:
            ok, detail, data = fn(check, timeout)
        except Exception as exc:  # noqa: BLE001 - a probe never raises
            ok, detail, data = False, f"{type(exc).__name__}: {exc}", None
        res: dict[str, Any] = {"name": name, "kind": kind, "ok": bool(ok), "detail": redact_text(str(detail))[:1000], "ms": round((time.monotonic() - t0) * 1000, 1)}
        if data is not None:
            res["data"] = data
        return res

    # ------------------------------------------------------------------ checks
    @staticmethod
    def _tcp_connect(host: str, port: int, timeout: float) -> tuple[bool, str]:
        t0 = time.monotonic()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True, f"connected to {host}:{port} in {(time.monotonic() - t0) * 1000:.0f} ms"
        except socket.gaierror as exc:
            return False, f"cannot resolve {host!r}: {exc.strerror or exc}"
        except TimeoutError:
            return False, f"timed out connecting to {host}:{port} after {timeout:g}s (firewall or routing?)"
        except OSError as exc:
            return False, f"cannot connect to {host}:{port}: {exc.strerror or exc}"

    def _tcp(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        host, port = str(c.get("host") or ""), int(c.get("port") or 0)
        if not host or not port:
            return False, "tcp check needs host and port", None
        ok, detail = self._tcp_connect(host, port, timeout)
        return ok, detail, None

    def _dns(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        host = str(c.get("host") or "")
        if not host:
            return False, "dns check needs host", None
        try:
            infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            return False, f"cannot resolve {host!r}: {exc.strerror or exc}", None
        addrs = sorted({i[4][0] for i in infos})
        exp = c.get("expect_ip")
        if exp and exp not in addrs:
            return False, f"{host} resolves to {addrs}, expected {exp}", addrs
        return True, f"{host} -> {', '.join(addrs)}", addrs

    def _verify_arg(self, c: dict[str, Any]) -> ssl.SSLContext | bool:
        if c.get("verify") is False:
            return False
        if c.get("use_agent_ca") and self.ca_pem:
            return ssl.create_default_context(cadata=self.ca_pem)
        if c.get("ca_pem"):
            return ssl.create_default_context(cadata=str(c["ca_pem"]))
        return True

    def _tls_info(self, host: str, port: int, timeout: float, ctx: ssl.SSLContext | bool) -> dict[str, Any]:
        sslctx = ctx if isinstance(ctx, ssl.SSLContext) else (ssl.create_default_context() if ctx else ssl._create_unverified_context())  # noqa: S323, SLF001
        with socket.create_connection((host, port), timeout=timeout) as raw, sslctx.wrap_socket(raw, server_hostname=host) as tls:
            cert = tls.getpeercert()
            info: dict[str, Any] = {"protocol": tls.version(), "cipher": (tls.cipher() or ("",))[0]}
            if cert and cert.get("notAfter"):
                exp = datetime.fromtimestamp(ssl.cert_time_to_seconds(cert["notAfter"]), UTC)
                info["not_after"] = exp.isoformat()
                info["days_left"] = round((exp - datetime.now(UTC)).total_seconds() / 86400, 1)
            return info

    def _http(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        url = str(c.get("url") or "")
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not u.hostname:
            return False, f"http check needs an http(s) url, got {url!r}", None
        verify = self._verify_arg(c)
        tls: dict[str, Any] | None = None
        if u.scheme == "https":
            try:
                tls = self._tls_info(u.hostname, u.port or 443, timeout, verify)
            except ssl.SSLCertVerificationError as exc:
                return False, f"TLS certificate verification failed for {u.hostname}: {exc.verify_message or exc}", None
            except (ssl.SSLError, OSError) as exc:
                return False, f"TLS handshake with {u.hostname}:{u.port or 443} failed: {exc}", None
        headers = {str(k): str(v) for k, v in (c.get("headers") or {}).items()}
        t0 = time.monotonic()
        try:
            # a Spark master probe talks to the cluster-internal address and must not go through a site proxy; other probes measure what a client would see
            with httpx.Client(verify=verify, timeout=timeout, follow_redirects=False, trust_env=c.get("kind") != "spark_master") as client:
                r = client.request(str(c.get("method") or "GET").upper(), url, headers=headers)
        except httpx.HTTPError as exc:
            return False, f"request to {u.netloc} failed: {type(exc).__name__}: {exc}", None
        expect = c.get("expect_status")
        ok = (r.status_code in expect) if isinstance(expect, list) else r.status_code < 400
        data: Any = {"status": r.status_code}
        if tls:
            data["tls"] = tls
        try:
            body = r.json()
            if c.get("return_json") or c.get("kind") == "spark_master":
                data = body
        except ValueError:
            pass
        detail = f"HTTP {r.status_code} in {(time.monotonic() - t0) * 1000:.0f} ms" + (f", TLS {tls['protocol']}" if tls else "")
        if tls and tls.get("days_left") is not None and tls["days_left"] < 14:
            detail += f" (certificate expires in {tls['days_left']} days)"
        return ok, detail, data

    # -- storage
    @staticmethod
    def _storage_target(uri: str, endpoint: str | None) -> tuple[str, str | None, int, bool] | None:
        """(kind, host, port, tls) for a remote storage URI; ``kind`` is a label. ``None`` for local paths."""
        if endpoint:
            e = urlparse(endpoint if "://" in endpoint else "https://" + endpoint)
            return "endpoint", e.hostname, e.port or (443 if e.scheme == "https" else 80), e.scheme == "https"
        u = urlparse(uri)
        s = u.scheme.lower()
        if s in ("abfs", "abfss", "wasb", "wasbs"):
            host = (u.hostname or "").split("@")[-1]
            return "azure", host or None, 443, True
        if s in ("s3", "s3a", "s3n"):
            bucket = u.hostname or ""
            return "s3", f"{bucket}.s3.amazonaws.com" if bucket else "s3.amazonaws.com", 443, True
        if s == "gs":
            return "gcs", "storage.googleapis.com", 443, True
        if s in ("http", "https"):
            return "http", u.hostname, u.port or (443 if s == "https" else 80), s == "https"
        return None

    def _storage(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        uri = str(c.get("uri") or c.get("url") or "")
        if not uri:
            return False, "storage check needs uri", None
        tgt = self._storage_target(uri, c.get("endpoint"))
        if tgt is None:
            path = Path(urlparse(uri).path if uri.startswith("file:") else uri)
            if not path.exists():
                return False, f"path {path} does not exist on this node (mount missing or wrong path?)", None
            if not os.access(path, os.R_OK):
                return False, f"path {path} is not readable by the agent user", None
            detail = f"{path} exists and is readable"
            if c.get("write"):
                d = path if path.is_dir() else path.parent
                probe = d / f".olcf-probe-{uuid.uuid4().hex[:8]}"
                try:
                    probe.write_text("probe")
                    probe.unlink()
                except OSError as exc:
                    return False, f"{d} is not writable: {exc.strerror or exc}", None
                detail += " and writable"
            return True, detail, {"path": str(path)}
        kind, host, port, tls = tgt
        if not host:
            return False, f"cannot derive a host from storage uri {uri!r}", None
        try:
            addrs = sorted({i[4][0] for i in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)})
        except socket.gaierror as exc:
            return False, f"cannot resolve storage host {host!r} ({kind}): {exc.strerror or exc}", None
        ok, detail = self._tcp_connect(host, port, timeout)
        if ok and tls:
            try:
                info = self._tls_info(host, port, timeout, self._verify_arg(c))
                detail += f", TLS {info['protocol']}"
            except (ssl.SSLError, OSError) as exc:
                return False, f"{detail}, but the TLS handshake failed: {exc}", {"addresses": addrs}
        return ok, f"{kind}: {detail} (reachability only; credentials are not checked from the node)", {"host": host, "port": port, "addresses": addrs}

    def _disk(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        path = Path(str(c.get("path") or self.state_dir))
        while not path.exists() and path != path.parent:
            path = path.parent
        du = psutil.disk_usage(str(path))
        free_gb = du.free / 1024**3
        data = {"path": str(path), "free_gb": round(free_gb, 2), "total_gb": round(du.total / 1024**3, 2), "percent_used": du.percent}
        min_gb, min_pct = float(c.get("min_free_gb") or 1.0), c.get("min_free_percent")
        if free_gb < min_gb:
            return False, f"only {free_gb:.1f} GB free on {path} (need at least {min_gb:g} GB)", data
        if min_pct is not None and (100 - du.percent) < float(min_pct):
            return False, f"only {100 - du.percent:.0f}% free on {path} (need at least {float(min_pct):g}%)", data
        return True, f"{free_gb:.1f} GB free on {path} ({du.percent:.0f}% used)", data

    def _clock(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        limit = float(c.get("max_skew_seconds") or 5.0)
        if c.get("server_time"):
            from olcf.worker.protocol import parse_ts

            ts = parse_ts(c["server_time"])
            skew = None if ts is None else time.time() - ts
        else:
            skew = self.skew()
        if skew is None:
            return False, "no controller timestamp has been observed yet, cannot estimate the clock skew", None
        ok = abs(skew) <= limit
        return ok, f"local clock is {skew:+.2f}s relative to the controller (limit {limit:g}s)" + ("" if ok else ": fix NTP/chrony on this node (TLS, token expiry and scheduling depend on it)"), {"skew_seconds": round(skew, 3)}

    def _runtime(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        if self.runtimes is None:
            return False, "no runtime manager", None
        rid = str(c.get("runtime_id") or "")
        if not rid:
            return False, "runtime check needs runtime_id", None
        try:
            res = self.runtimes.verify(rid, deep=bool(c.get("deep")))
        except RuntimeManagerError as exc:
            return False, f"{exc.message}" + (f" - {exc.remediation}" if exc.remediation else ""), exc.details or None
        return True, f"runtime {rid} verified ({len(res['steps'])} steps)", {"fingerprint": res.get("fingerprint")}

    def _spark_master(self, c: dict[str, Any], timeout: float) -> tuple[bool, str, Any]:
        url = c.get("url")
        if url:
            ok, detail, data = self._http({**c, "url": url, "return_json": True}, timeout)
            if ok and isinstance(data, dict):
                alive = str(data.get("status", "")).upper() == "ALIVE"
                if not alive:
                    return False, f"master answered but its status is {data.get('status')!r}", data
                workers = [w for w in data.get("workers") or [] if str(w.get("state", "")).upper() == "ALIVE"]
                return True, f"master ALIVE with {len(workers)} worker(s)", data
            return ok, detail, data
        mu = urlparse(str(c.get("master_url") or ""))
        if mu.scheme != "spark" or not mu.hostname:
            return False, "spark_master check needs url (…/json) or master_url (spark://host:port)", None
        ok, detail = self._tcp_connect(mu.hostname, mu.port or 7077, timeout)
        return ok, detail, None
