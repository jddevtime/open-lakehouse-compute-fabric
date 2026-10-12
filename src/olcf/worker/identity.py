# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Enrolment client and identity storage (CONTRACTS section 4.2 "Enrollment").

``POST /v1/workers/enroll`` with the single-use enrolment token, a CSR generated on this machine (the private key never
leaves it) and the inventory. The response (worker id, long-lived worker token, CA certificate, optional client certificate,
channel URL) is stored under the state dir with 0600 permissions::

    identity.json   worker_id, worker_token, channel_url, heartbeat_seconds, url, name   (0600)
    ca.pem          pinned CA certificate for the controller (TLS verification of https/wss)
    client.crt      machine certificate issued from our CSR (mTLS)
    client.key      its private key (0600)

The enrolment token itself is never persisted or logged.

Renewal (CONTRACTS 4.2 "Credential renewal") replaces ``client.crt`` / ``client.key`` and the token in ``identity.json`` without ever leaving the node without a working
identity: the new pair is written next to the old one (``client.crt.next`` / ``client.key.next``) and verified to load, ``identity.json`` is rewritten atomically, and only
then are the files swapped (key first). :func:`recover_staged` finishes an interrupted swap, so a crash at any point leaves either the old or the new pair in place.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import ssl
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx

from olcf.core.errors import OlcfError
from olcf.core.redaction import redact_text
from olcf.core.timeutil import now_iso
from olcf.security.pki import PKI
from olcf.version import WORKER_PROTOCOL_VERSION, __version__
from olcf.worker.config import AgentConfig

log = logging.getLogger("olcf.worker.identity")


class EnrollmentError(OlcfError):
    """Enrolment with the control plane failed (code ``enrollment_failed``)."""
    code = "enrollment_failed"
    status = 502


@dataclass
class Identity:
    """What a worker keeps after enrolment: worker id, worker token, channel URL and control-plane URL."""
    worker_id: str
    worker_token: str
    channel_url: str
    url: str
    name: str
    heartbeat_seconds: float = 5.0
    enrolled_at: str = ""
    ca_pem: str | None = None
    client_cert_pem: str | None = None

    def redacted(self) -> dict[str, Any]:
        d = asdict(self)
        d["worker_token"] = "***REDACTED***"  # noqa: S105
        d.pop("ca_pem", None)
        d.pop("client_cert_pem", None)
        return d


def _write_private(path: Path, data: str | bytes) -> None:
    """Atomic write with 0600 permissions from the first byte."""
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data.encode() if isinstance(data, str) else data)
    os.replace(tmp, path)
    try:
        os.chmod(path, 0o600)
    except OSError:  # pragma: no cover - Windows
        pass


def identity_paths(state_dir: Path) -> dict[str, Path]:
    return {"identity": state_dir / "identity.json", "ca": state_dir / "ca.pem", "cert": state_dir / "client.crt", "key": state_dir / "client.key"}


def load_identity(state_dir: Path) -> Identity | None:
    recover_staged(state_dir)  # a swap interrupted by a crash or power loss
    p = identity_paths(state_dir)
    try:
        d = json.loads(p["identity"].read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    ident = Identity(worker_id=d["worker_id"], worker_token=d["worker_token"], channel_url=d["channel_url"], url=d.get("url", ""), name=d.get("name", ""),
                     heartbeat_seconds=float(d.get("heartbeat_seconds", 5.0)), enrolled_at=d.get("enrolled_at", ""))
    if p["ca"].is_file():
        ident.ca_pem = p["ca"].read_text()
    if p["cert"].is_file():
        ident.client_cert_pem = p["cert"].read_text()
    return ident


def save_identity(state_dir: Path, ident: Identity, *, key_pem: str | None = None) -> None:
    p = identity_paths(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    doc = {k: v for k, v in asdict(ident).items() if k not in ("ca_pem", "client_cert_pem")}
    _write_private(p["identity"], json.dumps(doc, indent=2))
    if ident.ca_pem:
        _write_private(p["ca"], ident.ca_pem)
    if ident.client_cert_pem:
        _write_private(p["cert"], ident.client_cert_pem)
    if key_pem:
        _write_private(p["key"], key_pem)


def delete_identity(state_dir: Path) -> None:
    for path in identity_paths(state_dir).values():
        path.unlink(missing_ok=True)
    for path in _staged_paths(state_dir).values():
        path.unlink(missing_ok=True)


def _staged_paths(state_dir: Path) -> dict[str, Path]:
    return {"cert": state_dir / "client.crt.next", "key": state_dir / "client.key.next"}


def cert_info(state_dir: Path) -> dict[str, str] | None:
    """What this node holds: ``{"serial": hex, "not_after": ISO}`` of ``client.crt`` (``None`` when there is no usable certificate)."""
    from cryptography import x509

    p = identity_paths(state_dir)["cert"]
    try:
        c = x509.load_pem_x509_certificate(p.read_bytes())
    except (OSError, ValueError):
        return None
    return {"serial": format(c.serial_number, "x"), "not_after": c.not_valid_after_utc.strftime("%Y-%m-%dT%H:%M:%SZ")}


def _pair_loads(cert: Path, key: Path) -> bool:
    try:
        ssl.create_default_context().load_cert_chain(str(cert), str(key))
        return True
    except (OSError, ssl.SSLError, ValueError):
        return False


_swap_lock = threading.RLock()  # the swap of the two files and a connection reading them must not interleave


def recover_staged(state_dir: Path) -> str | None:
    """Finish or discard an interrupted certificate swap. Returns what it did (``"installed"``, ``"discarded"``) or ``None`` when nothing was pending.

    * both ``.next`` files present: they are a verified pair (written and checked before the swap starts) - install both;
    * only ``client.crt.next`` present: the key was already swapped - install the certificate if it matches the key now in place;
    * a pair that does not load is discarded and the current files stay.
    """
    with _swap_lock:
        return _recover_staged(state_dir)


def _recover_staged(state_dir: Path) -> str | None:
    nxt, cur = _staged_paths(state_dir), identity_paths(state_dir)
    have_c, have_k = nxt["cert"].is_file(), nxt["key"].is_file()
    if not (have_c or have_k):
        return None
    if have_c and have_k:
        if not _pair_loads(nxt["cert"], nxt["key"]):
            nxt["cert"].unlink(missing_ok=True)
            nxt["key"].unlink(missing_ok=True)
            return "discarded"
        os.replace(nxt["key"], cur["key"])
        os.replace(nxt["cert"], cur["cert"])
        return "installed"
    if have_c and _pair_loads(nxt["cert"], cur["key"]):
        os.replace(nxt["cert"], cur["cert"])
        return "installed"
    for path in nxt.values():
        path.unlink(missing_ok=True)
    return "discarded"


def apply_renewal(state_dir: Path, ident: Identity, data: dict[str, Any], key_pem: str | None) -> list[str]:
    """Persist the answer of a ``renew_cert`` exchange (new certificate with the key generated for it, and/or a rotated worker token).

    Raises :class:`EnrollmentError` - without having changed anything - when the delivered certificate does not match the new key. Returns what was applied.
    """
    applied: list[str] = []
    cert_pem = data.get("cert_pem")
    paths, nxt = identity_paths(state_dir), _staged_paths(state_dir)
    if cert_pem:
        if not key_pem:
            raise EnrollmentError("the controller delivered a certificate but no certificate request was made")
        _write_private(nxt["key"], key_pem)
        _write_private(nxt["cert"], cert_pem)
        if not _pair_loads(nxt["cert"], nxt["key"]):
            for p in nxt.values():
                p.unlink(missing_ok=True)
            raise EnrollmentError("the delivered certificate does not match the new private key; the current certificate stays in use")
    if data.get("worker_token"):
        ident.worker_token = str(data["worker_token"])
        applied.append("token")
    if data.get("ca_pem") and data["ca_pem"] != ident.ca_pem:
        ident.ca_pem = str(data["ca_pem"])
        _write_private(paths["ca"], ident.ca_pem)
    if cert_pem:
        ident.client_cert_pem = str(cert_pem)
    # identity.json (atomic) goes first: if the process dies now, the token is saved and recover_staged() finishes the certificate swap on the next start
    doc = {k: v for k, v in asdict(ident).items() if k not in ("ca_pem", "client_cert_pem")}
    _write_private(paths["identity"], json.dumps(doc, indent=2))
    if cert_pem:
        recover_staged(state_dir)
        applied.insert(0, "certificate")
    return applied


def tls_context(ca_pem: str | None, *, cert_file: Path | None = None, key_file: Path | None = None) -> ssl.SSLContext:
    """TLS context pinned to ``ca_pem`` (only that CA is trusted when given); loads the machine certificate for mTLS."""
    ctx = ssl.create_default_context(cadata=ca_pem) if ca_pem else ssl.create_default_context()
    if cert_file and key_file and cert_file.is_file() and key_file.is_file():
        with _swap_lock:  # never read the pair half-way through a renewal swap
            ctx.load_cert_chain(str(cert_file), str(key_file))
    return ctx


def channel_url_for(base_url: str, channel_url: str) -> str:
    """Absolute ws(s) URL for the channel: accepts relative paths and http(s) schemes from the controller."""
    u = urlparse(channel_url)
    if not u.scheme:
        b = urlparse(base_url)
        u = urlparse(urlunparse((b.scheme, b.netloc, channel_url if channel_url.startswith("/") else "/" + channel_url, "", "", "")))
    scheme = {"http": "ws", "https": "wss"}.get(u.scheme, u.scheme)
    return urlunparse((scheme, u.netloc, u.path, u.params, u.query, u.fragment))


def enroll(cfg: AgentConfig, inventory: dict[str, Any], *, attempts: int = 1, retry_wait: float = 3.0) -> Identity:
    """Enrol with the control plane and persist the identity. Raises :class:`EnrollmentError` with an actionable message."""
    if not cfg.url or not cfg.token:
        raise EnrollmentError("enrolment needs --url and --token", remediation="Create a token in the control plane (POST /v1/workers/enrollment-tokens) and pass it with --token.")
    name = cfg.name or socket.gethostname()
    csr_pem, key_pem = PKI.generate_csr(name, sans=[name, inventory.get("hostname", name)])
    body = {"token": cfg.token, "name": name, "hostname": inventory.get("hostname"), "csr_pem": csr_pem, "inventory": inventory,
            "agent_version": __version__, "protocol": WORKER_PROTOCOL_VERSION, "labels": cfg.labels, "pool": cfg.pool}
    base = cfg.url.rstrip("/")
    verify: ssl.SSLContext | bool = True
    if cfg.ca_pem:
        verify = tls_context(Path(cfg.ca_pem).read_text())
    last: Exception | None = None
    for attempt in range(max(1, attempts)):
        try:
            with httpx.Client(verify=verify, timeout=30.0) as client:
                resp = client.post(f"{base}/v1/workers/enroll", json=body)
        except (httpx.HTTPError, OSError) as exc:
            last = exc
            log.warning("enrolment attempt %d failed: %s", attempt + 1, redact_text(str(exc)))
            if attempt + 1 < attempts:
                time.sleep(retry_wait)
            continue
        if resp.status_code in (200, 201):
            data = resp.json()
            ident = Identity(
                worker_id=data["worker_id"], worker_token=data["worker_token"], channel_url=channel_url_for(base, data["channel_url"]), url=base, name=name,
                heartbeat_seconds=float(data.get("heartbeat_seconds", 5.0)), enrolled_at=now_iso(), ca_pem=data.get("ca_pem") or (Path(cfg.ca_pem).read_text() if cfg.ca_pem else None),
                client_cert_pem=data.get("client_cert_pem"))
            save_identity(cfg.state_dir, ident, key_pem=key_pem if data.get("client_cert_pem") else None)
            log.info("enrolled as %s (%s)", ident.worker_id, name)
            return ident
        detail = redact_text(resp.text[:400])
        hint = {401: "The enrolment token is invalid or expired.", 403: "The enrolment token was rejected (revoked or already used).",
                404: "The control plane has no /v1/workers/enroll endpoint: check --url.", 410: "The enrolment token was already used."}.get(resp.status_code, "")
        raise EnrollmentError(f"enrolment rejected with HTTP {resp.status_code}: {detail}", remediation=(hint + " Create a new single-use token and retry.").strip(),
                              details={"status": resp.status_code})
    raise EnrollmentError(f"cannot reach the control plane at {base}: {last}", remediation="Check the URL, DNS, firewall/proxy and (for https) --ca-pem.")
