# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``collect_diagnostics``: a redacted support bundle of this node (CONTRACTS 4.2).

The bundle is a zip written to ``<state>/diagnostics/diag-<UTC timestamp>.zip`` and contains **only an allow-list** of files, every text
file passed through the redaction filter (bearer tokens, ``olcf_*`` tokens, private keys, passwords, cloud keys):

* ``summary.json``      inventory, metrics, runtimes, jobs, daemons, kernels, agent version, python, platform
* ``agent.log``         tail of the agent log
* ``daemons/<d>.log``   tails of the Spark master/worker logs
* ``jobs/<id>/...``     ``job.public.json`` (secret-free spec), ``agent-job.json``, tails of ``stdout.log``/``stderr.log``, ``result.json``
* ``runtimes/<id>/{manifest.json,lock.json}``

Never included: identity/keys/certificates, ``job.json`` / ``kernel-spec.json`` (credentials), environment variables, anything outside
the state directory. The command result carries the summary and the archive's path/size/sha256; when ``inline`` is requested and the
archive is at most 4 MiB it is also returned base64-encoded (the control channel is not meant for bulk transfer).
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import platform
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

from olcf.core.redaction import redact_obj, redact_text
from olcf.core.timeutil import now_iso
from olcf.version import WORKER_PROTOCOL_VERSION, __version__

TAIL_BYTES = 256 * 1024
INLINE_LIMIT = 4 * 1024 * 1024


def _tail_text(path: Path, limit: int = TAIL_BYTES) -> str | None:
    try:
        with open(path, "rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - limit))
            data = fh.read().decode("utf-8", "replace")
    except OSError:
        return None
    return ("[... truncated ...]\n" if size > limit else "") + redact_text(data)


def collect(state_dir: Path, summary: dict[str, Any], *, job_ids: list[str] | None = None, inline: bool = False, max_jobs: int = 20) -> dict[str, Any]:
    """Build the bundle. ``summary`` is the agent's live state (see :meth:`olcf.worker.agent.Agent.diagnostic_summary`)."""
    out_dir = state_dir / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    path = out_dir / f"diag-{stamp}.zip"
    buf = io.BytesIO()
    included: list[str] = []
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        def add(name: str, text: str | None) -> None:
            if text is not None:
                zf.writestr(name, text)
                included.append(name)

        base = {"generated_at": now_iso(), "agent_version": __version__, "protocol": WORKER_PROTOCOL_VERSION, "python": sys.version, "platform": platform.platform()}
        add("summary.json", json.dumps(redact_obj({**base, **summary}), indent=2, default=str))
        add("agent.log", _tail_text(state_dir / "logs" / "agent.log"))
        for d in sorted((state_dir / "daemons").glob("*/logs/daemon.log")) if (state_dir / "daemons").is_dir() else []:
            add(f"daemons/{d.parent.parent.name}.log", _tail_text(d))
        work = state_dir / "work"
        wanted = [work / j for j in job_ids] if job_ids else sorted((p for p in work.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime, reverse=True)[:max_jobs] if work.is_dir() else []
        for jd in wanted:
            for name in ("job.public.json", "agent-job.json", "result.json"):
                add(f"jobs/{jd.name}/{name}", _tail_text(jd / name, 64 * 1024))
            for name in ("stdout.log", "stderr.log"):
                add(f"jobs/{jd.name}/{name}", _tail_text(jd / name, 128 * 1024))
        rt = state_dir / "runtimes"
        for r in sorted(rt.iterdir()) if rt.is_dir() else []:
            if r.is_dir() and not r.name.startswith((".", "_")):
                for name in ("manifest.json", "lock.json"):
                    add(f"runtimes/{r.name}/{name}", _tail_text(r / name, 1024 * 1024))
    data = buf.getvalue()
    path.write_bytes(data)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    # keep the five most recent bundles
    old = sorted(out_dir.glob("diag-*.zip"))[:-5]
    for p in old:
        p.unlink(missing_ok=True)
    res: dict[str, Any] = {"path": str(path), "size": len(data), "sha256": hashlib.sha256(data).hexdigest(), "files": included, "summary": summary}
    if inline:
        if len(data) <= INLINE_LIMIT:
            res["archive_b64"] = base64.b64encode(data).decode("ascii")
        else:
            res["inline_skipped"] = f"archive is {len(data)} bytes (> {INLINE_LIMIT}); fetch it from the node"
    return res
