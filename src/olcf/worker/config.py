# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Agent configuration (CLI flags, optional ``agent.toml`` in the state dir, environment) and state-dir layout (CONTRACTS section 6)."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_STATE_DIR = Path("~/.olcf-worker")


def parse_labels(text: str | None) -> dict[str, str]:
    """``"site=hq,rack=3"`` -> dict (empty values allowed, keys validated)."""
    out: dict[str, str] = {}
    for part in (text or "").split(","):
        part = part.strip()
        if not part:
            continue
        k, _, v = part.partition("=")
        k = k.strip()
        if not k or not all(c.isalnum() or c in "-_./" for c in k):
            raise ValueError(f"invalid label key {k!r}")
        out[k] = v.strip()
    return out


@dataclass
class AgentConfig:
    """All knobs of the agent. Paths are absolute after :meth:`resolve`."""

    url: str | None = None
    token: str | None = None  # enrollment token (single use); never persisted
    name: str | None = None
    state_dir: Path = DEFAULT_STATE_DIR
    ca_pem: Path | None = None
    labels: dict[str, str] = field(default_factory=dict)
    pool: str = "default"
    embedded: bool = False
    log_level: str = "INFO"
    heartbeat_seconds: float = 5.0
    max_jobs: int | None = None  # default: number of cores
    max_cores: int | None = None
    max_memory_mb: int | None = None
    advertise_host: str | None = None
    #: Command prefix wrapped around every job (bubblewrap / sandbox-exec / docker run ...). ``{workdir}``, ``{runtime_dir}``,
    #: ``{state_dir}``, ``{home}`` are substituted. Fail closed: a configured but missing sandbox binary fails the job.
    sandbox_command: list[str] = field(default_factory=list)
    runtime_trust_dir: Path | None = None
    maven_repositories: list[str] = field(default_factory=list)
    pip_index_url: str | None = None
    pip_find_links: list[str] = field(default_factory=list)
    #: Per-job dependency layers (environments): never contact a package index / Maven repository (layers already in the cache, the wheelhouse
    #: ``<state>/layers/wheelhouse`` and ``pip_find_links`` only), accept only these index URL prefixes (empty = whatever the lock names), build timeout.
    layer_offline: bool = False
    layer_allowed_indexes: list[str] = field(default_factory=list)
    layer_timeout_seconds: float = 1800.0
    env_passthrough: list[str] = field(default_factory=list)  # extra env var names jobs may inherit from the agent
    #: Offer the built-in ``olcf-host-python`` runtime (Python-engine jobs and sessions on this worker's own interpreter, no Spark). Disable it (``host_python = false``)
    #: on workers that must only run work on an installed, pinned runtime.
    host_python: bool = True
    stop_children_on_exit: bool | None = None  # job processes: default True when embedded, otherwise they outlive the agent and are adopted
    #: Spark Master/Worker daemons started by this agent are stopped (process-group kill) when the agent exits (SIGTERM/SIGINT/SIGHUP/``shutdown``) unless this is true: a stopped
    #: agent must not leave orphan JVMs. Set true (``--keep-daemons-on-exit``) for rolling restarts that keep the cluster up; the next agent adopts them.
    keep_daemons_on_exit: bool = False
    daemon_memory: str = "512m"
    ready_timeout_seconds: float = 90.0
    job_retention_hours: float = 24.0
    log_chunk_bytes: int = 64 * 1024
    log_buffer_frames: int = 64
    implicit_log_ack_seconds: float = 5.0
    kill_grace_seconds: float = 10.0
    timeout_slack_seconds: float = 10.0
    ws_ping_seconds: float = 20.0
    backoff_min: float = 1.0
    backoff_max: float = 30.0
    insecure_ws: bool = False  # allow ws:// (plain) even when not embedded; default True for http:// controller URLs anyway
    #: Credential renewal (CONTRACTS 4.2): the controller says when (hello ``credentials.renew_after``); the agent adds a random delay up to this many seconds so a fleet
    #: whose certificates were issued together does not ask at the same instant, and retries a failed exchange after ``cert_renew_retry_seconds`` (doubling, at most 1 h).
    cert_renew_jitter_seconds: float = 300.0
    cert_renew_retry_seconds: float = 300.0

    def resolve(self) -> AgentConfig:
        self.state_dir = Path(self.state_dir).expanduser().resolve()
        if self.ca_pem:
            self.ca_pem = Path(self.ca_pem).expanduser().resolve()
        if self.max_cores is not None and self.max_cores < 1:
            raise ValueError("max_cores must be >= 1")
        if self.stop_children_on_exit is None:
            self.stop_children_on_exit = bool(self.embedded)
        return self

    # ---- layout
    def path(self, *parts: str) -> Path:
        return self.state_dir.joinpath(*parts)

    def ensure_dirs(self) -> None:
        for sub in ("", "runtimes", "work", "logs", "daemons", "kernels", "diagnostics", "cache"):
            d = self.state_dir / sub
            d.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.state_dir, 0o700)
            os.chmod(self.state_dir / "work", 0o700)
        except OSError:  # pragma: no cover - Windows
            pass

    @classmethod
    def from_file(cls, path: Path, **overrides: Any) -> AgentConfig:
        data = tomllib.loads(Path(path).read_text(encoding="utf-8")) if Path(path).is_file() else {}
        fields = {f for f in cls.__dataclass_fields__}  # noqa: C419
        kw = {k: v for k, v in data.items() if k in fields}
        kw.update({k: v for k, v in overrides.items() if v is not None})
        for key in ("state_dir", "ca_pem", "runtime_trust_dir"):
            if kw.get(key) is not None:
                kw[key] = Path(kw[key])
        return cls(**kw)
