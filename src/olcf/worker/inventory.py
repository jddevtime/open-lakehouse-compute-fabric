# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Machine inventory reported at enrolment and in every ``hello`` (spec section 13: cores, usable RAM, disk, labels, accelerators,
installed runtimes, OS, agent version).

Expensive discovery (Python/Java probes spawn processes, ``nvidia-smi``) is cached for ten minutes.
"""

from __future__ import annotations

import platform
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import psutil

from olcf.core import hostname
from olcf.runtime import hostenv
from olcf.runtime.manifest import normalise_arch, normalise_os
from olcf.version import WORKER_PROTOCOL_VERSION, __version__

_cache_lock = threading.Lock()
_cache: tuple[float, dict[str, Any]] | None = None


def _os_release() -> dict[str, str]:
    p = Path("/etc/os-release")
    out: dict[str, str] = {}
    if p.is_file():
        for line in p.read_text(errors="replace").splitlines():
            if "=" in line:
                k, _, v = line.partition("=")
                out[k] = v.strip().strip('"')
    return out


def _addresses() -> list[str]:
    out: list[str] = []
    for name, addrs in psutil.net_if_addrs().items():
        if name.startswith(("lo", "docker", "veth", "br-")):
            continue
        for a in addrs:
            if a.family == socket.AF_INET and not a.address.startswith("127."):
                out.append(a.address)
    return out


def gpus() -> list[dict[str, Any]]:
    """NVIDIA GPUs through ``nvidia-smi`` (absent -> empty list)."""
    exe = shutil.which("nvidia-smi")
    if not exe:
        return []
    try:
        res = subprocess.run([exe, "--query-gpu=name,memory.total,driver_version,uuid", "--format=csv,noheader,nounits"],  # noqa: S603
                             capture_output=True, text=True, timeout=15, check=False)
    except (OSError, subprocess.SubprocessError):
        return []
    out = []
    for line in res.stdout.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 4:
            try:
                out.append({"vendor": "nvidia", "name": parts[0], "memory_mb": int(float(parts[1])), "driver": parts[2], "uuid": parts[3]})
            except ValueError:
                continue
    return out


def _discover() -> dict[str, Any]:
    pythons = []
    for series in ("3.11", "3.12", "3.13"):
        for p in hostenv.discover_pythons(series)[:1]:
            pythons.append({"version": p.version, "path": p.executable, "venv": p.has_venv})
    javas = [{"major": j.major, "version": j.version, "home": j.home, "vendor": j.vendor} for j in hostenv.discover_javas()]
    return {"python": pythons, "java": javas, "gpus": gpus(), "uv": bool(shutil.which("uv"))}


def collect_inventory(state_dir: Path, *, labels: dict[str, str], pool: str, runtimes: list[dict[str, Any]] | None = None,
                      reserve_memory_fraction: float = 0.15, refresh: bool = False, advertise_host: str | None = None) -> dict[str, Any]:
    """Full inventory dict. ``cores``/``memory_mb``/``disk_gb`` are the keys the controller topology uses."""
    global _cache
    with _cache_lock:
        if refresh or _cache is None or time.time() - _cache[0] > 600:
            _cache = (time.time(), _discover())
        disc = _cache[1]
    vm = psutil.virtual_memory()
    try:
        du = psutil.disk_usage(str(state_dir))
        disk_total, disk_free = du.total, du.free
    except OSError:
        disk_total = disk_free = 0
    gb = 1024**3
    mem_mb = round(vm.total / 1024**2)
    osr = _os_release()
    addrs = _addresses()
    return {
        "hostname": socket.gethostname(), "fqdn": hostname.fqdn(), "address": advertise_host or (addrs[0] if addrs else "127.0.0.1"), "addresses": addrs,
        "os": normalise_os(), "os_name": osr.get("PRETTY_NAME") or f"{platform.system()} {platform.release()}", "os_version": platform.version(),
        "kernel": platform.release(), "arch": normalise_arch(), "cpu_model": platform.processor() or osr.get("NAME", ""),
        "cores": psutil.cpu_count(logical=True) or 1, "cores_physical": psutil.cpu_count(logical=False) or psutil.cpu_count(logical=True) or 1,
        "memory_mb": mem_mb, "usable_memory_mb": round(mem_mb * (1 - reserve_memory_fraction)),
        "disk_gb": round(disk_total / gb, 1), "disk_free_gb": round(disk_free / gb, 1),
        "labels": dict(labels), "pool": pool, "agent_version": __version__, "protocol": WORKER_PROTOCOL_VERSION,
        "python": disc["python"], "java": disc["java"], "gpus": disc["gpus"], "uv": disc["uv"],
        "runtimes": runtimes or [], "boot_time": psutil.boot_time(), "state_dir": str(state_dir),
    }
