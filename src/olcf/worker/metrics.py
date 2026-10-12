# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Real node metrics via ``psutil`` (heartbeat ``metrics`` object, CONTRACTS section 4.2).

Keys: ``cpu_percent, load1, mem_total_mb, mem_available_mb, disk_total_gb, disk_free_gb, net_rx_bps, net_tx_bps`` plus extras
(``load5, load15, swap_percent, mem_percent, disk_percent, process_count, uptime_seconds``). Network rates are computed between
two consecutive calls. Works on Linux, macOS and Windows (``getloadavg`` falls back to CPU utilisation on Windows).
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

import psutil


class MetricsCollector:
    """Samples host metrics (CPU, load, memory, swap, disk, network, process count and uptime) with psutil."""
    def __init__(self, disk_path: Path | str) -> None:
        self.disk_path = str(disk_path)
        self._lock = threading.Lock()
        self._last_net: tuple[float, int, int] | None = None
        psutil.cpu_percent(interval=None)  # prime: the first call always returns 0.0
        self._sample_net()

    def _sample_net(self) -> tuple[float, float]:
        now = time.monotonic()
        io = psutil.net_io_counters()
        rx, tx = (io.bytes_recv, io.bytes_sent) if io else (0, 0)
        with self._lock:
            prev, self._last_net = self._last_net, (now, rx, tx)
        if prev is None or now - prev[0] <= 0:
            return 0.0, 0.0
        dt = now - prev[0]
        return max(0.0, (rx - prev[1]) / dt), max(0.0, (tx - prev[2]) / dt)

    def collect(self) -> dict[str, Any]:
        vm = psutil.virtual_memory()
        try:
            du = psutil.disk_usage(self.disk_path)
            disk_total, disk_free, disk_pct = du.total, du.free, du.percent
        except OSError:
            disk_total = disk_free = 0
            disk_pct = 0.0
        cpu = psutil.cpu_percent(interval=None)
        try:
            l1, l5, l15 = os.getloadavg()
        except (AttributeError, OSError):  # Windows
            l1 = l5 = l15 = cpu / 100.0 * (psutil.cpu_count() or 1)
        rx, tx = self._sample_net()
        swap = psutil.swap_memory()
        gb = 1024.0**3
        mb = 1024.0**2
        return {
            "cpu_percent": round(cpu, 1), "load1": round(l1, 2), "load5": round(l5, 2), "load15": round(l15, 2),
            "mem_total_mb": round(vm.total / mb), "mem_available_mb": round(vm.available / mb), "mem_percent": round(vm.percent, 1),
            "swap_percent": round(swap.percent, 1),
            "disk_total_gb": round(disk_total / gb, 2), "disk_free_gb": round(disk_free / gb, 2), "disk_percent": round(disk_pct, 1),
            "net_rx_bps": round(rx), "net_tx_bps": round(tx),
            "process_count": len(psutil.pids()), "uptime_seconds": round(time.time() - psutil.boot_time()),
        }
