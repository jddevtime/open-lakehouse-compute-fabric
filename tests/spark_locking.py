# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The sandbox-wide Spark lock shared by every test that starts a JVM (two builders share 2 CPUs / 7 GB, see CONTRACTS section 0).

One implementation on top of :class:`olcf.core.filelock.FileLock` so test collection does not import ``fcntl`` (it does not exist on Windows).
POSIX hosts keep the historical ``/tmp/olcf-spark.lock`` so ``flock /tmp/olcf-spark.lock <command>`` from a shell still serialises with the tests.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

from olcf.core.filelock import FileLock

SPARK_LOCK_PATH = Path("/tmp/olcf-spark.lock") if os.name != "nt" else Path(tempfile.gettempdir()) / "olcf-spark.lock"  # noqa: S108 - the documented shared lock path


@contextlib.contextmanager
def spark_lock_held() -> Iterator[None]:
    """Hold the exclusive Spark lock (blocking, no timeout) for the duration of the ``with`` block. Do not nest."""
    with FileLock(SPARK_LOCK_PATH):
        yield
