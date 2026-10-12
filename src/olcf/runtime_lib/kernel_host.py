# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Kernel host: an ipykernel process with the OLCF bootstrap pre-loaded (interactive sessions and batch notebooks).

``python -m olcf.runtime_lib.kernel_host --spec <path> [-f <connection_file>]``

* ``--spec`` points at a JSON file: either a job spec (``schema: olcf.jobspec/1``) or ``{"spec": <job spec>, "options": {...}}``.
  Options: ``eager_spark`` (start the SparkSession in the background right after the kernel is up - interactive sessions
  want this; batch runs keep Spark lazy so ``%%configure`` still works), ``workdir``.
* When ``-f`` is omitted a connection file is created next to the spec. Once the kernel is listening the host prints one line
  ``@@OLCF@@ {"event":"kernel_ready","connection_file":"..."}`` (CONTRACTS section 11).
* The user namespace gets ``spark`` (lazy), ``sc``, ``sqlContext``, ``display``, ``notebookutils``/``mssparkutils`` (via the
  FABRIC hook, see :mod:`olcf.runtime_lib.bootstrap`) and the magics from :mod:`olcf.runtime_lib.magics`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import traceback
from pathlib import Path
from typing import Any

from olcf.runtime_lib import bootstrap, magics, paths


class BenignKernelNoise(logging.Filter):
    """Drops the one ipykernel warning that is noise here and nothing else.

    ``IPKernelApp`` warns "Kernel is running over TCP without encryption ...". The kernel is a child of the node agent: its sockets are bound to the loopback
    interface and the connection file is private to the job directory, so the warning only pollutes every job's / Fabric submission's log. The match is exact
    (logger name, level and message prefix) - every other record, including other warnings of the same logger, passes through.
    """

    PREFIX = "Kernel is running over TCP without encryption"

    def filter(self, record: logging.LogRecord) -> bool:
        return not (record.name == "IPKernelApp" and record.levelno == logging.WARNING and str(record.msg).startswith(self.PREFIX))


def load_kernel_spec(path: str) -> tuple[dict[str, Any], dict[str, Any]]:
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if "spec" in doc and isinstance(doc["spec"], dict):
        return doc["spec"], doc.get("options") or {}
    return doc, {}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="olcf.runtime_lib.kernel_host")
    ap.add_argument("--spec", required=True)
    ap.add_argument("-f", "--connection-file", default=None)
    ns, _unknown = ap.parse_known_args(argv)
    spec, options = load_kernel_spec(ns.spec)
    workdir = options.get("workdir") or os.environ.get("OLCF_JOB_DIR") or os.getcwd()
    os.environ.setdefault("OLCF_JOB_DIR", str(workdir))
    bootstrap.apply_job_env(spec)
    paths.configure(spec)
    conn_file = ns.connection_file or str(Path(ns.spec).with_suffix(".connection.json"))

    from ipykernel.kernelapp import IPKernelApp

    app = IPKernelApp.instance()
    app.log.addFilter(BenignKernelNoise())  # before initialize(): the warning is logged while the sockets are created
    # fd-level capture would turn JVM log lines and @@OLCF@@ events into notebook outputs; keep them on the real stdout/stderr
    # (job logs) so the agent can ship them and parse the events.
    app.initialize(["-f", conn_file, "--IPKernelApp.capture_fd_output=False"])
    shell = app.shell
    spark = bootstrap.LazySpark(spec)
    namespace = bootstrap.build_namespace(spec, spark)
    shell.user_ns.update(namespace)
    magics.register_magics(shell, spec, spark)
    # exit() must stop the cell quietly: the executor reads the reserved display-data message, not the traceback
    shell.set_custom_exc((bootstrap.NotebookExit,), lambda *a, **k: None)
    bootstrap.emit_event("kernel_ready", connection_file=app.abs_connection_file, pid=os.getpid())
    if options.get("eager_spark"):
        def warm() -> None:
            try:
                spark.materialize()
            except Exception as exc:  # noqa: BLE001 - surfaced on first use as well
                bootstrap.emit_event("spark_error", error=f"{type(exc).__name__}: {exc}", traceback=traceback.format_exc()[-3000:])

        threading.Thread(target=warm, name="spark-warmup", daemon=True).start()
    app.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
