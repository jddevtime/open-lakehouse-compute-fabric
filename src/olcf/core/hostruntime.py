# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Names and messages of the built-in ``olcf-host-python`` pseudo-runtime, shared by the controller (planner, catalog), the worker and the CLI.

Kept free of imports (constants only) so the pure planner and the runtime-side code can use them without pulling in the runtime manager.
The implementation of the runtime itself is :mod:`olcf.runtime.hostpython`.
"""

from __future__ import annotations

HOST_RUNTIME_ID = "olcf-host-python"
HOST_FAMILY = "host-python"
HOST_SCHEMA = "olcf.runtime.host/1"

NO_SPARK_MESSAGE = ("this job runs on the worker's own Python interpreter (runtime 'olcf-host-python'), which has no Spark: "
                    "cells that use `spark`, `sc` or `%%sql` cannot run here")
NO_SPARK_REMEDIATION = ("install a Spark runtime (`olcf runtime install`, needs Maven Central or an offline bundle) and set the notebook engine to 'spark', "
                        "or rewrite the cell with DuckDB / deltalake / pandas")

__all__ = ["HOST_FAMILY", "HOST_RUNTIME_ID", "HOST_SCHEMA", "NO_SPARK_MESSAGE", "NO_SPARK_REMEDIATION"]
