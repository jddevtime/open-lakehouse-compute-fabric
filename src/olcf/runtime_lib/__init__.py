# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Code that executes *inside* a runtime virtualenv next to Spark (CONTRACTS section 12).

Modules here may import only the standard library, ``pyspark``, optional packages installed by the runtime
(``ipykernel``, ``jupyter_client``, ``nbformat``, ``pandas`` ...) and the stdlib-only parts of ``olcf``
(``olcf.core.redaction``, ``olcf.core.errors``, ``olcf.version``). Never import ``fastapi``, ``pydantic``,
``sqlalchemy`` or ``olcf.core.models`` from here.

* :mod:`~olcf.runtime_lib.runner` - job entry point (``python -m olcf.runtime_lib.runner <workdir>/job.json``)
* :mod:`~olcf.runtime_lib.spark` - SparkSession factory + default-lakehouse semantics
* :mod:`~olcf.runtime_lib.executor` - batch notebook execution through a real Jupyter kernel
* :mod:`~olcf.runtime_lib.kernel_host` - interactive kernel process
* :mod:`~olcf.runtime_lib.bootstrap` - pre-user-code hook (namespace, events, ``notebookutils`` hook)
* :mod:`~olcf.runtime_lib.paths` - relative ``Files/`` / ``Tables/`` path resolution
* :mod:`~olcf.runtime_lib.logbuf` - bounded buffers for output truncation
* :mod:`~olcf.runtime_lib.lineage_probe` - reports the datasets user code reads and writes (``@@OLCF@@ dataset_*`` events)
"""
