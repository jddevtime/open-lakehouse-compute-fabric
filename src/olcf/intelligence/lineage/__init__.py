# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Pure lineage analyzers: a stored definition (notebook, pipeline, SQL, semantic model, report, scanner result) goes in, :class:`~olcf.intelligence.model.Facts` come out.

Nothing in this package touches a database, the network or the file system, and nothing executes or imports user code (only ``ast``, ``sqlglot``, ``json`` and
regular expressions are used). The single entry point for callers is :func:`olcf.intelligence.lineage.engine.analyze_definition`; the individual analyzers
(``sql``, ``pyspark``, ``notebook``, ``pipeline_olcf``, ``pipeline_fabric``, ``tmdl``, ``semantic_model``, ``dax``, ``mcode``, ``report_pbi``, ``report_olcf``,
``scanner``) can be used directly as well.
"""
