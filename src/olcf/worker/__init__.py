# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The OLCF Worker Agent: runs on every machine of the compute fleet (CONTRACTS section 4.2, spec section 13).

The agent enrols with the control plane, keeps one outbound WebSocket open (reconnecting forever), reports inventory,
metrics and runtime/daemon state, and executes controller commands: run/cancel jobs (supervised runner processes), manage
Spark Standalone master/worker daemons from the installed runtimes, host interactive kernels, install/verify runtimes,
probe connectivity from the node's perspective and collect diagnostics.

Module map: :mod:`~olcf.worker.main` (CLI), :mod:`~olcf.worker.agent` (connection + dispatcher), :mod:`~olcf.worker.jobs`,
:mod:`~olcf.worker.daemons`, :mod:`~olcf.worker.kernels`, :mod:`~olcf.worker.probes`, :mod:`~olcf.worker.logship`,
:mod:`~olcf.worker.identity`, :mod:`~olcf.worker.metrics`, :mod:`~olcf.worker.inventory`, :mod:`~olcf.worker.sandbox`,
:mod:`~olcf.worker.diagnostics`, :mod:`~olcf.worker.procutil`, :mod:`~olcf.worker.protocol`, :mod:`~olcf.worker.config`.
"""
