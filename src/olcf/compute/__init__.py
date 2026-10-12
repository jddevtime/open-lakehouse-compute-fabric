# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Controller side of the OLCF compute fabric.

This package owns everything the control plane does with compute: the worker registry and WebSocket hub,
compute providers (Spark Standalone, local, external, Kubernetes, Fabric Livy), the scheduler and resource
admission, the job service, interactive notebook sessions, the runtime catalog and the topology/test APIs.

The agent side of the protocol (worker agent, runtime manager, runner) lives in ``olcf.worker`` / ``olcf.runtime``
and is specified in ``docs/dev/CONTRACTS.md`` section 4 plus the "Controller addenda (CTRL)" at the end of that file.
"""
