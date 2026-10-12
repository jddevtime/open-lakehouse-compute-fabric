# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Single source of truth for version numbers and contract versions."""

__version__ = "0.1.0"

#: Version of the public REST contract (path prefix ``/v1``).
API_VERSION = "v1"
#: Version of the execution-capsule JSON schema (see docs and ``schemas/capsule-v1.json``).
CAPSULE_SCHEMA_VERSION = "1.0"
#: Version of the controller <-> worker agent wire protocol.
WORKER_PROTOCOL_VERSION = "1.0"
#: Version of the compatibility rule set used by the Fabric analyzer.
COMPAT_RULESET_VERSION = "1.0.0"
#: Version of the database schema this build expects (see ``olcf.db.migrate``).
PRODUCT_NAME = "Open Lakehouse Compute Fabric"
PRODUCT_SHORT = "OLCF"
AUTHOR = "Justin Dodrill"
