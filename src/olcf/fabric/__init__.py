# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Microsoft Fabric Compute Extension adapter (work package FABRIC).

A Fabric notebook or pipeline activity submits itself (as an *execution capsule*) to this control plane; the adapter validates and
analyses it, creates a ``capsule`` job on customer-controlled Spark and returns status/results.  Sub-modules:

``capsule``     capsule models, canonical JSON, size/secret scanning, JSON-schema export
``signing``     challenge/nonce/HMAC protocol (integrity + replay protection)
``entra``       local Entra v2 JWT validator (JWKS cache, key rotation)
``auth``        Fabric caller authentication and least-privilege principal derivation
``rest``        Fabric REST client (getDefinition long-running operations, token acquisition)
``diagnostics`` stepwise connectivity diagnostics (DNS, TLS, proxy, Entra, Fabric API, OneLake, inbound)
``analyzer``    compatibility analyzer (Python AST + SQL, versioned declarative rules)
``service``     the ``fabric`` service registered in the application context
``api``         REST routes under ``/v1/integrations/fabric``

Labels in this package always read "Fabric Runtime 2.0 Compatible"; OLCF is not Microsoft Fabric and is not endorsed by Microsoft.
"""
