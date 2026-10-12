# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Health of the runtime supply chain trust anchor (review: "the development signing key is trusted by default").

The Ed25519 key bundled in ``olcf/runtime/keys/`` signs the shipped runtime manifests, and its **private half is in the source tree**. A control plane
that trusts it accepts a manifest forged by anyone who can reach the runtime registration API or a worker's runtime directory. The platform therefore
trusts it only outside production (``runtime.trust.allow_development_key``: automatic = ``environment != production``). The ``runtime-trust`` check:

* ``down``     - the development key is trusted while ``environment`` is ``production`` (an operator opted in explicitly, or has set no trust anchor of
                 their own and enabled it by hand). The check is **critical**: overall health is ``down`` and ``GET /v1/health/ready`` answers 503;
* ``degraded`` - the development key is trusted in a non-production environment (expected on a laptop or in ``olcf quickstart``; shown so it is not forgotten);
* ``ok``       - it is not trusted; the trust anchor is the operator's own key(s).
"""

from __future__ import annotations

from typing import Any

from olcf.core.context import AppContext
from olcf.core.health import ComponentHealth, HealthCheck, Status


def trust_status(settings: Any) -> dict[str, Any]:
    """Describe the trust store the control plane builds from ``settings`` (no key material)."""
    from olcf.runtime.signing import TrustStore

    store = TrustStore.for_settings(settings)
    production = str(settings.environment).strip().lower() == "production"
    return {"environment": settings.environment, "production": production, "dev_key_trusted": store.trusts_dev_key(), "trusted_key_ids": store.key_ids(),
            "development_key_ids": store.dev_key_ids(), "explicit_setting": settings.runtime.trust.allow_development_key}


#: What an operator does when the development key is trusted (one text for the health check, ``olcf status`` and ``olcf doctor``).
DEV_KEY_FIX = ("set runtime.trust.allow_development_key = false (or OLCF_RUNTIME_ALLOW_DEV_KEY=0), generate your own key (`python -m olcf.runtime keygen`), "
               "re-sign the runtime manifests and point runtime.trust.dir / OLCF_RUNTIME_TRUST_DIR at its public key")
#: What an operator does when no signing key is trusted at all.
NO_KEY_FIX = "add your public key to the trust directory (runtime.trust.dir / OLCF_RUNTIME_TRUST_DIR)"


def remediation_for(labels: dict[str, Any]) -> str | None:
    """The fix for a ``runtime-trust`` health result that is not ``ok``, from its ``labels`` (as ``GET /v1/admin/health`` returns them); ``None`` when nothing needs fixing."""
    if labels.get("dev_key_trusted"):
        return DEV_KEY_FIX
    if not labels.get("trusted_key_ids"):
        return NO_KEY_FIX
    return None


def check(settings: Any) -> ComponentHealth:
    st = trust_status(settings)
    labels = {k: st[k] for k in ("environment", "dev_key_trusted", "trusted_key_ids", "explicit_setting")}
    if st["dev_key_trusted"]:
        fix = DEV_KEY_FIX
        if st["production"]:
            return ComponentHealth("runtime-trust", Status.DOWN, f"the development runtime-signing key (its private half is public) is trusted in a production environment: {fix}",
                                   category="security", labels=labels)
        return ComponentHealth("runtime-trust", Status.DEGRADED, f"the development runtime-signing key is trusted (environment={st['environment']}): fine for development, never for production: {fix}",
                               category="security", labels=labels)
    n = len(st["trusted_key_ids"])
    if n == 0:
        return ComponentHealth("runtime-trust", Status.DEGRADED, f"no runtime signing key is trusted: no signed runtime manifest can be installed; {NO_KEY_FIX}", category="security", labels=labels)
    return ComponentHealth("runtime-trust", Status.OK, f"{n} operator key(s) trusted; the development key is not trusted", category="security", labels=labels)


def register_health(ctx: AppContext) -> None:
    ctx.health.register(HealthCheck("runtime-trust", "security", "Trust anchor for signed runtime manifests (development key)", lambda: check(ctx.settings), None, ttl=30, critical=True))
