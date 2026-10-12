# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Governance for OLCF Intelligence: business glossary, domains and data products, owners and stewards, classification (suggest -> review -> approve, with lineage propagation),
certification and change requests, OBSERVE-mode policies and violations, and a quality score.

Governance **observes and reports**. Nothing in the compute, SQL, pipeline or lineage-ingest path calls it; a policy with ``mode: "enforce"`` is refused; suggestions are never approved
automatically. The package is optional: the Intelligence module loads it with ``install(ctx, service)`` (after the database migrations) and keeps working without it - or with its
tables missing.

``install`` creates :class:`~olcf.intelligence.governance.service.GovernanceService` as ``service.governance`` (which implements the optional hooks ``decorate``, ``owners_of`` and
``asset_filter`` of the core), registers the ``governance.*`` runtime settings and the ``intelligence-governance`` health check, seeds the built-in classification labels
(idempotent), adds a merge hook to the asset registry so governance rows follow merged assets, subscribes to the event bus (it only appends a row to a queue), registers the
classified-column provider of the sampling service, and returns a :class:`~olcf.core.context.Module` that mounts ``/v1/governance/*`` and runs the leader-only background loop. The
contract is ``docs/dev/GOVERNANCE_API.md``. Imports inside :func:`install` are lazy so that importing this package for its migration stream (``schema``) stays cheap.
"""

from __future__ import annotations

import logging
from typing import Any

from olcf.core.context import AppContext, Module
from olcf.core.errors import BadRequest

log = logging.getLogger("olcf.intelligence.governance")

__all__ = ["GovernanceModule", "install", "register_settings"]

QUALITY_WEIGHT_HELP = {
    "documentation": "a description of at least 10 characters", "ownership": "an effective (direct or inherited) owner", "classification": "classification suggestions that were reviewed",
    "freshness": "age of the newest known write against governance.freshness_hours", "certification": "certified (full), draft (half), otherwise none",
    "lineage": "the asset appears in recorded data-flow lineage", "violations": "no open policy violations", "schema_stability": "time since the last column change",
}

#: (key, description, default, kind, bounds)
SETTINGS: list[tuple[str, str, Any, str, tuple[float, float] | tuple[str, ...]]] = [
    ("governance.allow_self_approval", "Allow the author of a change request (or of a classification proposal under four-eyes approval) to approve it themselves; off by default", False, "bool", (0, 1)),
    ("governance.classification_four_eyes", "Four-eyes approval: a classification assigned by a curator without governance.approve waits in the review queue as 'suggested'; and nobody approves their own proposal", False, "bool", (0, 1)),
    ("governance.auto_classify_on_scan", "After a discovery scan, turn the sensitive-data suggestions found on columns into review-queue items (never approved automatically)", True, "bool", (0, 1)),
    ("governance.suggest_term_links", "After a discovery scan, propose glossary links for assets whose name equals a term, synonym or abbreviation (review-queue items)", True, "bool", (0, 1)),
    ("governance.evaluate_policies_on_scan", "Evaluate all enabled governance policies after every discovery scan", True, "bool", (0, 1)),
    ("governance.suggestion_min_confidence", "Lowest confidence (0..1) of a detector suggestion that is put into the review queue", 0.5, "float", (0, 1)),
    ("governance.propagation_depth", "Dataset hops a classification is followed downstream along lineage", 6, "int", (1, 10)),
    ("governance.propagation_max_nodes", "Most assets reached from one classified asset before the walk is cut off (reported as truncated)", 500, "int", (10, 20000)),
    ("governance.propagation_min_confidence", "Weakest lineage edge confidence a classification is propagated over: verified, derived, inferred or unknown", "derived", "choice",
     ("verified", "derived", "inferred", "unknown")),
    ("governance.freshness_hours", "Hours since the last write within which a table earns full freshness points in the quality score", 24, "int", (1, 87600)),
    ("governance.schema_stability_days", "Days without a column change that earn full schema-stability points in the quality score", 30, "int", (1, 3650)),
    ("governance.quality_column_rollup_weight", "Share (0..1) of a table's quality score taken from the mean score of its columns", 0.3, "float", (0, 1)),
    ("governance.quality_product_member_weight", "Share (0..1) of a data product's quality score taken from the mean score of its members", 0.7, "float", (0, 1)),
    ("governance.tick_seconds", "Seconds between passes of the governance background loop (signals, certification sweep)", 30, "int", (1, 3600)),
    ("governance.max_evaluation_assets", "Most assets one policy evaluation or coverage computation looks at (a larger estate is reported as truncated)", 20000, "int", (100, 1_000_000)),
    ("governance.violation_retention_days", "Days to keep resolved policy violations", 365, "int", (1, 3650)),
]


def _validator(kind: str, bounds: tuple[Any, ...]) -> Any:
    def check(v: Any) -> Any:
        if kind == "bool":
            if not isinstance(v, bool):
                raise BadRequest("must be true or false")
            return v
        if kind == "choice":
            if v not in bounds:
                raise BadRequest(f"must be one of {list(bounds)}")
            return v
        if isinstance(v, bool) or not isinstance(v, (int, float)) or (kind == "int" and not isinstance(v, int)) or not bounds[0] <= v <= bounds[1]:
            raise BadRequest(f"must be {'an integer' if kind == 'int' else 'a number'} between {bounds[0]:g} and {bounds[1]:g}")
        return v

    return check


def register_settings(settings: Any) -> None:
    """Register the ``governance.*`` runtime settings with help text (a key some other component already registered is left alone)."""
    from olcf.intelligence.governance.quality import DEFAULT_WEIGHTS

    registry = getattr(settings, "registry", {})
    for key, desc, default, kind, bounds in SETTINGS:
        if key not in registry:
            settings.register(key, description=desc, default=default, validator=_validator(kind, bounds))
    for name, weight in DEFAULT_WEIGHTS.items():
        key = f"governance.quality_weight.{name}"
        if key not in registry:
            settings.register(key, description=f"Weight of the '{name}' component in the quality score ({QUALITY_WEIGHT_HELP[name]}); a component that does not apply to an asset leaves the mean",
                              default=weight, validator=_validator("float", (0, 1000)))


class GovernanceModule(Module):
    """The part of the Intelligence module that governance contributes: its routes and the lifecycle of its background loop."""

    name = "intelligence-governance"

    def __init__(self, ctx: AppContext, governance: Any, unsubscribe: Any) -> None:
        super().__init__()
        from olcf.intelligence.governance import api
        from olcf.intelligence.ingest import LeaderLoop

        self.ctx, self.governance, self._unsubscribe = ctx, governance, unsubscribe
        self.routers = [(api.router, "")]
        self.loop = LeaderLoop(ctx, "governance", governance.tick, governance.interval, log)

    def start(self) -> None:
        """Start the leader-only background loop (never in the ``test`` environment: tests call ``governance.tick()``)."""
        if self.ctx.settings.environment.strip().lower() != "test" and self.ctx.settings.intelligence.enabled:
            self.loop.start()

    def stop(self) -> None:
        """Stop the loop and detach from the event bus."""
        self.loop.stop()
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None


def install(ctx: AppContext, service: Any) -> GovernanceModule:
    """Install governance into the Intelligence ``service`` (see the module docstring)."""
    from olcf.core.health import HealthCheck
    from olcf.intelligence.governance.hooks import make_hook
    from olcf.intelligence.governance.service import GovernanceService

    settings = ctx.maybe_service("settings")
    if settings is not None:
        register_settings(settings)
    gov = GovernanceService(ctx, service)
    service.governance = gov
    try:
        gov.classifications.ensure_builtins()
    except Exception as exc:  # noqa: BLE001 - missing tables must not stop the application
        log.warning("the built-in classification labels could not be seeded: %s", str(exc)[:300])
    hook = make_hook()
    if not any(getattr(h, "__module__", "") == hook.__module__ and h.__name__ == hook.__name__ for h in service.registry.merge_hooks):
        service.registry.merge_hooks.append(hook)
    unsubscribe = ctx.events.subscribe(gov.on_event) if ctx.settings.intelligence.enabled else None
    discovery = getattr(service, "discovery", None)
    if discovery is not None and getattr(discovery, "sampling", None) is not None:
        discovery.sampling.add_classified_provider(gov.classifications.classified_column_names)
    ctx.health.register(HealthCheck(name="intelligence-governance", category="data", description="Governance storage, background signals and the last pass (database only)", quick=gov.health,
                                    ttl=15.0, critical=False))
    return GovernanceModule(ctx, gov, unsubscribe)
