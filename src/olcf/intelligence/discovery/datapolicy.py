# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The one data-read policy service of OLCF Intelligence: every read of *cell values* - the interactive sample (:class:`~olcf.intelligence.discovery.sampling.SamplingService`)
and the opt-in automatic value profiling of a scan (:class:`~olcf.intelligence.discovery.local.LocalConnector`) - asks it, and nothing else, whether it may read and what it must leave out.

Rules (the single place they are decided; there is no second copy to drift):

1. **The table decides.** The effective policy is always resolved for the *parent table asset*, using its authoritative id. A column asset has no policy of its own that could
   *broaden* anything: it **inherits** the restriction of its table. A policy row scoped to a column asset (``asset:<column id>``) is honoured only to *narrow* the table's policy (switch
   it off, lower ``max_rows``, mask harder, turn ``exclude_classified`` on) - it can never allow what the table's policy forbids. Hence a global / workspace / source allow can never
   defeat a table-level ban just because the caller named a column, and a table's stricter masking cannot be bypassed the same way.
2. **Existing tables are found by their authoritative aliases.** Profiling happens while a table is being discovered, before its facts are applied; the connector hands the table's aliases
   to :meth:`DataReadPolicy.resolve_assets`, which finds the existing asset (following a merge) so that a table-level policy applies to a *rescanned* table exactly as it does to an
   interactive sample. A table that is not known yet can only have a global / workspace / source policy; that never weakens the policy of an existing asset.
3. **Off means zero reads.** ``max_rows = 0``, a disabled row or no row at all means that no storage read is made at all - not even one that would be discarded.
4. **Classified columns are left out** (``exclude_classified``): columns carrying a classification in the asset's attributes, columns that
   :func:`~olcf.intelligence.discovery.detect.sensitive_column_names` suggests are sensitive, and the columns that the registered governance providers
   (:meth:`DataReadPolicy.add_classified_provider`) report as classified. **A provider that fails fails closed**: the read is refused (:attr:`ReadDecision.code` ``classification_unavailable``)
   rather than done with an unknown exclusion list.
5. **The projection is enforced on the result.** :meth:`DataReadPolicy.project` takes whatever the reader returned and keeps exactly the permitted, requested columns (case-insensitively
   matched, returned under the physical name); a requested column the table does not have is an error, never a silent widening to the whole table.
6. **Authorised reads are audited** by the caller with the column *names*, the row count and the policy scope - never a value (see ``SamplingService`` / ``LocalConnector``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from olcf.core.errors import BadRequest, PolicyDenied
from olcf.intelligence.discovery import detect
from olcf.intelligence.discovery.sampling_policy import EffectivePolicy, SamplingPolicyStore

#: Masking modes from the least to the most protective; a narrowing column policy may only move to the right.
MASKING_STRENGTH = {"none": 0, "partial": 1, "hash": 2, "redact": 3}

#: :attr:`ReadDecision.code` values.
ALLOWED = "allowed"
SAMPLING_DISABLED = "sampling_disabled"
CLASSIFICATION_UNAVAILABLE = "classification_unavailable"


class ClassificationLookupFailed(Exception):
    """A registered classification provider raised: the exclusion list is unknown, so the read must not happen (fail closed)."""


def narrower(a: EffectivePolicy, b: EffectivePolicy) -> EffectivePolicy:
    """The more restrictive of two effective policies (off beats on; then fewer rows; then stronger masking; ``exclude_classified`` is on if either says so).
    The scope reported is that of the policy that decided the row limit."""
    if not a.allowed:
        return a
    if not b.allowed:
        return b
    lead, other = (a, b) if a.max_rows <= b.max_rows else (b, a)
    masking = a.masking if MASKING_STRENGTH.get(a.masking, 3) >= MASKING_STRENGTH.get(b.masking, 3) else b.masking
    return EffectivePolicy(lead.max_rows, masking, a.exclude_classified or b.exclude_classified, lead.scope, "")


@dataclass(frozen=True)
class ReadDecision:
    """The outcome of :meth:`DataReadPolicy.decide`. ``allowed`` is false when the policy is off (``code`` ``sampling_disabled``) or the classification lookup failed
    (``classification_unavailable``). ``classified`` holds the lower-case column names the governance providers classified."""

    allowed: bool
    code: str
    policy: EffectivePolicy
    reason: str = ""
    table_asset_ids: tuple[str, ...] = ()
    classified: frozenset[str] = field(default_factory=frozenset)

    def excluded_in(self, columns: Iterable[Mapping[str, Any]]) -> set[str]:
        """Lower-case names, among ``columns`` (``{"name", "type", ...}`` dicts), that must not be read: classified by a provider, classified in the column's own attributes or
        suggested sensitive by name / type. Empty when the policy does not exclude classified columns."""
        if not self.policy.exclude_classified:
            return set()
        cols = [c for c in columns if isinstance(c, Mapping) and c.get("name")]
        out = {str(c["name"]).lower() for c in cols if c.get("classifications") or c.get("classification")}
        out |= detect.sensitive_column_names([dict(c) for c in cols])
        out |= set(self.classified)
        return out


DENIED_OFF = ReadDecision(False, SAMPLING_DISABLED, EffectivePolicy(0, "partial", True, "none", "no sampling policy exists"), "no sampling policy exists")


class DataReadPolicy:
    """See the module docstring. Holds no state of its own besides the registered classification providers; the policy rows live in :class:`SamplingPolicyStore`."""

    def __init__(self, registry: Any, store: SamplingPolicyStore) -> None:
        self.registry = registry
        self.policies = store
        self._classified: list[Callable[[str], set[str]]] = []

    # ---- extension point
    def add_classified_provider(self, provider: Callable[[str], set[str]]) -> None:
        """Register ``provider(asset_id) -> {column names classified in governance}``; both sampling and value profiling exclude those columns."""
        self._classified.append(provider)

    # ---- asset resolution
    def resolve_assets(self, aliases: Mapping[str, str]) -> list[str]:
        """The existing asset ids that ``aliases`` (``{kind: value}`` - the authoritative alias set of a table reference) point at, following merges. ``[]`` for a table not seen before."""
        if self.registry is None or not aliases:
            return []
        try:
            found = self.registry.lookup(list(aliases.items()))
        except Exception:  # noqa: BLE001 - an unreadable registry must not look like "a new table"; the caller treats it as unknown and the global policy still applies
            raise ClassificationLookupFailed("the asset registry could not be read") from None
        out: list[str] = []
        for asset_id in found.values():
            target = self.registry.merged_target(asset_id) or asset_id
            if target not in out:
                out.append(target)
        return out

    # ---- decision
    def effective(self, *, workspace_id: str | None, source_id: str | None, table_asset_ids: Sequence[str] = (), column_asset_id: str | None = None) -> EffectivePolicy:
        """The policy for a table (and optionally one of its columns). With several ids for the same table (aliases that were not merged yet) the most restrictive wins; a policy row on the
        column asset can only narrow it."""
        ids: list[str | None] = [i for i in table_asset_ids if i] or [None]
        policy: EffectivePolicy | None = None
        for tid in ids:
            p = self.policies.effective(workspace_id=workspace_id, source_id=source_id, asset_id=tid)
            policy = p if policy is None else narrower(policy, p)
        assert policy is not None
        if column_asset_id:
            col = self.policies.effective(workspace_id=workspace_id, source_id=source_id, asset_id=column_asset_id)
            if col.scope == f"asset:{column_asset_id}":
                policy = narrower(policy, col)
        return policy

    def decide(self, *, workspace_id: str | None, source_id: str | None, table_asset_ids: Sequence[str] = (), column_asset_id: str | None = None) -> ReadDecision:
        """May cell values of this table (column) be read, and which classified columns are excluded? Never raises for a refusal - read ``allowed`` / ``code``."""
        ids = tuple(dict.fromkeys(i for i in table_asset_ids if i))
        policy = self.effective(workspace_id=workspace_id, source_id=source_id, table_asset_ids=ids, column_asset_id=column_asset_id)
        if not policy.allowed:
            return ReadDecision(False, SAMPLING_DISABLED, policy, policy.reason, ids)
        classified: set[str] = set()
        if policy.exclude_classified:
            try:
                classified = self.classified_columns(tuple(dict.fromkeys([*ids, *([column_asset_id] if column_asset_id else [])])))
            except ClassificationLookupFailed:
                return ReadDecision(False, CLASSIFICATION_UNAVAILABLE, policy, "the classification lookup failed, so the read cannot be checked for classified columns", ids)
        return ReadDecision(True, ALLOWED, policy, "", ids, frozenset(classified))

    def classified_columns(self, asset_ids: Iterable[str]) -> set[str]:
        """Union of the lower-case column names every registered provider reports for ``asset_ids``; raises :class:`ClassificationLookupFailed` if any provider fails."""
        out: set[str] = set()
        for provider in self._classified:
            for aid in asset_ids:
                try:
                    out |= {str(x).lower() for x in provider(aid)}
                except Exception:  # noqa: BLE001 - a governance hiccup must not silently weaken the exclusion
                    raise ClassificationLookupFailed("a classification provider failed") from None
        return out

    # ---- projection
    @staticmethod
    def project(table: Any, decision: ReadDecision, *, declared: Sequence[Mapping[str, Any]] = (), requested: Iterable[str] | None = None, require: str | None = None) -> tuple[Any, list[str]]:
        """Enforce the projection on what a reader returned: keep exactly the requested (all when ``None``) columns that are not excluded, whichever schema they were found in.

        ``declared`` is the schema the scan recorded (may be empty); the exclusions are evaluated on the declared *and* the physical schema, so a column the reader returns beyond the
        declaration is checked too. ``require`` names a column that must be returned (a column asset): absent -> :class:`~olcf.core.errors.BadRequest`, excluded -> :class:`~olcf.core.errors.PolicyDenied`.
        Returns ``(arrow table with exactly the kept columns in physical order, excluded column names that were requested)``.
        """
        actual = {str(n): str(table.schema.field(n).type) for n in table.column_names}
        physical = [{"name": n, "type": t} for n, t in actual.items()]
        excluded = decision.excluded_in([*declared, *physical])
        by_lower: dict[str, str] = {}
        for n in table.column_names:
            by_lower.setdefault(str(n).lower(), str(n))
        wanted: set[str] | None = None if requested is None else {str(r).lower() for r in requested}
        if require is not None:
            wanted = {require.lower()} if wanted is None else (wanted & {require.lower()})
            if require.lower() not in by_lower:
                if require.lower() in excluded:
                    raise PolicyDenied(f"column '{require}' is excluded from sampling (classified or sensitive)", details={"reason": "column_excluded"},
                                       remediation="Turn off exclude_classified in the sampling policy if this column may be shown, or sample another column.")
                raise BadRequest(f"column '{require}' is not present in the table", details={"reason": "unknown_column"})
        if wanted is not None:
            missing = sorted(w for w in wanted if w not in by_lower and w not in excluded)
            if missing:
                raise BadRequest(f"unknown column(s) {missing} for this table", details={"reason": "unknown_column"})
        candidate = [str(n) for n in table.column_names if wanted is None or str(n).lower() in wanted]
        keep = [n for n in candidate if n.lower() not in excluded]
        dropped = sorted({n for n in candidate if n.lower() in excluded} | {w for w in (wanted or set()) if w in excluded and w not in by_lower})
        if require is not None and require.lower() in excluded:
            raise PolicyDenied(f"column '{require}' is excluded from sampling (classified or sensitive)", details={"reason": "column_excluded"},
                               remediation="Turn off exclude_classified in the sampling policy if this column may be shown, or sample another column.")
        return table.select(keep), dropped
