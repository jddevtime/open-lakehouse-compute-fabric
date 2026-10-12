# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The relational model shared by the SQL and PySpark analyzers: relations (query results, DataFrames, views), their columns and where each column comes from.

A :class:`Relation` is an *abstract* result set: an ordered list of output columns (:class:`ColInfo`), each knowing the set of base-table columns it is
computed from (:class:`Origin`), plus what is **not** known about it:

``tail``
    dataset keys whose further, unlisted columns pass through (``SELECT *`` / ``spark.table("t")`` over a table whose schema is unknown). A later reference to a
    column that is not in ``cols`` can still be attributed when exactly one dataset is in the tail.
``opaque``
    nothing is known about the columns (the result of an operation the analyzers do not model, e.g. a pandas ``groupby`` or a UDF-based ``mapInPandas``).

An :class:`Origin` is ``(dataset key, column, steps)``; ``steps`` are the expression texts applied on the way (outermost first), so that the transformation recorded on
a ``TRANSFORMS`` edge can say how a target column was computed. Direct column copies add no step.

Everything here is immutable and hashable, which lets the analyzers share and compare relations safely; sets are always sorted before they are emitted so that the
output is deterministic regardless of ``PYTHONHASHSEED``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import NamedTuple

from olcf.intelligence.model import AssetRef

MAX_STEPS = 6


class Origin(NamedTuple):
    """One base-table column feeding a value: ``ds`` is a dataset key, ``steps`` the expressions applied between it and the output (outermost first)."""

    ds: str
    col: str
    steps: tuple[str, ...] = ()


@dataclass(frozen=True)
class Dataset:
    """A dataset an analyzer met: ``kind`` is ``table`` | ``view`` | ``file`` | ``external``; ``ref`` is how the registry should resolve it; ``schema`` the known columns."""

    key: str
    kind: str
    name: str
    ref: AssetRef
    schema: tuple[str, ...] | None = None


@dataclass(frozen=True)
class ColInfo:
    """An output column: its name, the origins it is computed from, whether some referenced column could not be resolved (``partial``) and the text of the expression that computes it (``expr``)."""

    name: str
    origins: frozenset[Origin] = frozenset()
    partial: bool = False
    expr: str | None = None


@dataclass(frozen=True)
class Lookup:
    """Result of looking a column up: ``status`` is ``ok`` | ``ambiguous`` | ``missing`` | ``opaque``."""

    status: str
    origins: frozenset[Origin] = frozenset()
    name: str | None = None


@dataclass(frozen=True)
class Relation:
    """An abstract result set, see the module docstring. ``quals`` maps lower-case qualifiers (aliases) of the inputs of a join to their relations."""

    cols: tuple[ColInfo, ...] = ()
    tail: tuple[str, ...] = ()
    datasets: frozenset[str] = frozenset()
    quals: tuple[tuple[str, Relation], ...] = field(default=(), compare=False)
    alias: str | None = None
    opaque: bool = False

    # ---- properties
    @property
    def positional_ok(self) -> bool:
        """True when the column list is complete and ordered (no unknown tail, not opaque), so that position ``i`` is really the ``i``-th output column."""
        return not self.tail and not self.opaque

    @property
    def names(self) -> list[str]:
        """The known column names in order."""
        return [c.name for c in self.cols]

    # ---- lookup
    def lookup(self, name: str) -> Lookup:
        """Look an unqualified column name up (case-insensitive).

        ``ok`` with the origins when exactly one column matches (or several with identical origins, as after a ``USING`` join); ``ambiguous`` for several different
        matches or an unlisted column with more than one tail dataset; ``ok`` with an origin in the single tail dataset for an unlisted column of a table whose schema is unknown;
        ``opaque`` when nothing is known; ``missing`` when the relation is complete and has no such column.
        """
        low = name.lower()
        hits = [c for c in self.cols if c.name.lower() == low]
        if len(hits) == 1:
            return Lookup("ok", hits[0].origins, hits[0].name)
        if len(hits) > 1:
            first = hits[0].origins
            if all(h.origins == first for h in hits):
                return Lookup("ok", first, hits[0].name)
            return Lookup("ambiguous", frozenset(), hits[0].name)
        if self.opaque:
            return Lookup("opaque")
        if len(self.tail) == 1:
            return Lookup("ok", frozenset({Origin(self.tail[0], name)}), name)
        if len(self.tail) > 1:
            return Lookup("ambiguous")
        return Lookup("missing")

    def lookup_qualified(self, qualifier: str, name: str) -> Lookup | None:
        """Look ``qualifier.name`` up through the join inputs (``quals``) or this relation's own alias; ``None`` when the qualifier is not known here."""
        q = qualifier.lower()
        for qual, sub in self.quals:
            if qual == q:
                return sub.lookup(name)
        if self.alias is not None and self.alias.lower() == q:
            return self.lookup(name)
        return None

    def with_alias(self, alias: str | None) -> Relation:
        """The same columns under a new alias (``DataFrame.alias``); earlier join inputs stay reachable."""
        return Relation(self.cols, self.tail, self.datasets, self.quals, alias, self.opaque)


EMPTY = Relation()


def opaque_relation(datasets: Iterable[str] = ()) -> Relation:
    """A relation about which nothing is known except which datasets feed it."""
    return Relation(cols=(), tail=(), datasets=frozenset(datasets), opaque=True)


def add_step(origins: Iterable[Origin], step: str | None) -> frozenset[Origin]:
    """Apply an expression to origins: ``step`` is prepended to each origin's chain (``None`` for a plain column reference leaves them unchanged)."""
    if step is None:
        return frozenset(origins)
    out = set()
    for o in origins:
        steps = (step, *o.steps)
        if len(steps) > MAX_STEPS:
            steps = (*steps[:MAX_STEPS // 2], "...", *steps[-(MAX_STEPS // 2):])
        out.add(Origin(o.ds, o.col, steps))
    return frozenset(out)


def chain_text(steps: tuple[str, ...]) -> str:
    """Render a step chain for ``EdgeFact.transformation``: ``outer <- inner``."""
    return " <- ".join(steps)


def sorted_origins(origins: Iterable[Origin]) -> list[Origin]:
    """Deterministic order for emitting origins."""
    return sorted(origins, key=lambda o: (o.ds, o.col.lower(), o.steps))


def group_origins(origins: Iterable[Origin]) -> list[tuple[str, str, list[tuple[str, ...]]]]:
    """Group origins by ``(dataset, column)`` (case-insensitive column) and collect the distinct step chains; deterministic order."""
    groups: dict[tuple[str, str], tuple[str, list[tuple[str, ...]]]] = {}
    for o in sorted_origins(origins):
        key = (o.ds, o.col.lower())
        if key not in groups:
            groups[key] = (o.col, [])
        if o.steps not in groups[key][1]:
            groups[key][1].append(o.steps)
    return [(ds, col, chains) for (ds, _), (col, chains) in groups.items()]


def concat(left: Relation, right: Relation) -> Relation:
    """Columns of ``left`` followed by those of ``right`` (join output); both inputs stay reachable through ``quals``."""
    quals = left.quals + right.quals
    if left.alias:
        quals += ((left.alias.lower(), left),)
    if right.alias:
        quals += ((right.alias.lower(), right),)
    return Relation(left.cols + right.cols, left.tail + right.tail, left.datasets | right.datasets, quals, None, left.opaque or right.opaque)
