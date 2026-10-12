# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Helpers shared by the lineage analyzer tests: compact edge / gap descriptions and context builders.

An edge is described as ``"TYPE src -> dst [confidence]"`` (``+ " | transformation"`` when asked), a node as ``type:name`` (columns as ``col:<table>.<name>``), so that a test can state
the complete set of edges it expects and a failure shows the difference.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from olcf.intelligence.model import AssetRef, EdgeFact, Facts

CORPUS = Path(__file__).resolve().parents[2] / "corpus" / "lineage"
NB_PROCESS = AssetRef.make("notebook", {"olcf_item": "itm_nb"}, "nb")
LH = {"name": "lh", "id": "itm_lh"}


def lab(ref: AssetRef) -> str:
    """Short, stable label of a reference."""
    if ref.type == "column":
        sc = ref.scope_map
        if "table" in sc:
            return f"col:{sc['table']}.{(ref.name or '').lower()}"
        return f"col:{ref.alias_map.get('pbi') or ref.name}"
    if ref.type in ("table", "view"):
        return f"{ref.type}:{(ref.name or ref.alias_map.get('delta_location') or ref.alias_map.get('onelake_path') or '?').lower()}"
    if ref.type == "file":
        return f"file:{ref.alias_map.get('onelake_path') or (ref.name or '?')}" if ref.name is None else f"file:{ref.name}"
    if ref.alias_map.get("pbi"):
        return f"{ref.type}:{ref.alias_map['pbi']}"
    return f"{ref.type}:{ref.name or sorted(ref.alias_map.values())}"


def desc(e: EdgeFact, *, tr: bool = False, conf: bool = True) -> str:
    """``"READS_FROM notebook:nb -> table:lh.sales [derived]"``."""
    s = f"{e.type} {lab(e.src)} -> {lab(e.dst)}"
    if conf:
        s += f" [{e.effective_confidence()}]"
    if tr and e.transformation:
        s += f" | {e.transformation}"
    return s


def edges(f: Facts, *types: str, tr: bool = False, conf: bool = True) -> set[str]:
    """Descriptions of the edges of the given types (all when none given)."""
    return {desc(e, tr=tr, conf=conf) for e in f.edges if not types or e.type in types}


def edge_list(f: Facts, *types: str, tr: bool = False, conf: bool = True) -> list[str]:
    """Like :func:`edges` but keeps the order and duplicates."""
    return [desc(e, tr=tr, conf=conf) for e in f.edges if not types or e.type in types]


def find(f: Facts, etype: str, src: str, dst: str) -> EdgeFact:
    """The single edge ``etype`` from the node labelled ``src`` to ``dst`` (fails if there is none)."""
    got = [e for e in f.edges if e.type == etype and lab(e.src) == src and lab(e.dst) == dst]
    assert len(got) == 1, f"expected exactly one {etype} {src} -> {dst}, got {len(got)}; edges: {sorted(edges(f))}"
    return got[0]


def gaps(f: Facts, *kinds: str) -> list[tuple[str, str]]:
    """``[(kind, subject)]`` of the gaps (of the given kinds when given)."""
    return [(g.kind, g.subject) for g in f.gaps if not kinds or g.kind in kinds]


def gap_kinds(f: Facts) -> set[str]:
    """The set of gap kinds."""
    return {g.kind for g in f.gaps}


def gap_text(f: Facts) -> str:
    """All gap details joined (for substring assertions)."""
    return "\n".join(f"{g.kind}: {g.subject}: {g.detail}" for g in f.gaps)


def asset_labels(f: Facts, *types: str) -> set[str]:
    """Labels of the assets of the given types."""
    return {lab(a.ref) for a in f.assets if not types or a.ref.type in types}


def load_text(*parts: str) -> str:
    """Text of a corpus file below ``tests/corpus/lineage``."""
    return CORPUS.joinpath(*parts).read_text(encoding="utf-8")


def load_json(*parts: str) -> Any:
    """JSON of a corpus file below ``tests/corpus/lineage``."""
    return json.loads(load_text(*parts))


def load_parts(*parts: str) -> dict[str, str]:
    """All files below a corpus directory as ``{relative posix path: text}`` (the shape of a Fabric item definition)."""
    root = CORPUS.joinpath(*parts)
    return {p.relative_to(root).as_posix(): p.read_text(encoding="utf-8") for p in sorted(root.rglob("*")) if p.is_file()}


def ctx(**kw: Any) -> dict[str, Any]:
    """A plain context dict with the test lakehouse ``lh`` / ``itm_lh`` as default."""
    base: dict[str, Any] = {"default_lakehouse": dict(LH), "item_id": "itm_nb", "item_name": "nb"}
    base.update(kw)
    return base


def join_chain(join: str, count: int, tail: str = "", *, first: str = "t0") -> str:
    """``SELECT * FROM t0 <join> t1<tail> <join> t2<tail> ...`` with ``count`` joined tables (``{i}`` in ``tail`` is the table number)."""
    parts = " ".join(f"{join} t{i}{tail.replace('{i}', str(i))}" for i in range(1, count + 1))
    return f"SELECT * FROM {first} {parts}"  # noqa: S608 - a test fixture, never executed
