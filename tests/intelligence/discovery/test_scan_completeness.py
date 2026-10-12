# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Review finding F10 (runner side): a scan in which a definition could not be analysed completely is ``partial`` - never ``succeeded`` - and nothing is swept on its strength.

Lineage analyzers report failure as gaps inside the facts they return rather than as exceptions. The engine counts them (``ApplyStats.blocking_gaps`` / ``incomplete``) and keeps the
last known lineage of an incompletely analysed definition; the runner turns the count into the scan status. These tests use the real local connector, the real analyzers and the
real engine.
"""

from __future__ import annotations

from typing import Any

import pytest

from olcf.core.models import ItemUpdate
from olcf.intelligence.engine import ApplyStats
from olcf.security.rbac import SYSTEM_PRINCIPAL
from tests.intelligence.discovery.conftest import people_table

# The engine side (CORE's ApplyStats.blocking_gaps / incomplete, and keeping the last known edges of an incompletely analysed definition) is a separate change. Until it exists these tests are
# expected to fail; they become ordinary tests the moment the fields exist (strict=False: no one has to remember to remove the marker).
pytestmark = pytest.mark.xfail(not hasattr(ApplyStats, "incomplete"), reason="pending the engine's ApplyStats.incomplete / blocking_gaps (CORE)", strict=False)

GOOD = "df = spark.table('people')\ndf.write.mode('overwrite').saveAsTable('people_copy')\n"
BAD = "spark.sql('SELEC id FROM FROM people((')\n"
READ_ONLY = "df = spark.table('people')\n"


def edit(disc: Any, source: str) -> None:
    items = disc.ctx.service("items")
    it = items.find(disc.ws["id"], "notebook", "etl")
    definition = it["definition"]
    definition["cells"][0]["source"] = source
    items.update(SYSTEM_PRINCIPAL, it["id"], ItemUpdate(definition=definition, comment="test edit"), None)


def active(disc: Any) -> set[str]:
    return {e["type"] for e in disc.edges() if e["status"] == "active"}


def setup(disc: Any) -> None:
    disc.lakehouse()
    disc.write("people", people_table())
    disc.notebook("etl", GOOD)
    first = disc.scan()
    assert first["status"] == "succeeded" and active(disc) == {"READS_FROM", "WRITES_TO"}


def test_a_definition_that_no_longer_parses_makes_the_scan_partial_and_sweeps_nothing(disc):
    setup(disc)
    edit(disc, BAD)
    row = disc.scan()
    assert row["status"] == "partial" and "sweep" not in row["stats"]
    assert "analysed completely" in (row["error"] or "")
    assert any(g["kind"] == "parse_error" for g in disc.gaps())
    assert not disc.assets(status="removed", type="table")


def test_the_last_known_lineage_survives_an_incomplete_analysis(disc):
    setup(disc)
    edit(disc, BAD)
    disc.scan()
    assert active(disc) == {"READS_FROM", "WRITES_TO"}                          # not a false clean graph


def test_repairing_the_definition_retires_only_the_edges_that_really_went(disc):
    setup(disc)
    edit(disc, BAD)
    assert disc.scan()["status"] == "partial"
    edit(disc, READ_ONLY)
    row = disc.scan()
    assert row["status"] == "succeeded"
    assert active(disc) == {"READS_FROM"} and {e["type"] for e in disc.edges() if e["status"] == "removed"} == {"WRITES_TO"}
    assert not [g for g in disc.gaps() if g["kind"] == "parse_error"]
