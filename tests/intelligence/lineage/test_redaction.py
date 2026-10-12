# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Credentials in analysed code never reach the facts (evidence snippets, gaps, asset names / attributes, aliases)."""

from __future__ import annotations

import json

import pytest

from olcf.intelligence.lineage.base import LineageContext, classify_path, redact_secrets, strip_uri_secrets
from olcf.intelligence.lineage.engine import analyze_definition
from olcf.intelligence.lineage.mcode import analyze_m
from olcf.intelligence.lineage.notebook import analyze_notebook
from olcf.intelligence.lineage.pyspark import analyze_python
from olcf.intelligence.lineage.semantic_model import analyze_semantic_model
from olcf.intelligence.lineage.sql import analyze_sql
from olcf.intelligence.model import AssetRef, DefinitionFact, Facts
from tests.intelligence.lineage.support import LH, edges

SECRETS = ["hunter2", "SECRETSIG", "SECRETKEY", "AKIAIOSFODNN7EXAMPLE", "bearertoken123456", "KEYVAL", "s3cr3t-value"]


def dump(f: Facts) -> str:
    """Every string of the facts (refs, names, attrs, evidence, gaps) as one text."""
    return json.dumps(
        {
            "assets": [(repr(a.ref), a.name, a.attrs, a.description, a.path) for a in f.assets],
            "edges": [(repr(e.src), repr(e.dst), e.transformation, e.attrs, [(ev.snippet, ev.location, ev.ref) for ev in e.evidence]) for e in f.edges],
            "gaps": [(g.subject, g.detail, g.remediation, g.location) for g in f.gaps],
        },
        default=str,
    )


def assert_clean(f: Facts) -> None:
    """No known secret value appears anywhere in the facts (case-insensitively, aliases are lower-cased)."""
    text = dump(f).lower()
    for s in SECRETS:
        assert s.lower() not in text, s


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('.option("password","hunter2")', '.option("password","***")'),
        ("password=hunter2;user=u", "password=***;user=u"),
        ('pwd = "a b c"', 'pwd = "***"'),
        ('{"password": "x", "user": "u"}', '{"password": "***", "user": "u"}'),
        ("https://a.b/c?sv=1&sig=ABC%2F&x=1", "https://a.b/c?sv=1&sig=***&x=1"),
        ("s3a://AKIAIOSFODNN7EXAMPLE:SECRET@bucket/p", "s3a://***@bucket/p"),
        ('Authorization = "Bearer abcdefghijkl"', 'Authorization = "***"'),
        ("Bearer abcdefghijkl", "Bearer ***"),
        ("client_secret=abc123&grant=x", "client_secret=***&grant=x"),
        ('spark.conf.set("fs.azure.account.key.x.dfs.core.windows.net", "KEYVAL")', 'spark.conf.set("fs.azure.account.key.x.dfs.core.windows.net", "***")'),
        ("jdbc:sqlserver://h;user=u;password=hunter2;encrypt=true", "jdbc:sqlserver://h;user=u;password=***;encrypt=true"),
        ("AKIAIOSFODNN7EXAMPLE", "***"),
    ],
)
def test_redact_secrets_masks_the_value_and_keeps_the_rest(text: str, expected: str) -> None:
    assert redact_secrets(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "SELECT password FROM users",
        "u.password = p.password",
        "token_count = 5",
        "tokens=3",
        "def f(token: str): pass",
        '.option("header","true")',
        '.option("key_column","id")',
        '.option("dbtable","dbo.t")',
        "spark.read.table('t')",
        "",
    ],
)
def test_ordinary_code_is_left_alone(text: str) -> None:
    assert redact_secrets(text) == text


def test_redact_secrets_handles_none_and_is_idempotent() -> None:
    assert redact_secrets(None) is None
    once = redact_secrets("password=hunter2 and token=abc")
    assert redact_secrets(once) == once


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("https://u:p@h/a?sig=1#f", "https://h/a"),
        ("s3a://AK:SEC@b/p", "s3a://b/p"),
        ("abfss://c@acct.dfs.core.windows.net/lh/Tables/t", "abfss://c@acct.dfs.core.windows.net/lh/Tables/t"),
        ("abfss://<?>@onelake.dfs.fabric.microsoft.com/<?>/Files/x.json", "abfss://<?>@onelake.dfs.fabric.microsoft.com/<?>/Files/x.json"),
        ("https://h/<?>/a?x=<?>", "https://h/<?>/a"),
        ("Files/x?y", "Files/x?y"),
        ("not a uri", "not a uri"),
    ],
)
def test_strip_uri_secrets(uri: str, expected: str) -> None:
    assert strip_uri_secrets(uri) == expected


def test_classify_path_drops_query_and_userinfo() -> None:
    t = classify_path("https://acct.blob.core.windows.net/c/f.csv?sv=2020&sig=SECRETSIG")
    assert t.kind == "file" and "SECRETSIG" not in repr(t) and "secretsig" not in repr(t).lower()
    assert classify_path("s3a://AKIAKEY:SECRETKEY@bucket/path").name == "s3a://bucket/path"


PY = '''
df = spark.read.format("jdbc").option("url","jdbc:sqlserver://h:1433;databaseName=d;user=u;password=hunter2").option("dbtable","dbo.t").option("password","hunter2").load()
df.write.saveAsTable("x")
d2 = spark.read.csv("https://acct.blob.core.windows.net/c/f.csv?sv=2020&sig=SECRETSIG")
d2.write.saveAsTable("y")
d3 = spark.read.parquet("s3a://AKIAKEY:SECRETKEY@bucket/path")
d3.write.saveAsTable("z")
spark.conf.set("fs.azure.account.key.acct.dfs.core.windows.net", "KEYVAL")
d4 = spark.read.parquet("abfss://c@acct.dfs.core.windows.net/p")
d4.write.saveAsTable("w")
'''


def test_pyspark_snippets_names_and_aliases_are_clean() -> None:
    f = analyze_python(PY, context={"item_id": "itm_x", "default_lakehouse": dict(LH)})
    assert_clean(f)
    got = edges(f, "READS_FROM", "WRITES_TO")
    assert any(e.endswith("-> table:x [derived]") for e in got) and any("-> table:w [derived]" in e for e in got)
    # the lineage itself is unaffected: the JDBC table is still named, the HTTPS file is still a file
    assert any("external:jdbc:sqlserver://h:1433/dbo.t" in e for e in got)
    assert any("file:https://acct.blob.core.windows.net/c/f.csv" in e for e in got)


def test_notebook_cells_are_clean() -> None:
    nb = {"cells": [{"cell_type": "code", "source": PY}], "metadata": {}, "nbformat": 4}
    assert_clean(analyze_notebook(nb, context={"item_id": "itm_x", "default_lakehouse": dict(LH)}))


def test_sql_snippets_and_query_asset_text_are_clean() -> None:
    sql = "SELECT * FROM read_csv('https://a.blob.core.windows.net/c/f.csv?sig=SECRETSIG') WHERE password = 'hunter2'"
    f = analyze_sql(sql, dialect="duckdb", context={})
    assert_clean(f)
    assert any(e.dst.type == "file" for e in f.edges)
    q = next(a for a in f.assets if a.ref.type == "sql_query")
    assert "hunter2" not in json.dumps(q.attrs) and "hunter2" not in (q.name or "")


def test_m_urls_and_connection_options_are_clean() -> None:
    m = 'let Source = Json.Document(Web.Contents("https://api.example.com/v1/items?api_key=SECRETKEY", [Headers=[Authorization="Bearer bearertoken123456"]])) in Source'
    f = analyze_m(m, ctx=LineageContext())
    assert f.sources
    text = repr(f.sources).lower()
    assert "secretkey" not in text and "bearertoken" not in text
    assert "api.example.com/v1/items" in text


def test_semantic_model_expression_attributes_are_clean() -> None:
    parts = {
        "definition/model.tmdl": "model Model\n  culture: en-US\n",
        "definition/tables/T.tmdl": (
            "table T\n  measure Pw = \"s3cr3t-value\"\n\n  column a\n    dataType: string\n    sourceColumn: a\n\n"
            "  partition P = m\n    mode: import\n    source =\n      let\n"
            "        Source = Sql.Database(\"srv.example.com\", \"erp\", [Query=\"select a from t where pwd='s3cr3t-value'\"]),\n"
            "        Hidden = [password=\"hunter2\"]\n      in\n        Source\n"
        ),
    }
    f = analyze_semantic_model(parts, context={"item_id": "11111111-1111-4111-8111-111111111111", "fabric_workspace": "22222222-2222-4222-8222-222222222222"})
    assert f.assets
    text = dump(f)
    assert "hunter2" not in text and "pwd='s3cr3t-value'" not in text


def test_engine_level_gap_texts_are_clean() -> None:
    d = DefinitionFact(kind="notebook", ref=AssetRef.make("notebook", {"olcf_item": "itm_x"}, "X"), name="X", payload={"cells": [{"cell_type": "code", "source": "x = (password=hunter2"}]},
                       context={"item_id": "itm_x"})
    f = analyze_definition(d)
    assert_clean(f)


def test_redaction_never_changes_edge_counts() -> None:
    clean = PY.replace("hunter2", "x1").replace("SECRETSIG", "x2").replace("SECRETKEY", "x3").replace("KEYVAL", "x4")
    a = analyze_python(PY, context={"item_id": "itm_x", "default_lakehouse": dict(LH)})
    b = analyze_python(clean, context={"item_id": "itm_x", "default_lakehouse": dict(LH)})
    assert len(a.edges) == len(b.edges) and len(a.gaps) == len(b.gaps)
