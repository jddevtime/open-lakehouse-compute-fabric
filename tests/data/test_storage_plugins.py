# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Entry-point plug-in discovery (``olcf.storage_providers``), provider catalogue and health checks."""

from __future__ import annotations

from importlib import metadata

import pytest

from olcf.storage.providers import ENTRY_POINT_GROUP, ProviderRegistry
from olcf.storage.providers.base import Capabilities, StorageProvider
from olcf.storage.providers.local import LocalFileSystem


class MemProvider(StorageProvider):
    id = "memtest"
    label = "In-memory test provider"
    schemes = ("memtest",)
    connection_types = ("memtest",)
    capabilities = Capabilities()

    def validate_root(self, root_uri: str) -> str:
        return root_uri

    def filesystem(self, root_uri, config, secret):  # noqa: ANN001, ANN201
        raise NotImplementedError

    def physical_uri(self, root_uri, rel=""):  # noqa: ANN001, ANN201
        return root_uri

    def spark_conf(self, root_uri, config, secret):  # noqa: ANN001, ANN201
        return {}


class Impostor(MemProvider):
    id = "s3"   # tries to replace a built-in


class _EP:
    def __init__(self, name: str, target):  # noqa: ANN001
        self.name, self.value, self._t = name, f"fake:{name}", target

    def load(self):  # noqa: ANN201
        if isinstance(self._t, Exception):
            raise self._t
        return self._t


def _patch(monkeypatch, *eps):  # noqa: ANN001, ANN202
    def fake(group=None, **kw):  # noqa: ANN001, ANN003, ANN202
        assert group == ENTRY_POINT_GROUP
        return list(eps)

    monkeypatch.setattr(metadata, "entry_points", fake)


def test_plugin_provider_is_loaded_from_entry_point(monkeypatch):
    _patch(monkeypatch, _EP("mem", MemProvider), _EP("factory", lambda: type("P2", (MemProvider,), {"id": "mem2", "schemes": ("mem2",), "connection_types": ()})()))
    reg = ProviderRegistry()
    assert {"local", "s3", "adls", "onelake", "memtest", "mem2"} <= {p.id for p in reg.all()}
    assert reg.get("memtest").label == "In-memory test provider"
    assert reg.for_scheme("memtest").id == "memtest" and reg.for_connection_type("memtest").id == "memtest"
    assert reg.plugin_errors == {}


def test_plugins_cannot_replace_builtins_and_broken_plugins_are_isolated(monkeypatch):
    _patch(monkeypatch, _EP("evil", Impostor), _EP("boom", ImportError("no module named gcs")), _EP("wrongtype", lambda: object()), _EP("good", MemProvider))
    reg = ProviderRegistry()
    assert type(reg.get("s3")).__name__ == "S3Provider"            # fail closed: built-in untouched
    assert reg.get("memtest")                                         # the healthy plug-in still loaded
    assert set(reg.plugin_errors) == {"evil", "boom", "wrongtype"}
    assert "already registered" in reg.plugin_errors["evil"] and "gcs" in reg.plugin_errors["boom"] and "StorageProvider" in reg.plugin_errors["wrongtype"]


def test_entry_point_discovery_failure_never_breaks_startup(monkeypatch):
    monkeypatch.setattr(metadata, "entry_points", lambda **kw: (_ for _ in ()).throw(RuntimeError("metadata broken")))
    assert {p.id for p in ProviderRegistry().all()} == {"local", "s3", "adls", "onelake"}


def test_registry_lookup_errors_are_actionable():
    from olcf.core.errors import NotFound

    reg = ProviderRegistry(load_entry_points=False)
    with pytest.raises(NotFound) as e:
        reg.get("gcs")
    assert "available" in str(e.value.details)
    with pytest.raises(NotFound):
        reg.for_scheme("gs")
    assert reg.for_root("abfss://ws@onelake.dfs.fabric.microsoft.com/x.Lakehouse").id == "onelake"
    assert reg.for_root("abfss://c@acct.dfs.core.windows.net/x").id == "adls"
    assert reg.for_root("s3://bucket/x").id == "s3" and reg.for_root("file:///tmp/x").id == "local"
    assert reg.for_connection_type("minio").id == "s3"


def test_provider_catalogue_endpoint_lists_fields_and_capabilities(dc):
    items = {p["id"]: p for p in dc.get("/v1/storage/providers", headers=dc.admin).json()["items"]}
    assert {"local", "s3", "adls", "onelake"} <= set(items)
    s3 = items["s3"]
    assert "minio" in s3["connection_types"] and s3["capabilities"]["multipart_write"] is True
    assert any(f.get("secret") for f in s3["fields"])
    assert items["adls"]["capabilities"]["atomic_rename"] is True and items["s3"]["capabilities"]["delta_commits"] != items["adls"]["capabilities"]["delta_commits"]
    assert "plugin_errors" in dc.get("/v1/storage/providers", headers=dc.admin).json()


def test_health_checks_registered_and_local_is_green(dc):
    h = dc.admin
    quick = dc.get("/v1/admin/health", headers=h).json()
    comps = {c["name"]: c for c in (quick.get("components") or quick.get("items") or [])}
    assert {"storage:local", "storage:s3", "storage:adls", "storage:onelake"} <= set(comps), list(comps)
    assert comps["storage:local"]["status"] == "ok"
    deep = dc.post("/v1/admin/health/checks/storage:local/test", headers=h).json()
    assert deep["status"] == "ok" and [s["name"] for s in deep["steps"]][-4:] == ["list", "write", "read", "delete"]


def test_connection_tester_registered_for_every_connection_type(dc):
    for t in ("local", "s3", "minio", "adls", "onelake"):
        assert t in dc.ctx.connection_testers, t
    ws = dc.workspace("ws1")
    c = dc.post("/v1/connections", json={"name": "bad-minio", "type": "minio", "workspace_id": ws["id"],
                                         "config": {"bucket": "b", "endpoint_url": "http://127.0.0.1:1", "path_style": True}}, headers=dc.admin).json()
    r = dc.post(f"/v1/connections/{c['id']}/test", headers=dc.admin).json()
    assert r["ok"] is False and r["steps"][0]["name"] == "resolve DNS"
    assert any(not s["ok"] and not s.get("skipped") for s in r["steps"])


def test_local_filesystem_class_is_exported():
    assert LocalFileSystem.provider == "local"
