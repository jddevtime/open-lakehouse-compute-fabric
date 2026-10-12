# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The async client against the real app through ``httpx.ASGITransport``."""

from __future__ import annotations

import asyncio

import pytest

from olcf.sdk import NotFoundError, OlcfTimeoutError


async def test_async_crud_and_errors(aclient):
    assert (await aclient.admin.health())["status"] == "ok"
    ws = await aclient.workspaces.create("awork")
    assert ws.etag
    assert (await aclient.workspaces.get("awork"))["id"] == ws.id
    with pytest.raises(NotFoundError):
        await aclient.workspaces.get("nope")
    await aclient.workspaces.delete("awork")


async def test_async_concurrent_calls_and_pagination(aclient):
    ws = await aclient.workspaces.create("aconc")
    await asyncio.gather(*[aclient.items.create("aconc", "sql_query", f"q{i}", definition={"sql": "select 1", "engine": "duckdb"}) for i in range(5)])
    got = [i async for i in aclient.paginate(aclient.items.list, ws.id, limit=2)]
    assert len(got) == 5


async def test_async_jobs_wait_logs_and_idempotency(aclient):
    ws = await aclient.workspaces.create("ajobs")
    nb = await aclient.items.create("ajobs", "notebook", "n")
    a = await aclient.notebooks.run(nb.id, idempotency_key="k-1")
    b = await aclient.notebooks.run(nb.id, idempotency_key="k-1")
    assert a["id"] == b["id"]
    with pytest.raises(OlcfTimeoutError):
        await aclient.jobs.wait(a["id"], timeout=0.3, poll=0.1)
    await aclient.jobs.cancel(a["id"])
    done = await aclient.jobs.wait(a["id"], timeout=10)
    assert done["state"] == "cancelled"
    assert [c async for c in aclient.jobs.logs(a["id"], follow=True, poll=0.05)] == []
    assert a["id"] in [j["id"] async for j in aclient.jobs.iter(workspace_id=ws.id)]


async def test_async_sql_and_bytes(aclient):
    await aclient.workspaces.create("asql")
    lh = await aclient.lakehouses.create("asql", "lh")
    await aclient.lakehouses.write_file(lh.id, "Files/x.bin", b"\x00\x01\x02")
    assert await aclient.lakehouses.read_file(lh.id, "Files/x.bin") == b"\x00\x01\x02"
    assert (await aclient.sql.execute("select 41+1 as answer"))["rows"] == [[42]]
