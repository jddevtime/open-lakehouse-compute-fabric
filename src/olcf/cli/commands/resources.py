# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Data-plane commands: ``workspace``, ``item``, ``notebook``, ``lakehouse``, ``connection``, ``secret``, ``sql``, ``catalog``, ``search``.

Every list command accepts the global ``-o json|yaml|csv``; names are accepted wherever an id is (workspaces, items, lakehouses).  Secret values
are write-only in the API and are never printed; they are read from a prompt, stdin or an environment variable, not from arguments.
"""

from __future__ import annotations

import csv
import io
import json
import os
import sys
from pathlib import Path
from typing import Any

import click

from olcf.cli.commands.jobs import await_job, render_job, ws_id
from olcf.cli.common import (
    confirm,
    load_json_arg,
    parse_kv,
    pass_state,
    read_input,
    resolve_item,
    secret_prompt,
    workspace_option,
    write_or_print,
    yes_option,
)
from olcf.cli.output import human_bytes
from olcf.cli.state import State
from olcf.core.redaction import redact_text
from olcf.sdk.contexts import ContextStore


# --------------------------------------------------------------------------------------------- workspace
@click.group("workspace", help="Workspaces: the unit of organisation and access control.")
def workspace_group() -> None:
    pass


@workspace_group.command("list")
@click.option("-q", "--query", "q", default=None)
@click.option("--deleted", is_flag=True, help="Include soft-deleted workspaces.")
@pass_state
def ws_list(state: State, q: str | None, deleted: bool) -> None:
    c = state.client
    rows = list(c.paginate(c.workspaces.list, q=q, deleted=deleted))
    cur = state.resolve_connection().get("workspace")
    for r in rows:
        r["default"] = "*" if r.get("name") == cur else ""
    state.out.table(rows, [("", "default"), ("ID", "id"), ("NAME", "name", 30), ("DISPLAY NAME", "display_name", 30), ("STAGE", "stage"), ("UPDATED", "updated_at")], empty="no workspaces")


@workspace_group.command("get")
@click.argument("workspace")
@pass_state
def ws_get(state: State, workspace: str) -> None:
    state.out.emit(state.client.workspaces.get(workspace), lambda d: state.out.kv(list(d.items())))


@workspace_group.command("create")
@click.argument("name")
@click.option("--display-name")
@click.option("--description", default="")
@click.option("--tag", "tags", multiple=True)
@pass_state
def ws_create(state: State, name: str, display_name: str | None, description: str, tags: tuple[str, ...]) -> None:
    ws = state.client.workspaces.create(name, display_name=display_name, description=description, tags=list(tags))
    state.out.emit(ws, lambda d: state.out.success(f"created workspace {d['name']} ({d['id']})"))


@workspace_group.command("delete", help="Soft-delete a workspace (restorable with `restore`).")
@click.argument("workspace")
@yes_option
@pass_state
def ws_delete(state: State, workspace: str, yes: bool) -> None:
    confirm(state, f"Delete workspace '{workspace}' and everything in it?", yes)
    state.client.workspaces.delete(workspace)
    state.out.emit({"deleted": workspace}, lambda d: state.out.success(f"deleted {d['deleted']}"))


@workspace_group.command("restore")
@click.argument("workspace")
@pass_state
def ws_restore(state: State, workspace: str) -> None:
    state.out.emit(state.client.workspaces.restore(workspace), lambda d: state.out.success(f"restored {d.get('name', workspace)}"))


@workspace_group.command("use", help="Remember a default workspace for the current context.")
@click.argument("workspace")
@pass_state
def ws_use(state: State, workspace: str) -> None:
    ws = state.client.workspaces.get(workspace)
    store = ContextStore()
    name = state.resolve_connection().get("context") or store.current
    if not name:
        raise click.ClickException("no context to attach the default to: run `olcf login` first (or set OLCF_WORKSPACE)")
    store.set(name, make_current=False, workspace=ws["name"])
    state.out.emit({"context": name, "workspace": ws["name"]}, lambda d: state.out.success(f"context '{d['context']}' now defaults to workspace '{d['workspace']}'"))


@workspace_group.command("members")
@click.argument("workspace")
@pass_state
def ws_members(state: State, workspace: str) -> None:
    rows = state.client.workspaces.members(workspace)
    state.out.table(rows if isinstance(rows, list) else rows.get("items", []), [("PRINCIPAL", "principal_id"), ("NAME", "name"), ("ROLE", "role"), ("VIA", "source")])


@workspace_group.command("export", help="Export items, notebooks and lakehouse definitions as a zip (no secrets, no data files).")
@click.argument("workspace")
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None, help="Output file (default WORKSPACE.zip).")
@pass_state
def ws_export(state: State, workspace: str, out_file: str | None) -> None:
    data = state.client.workspaces.export_zip(workspace)
    write_or_print(state, data, out_file or f"{workspace}.zip")


@workspace_group.command("import", help="Import a workspace zip created by `workspace export`.")
@click.argument("file", type=click.Path(exists=True, dir_okay=False))
@click.option("--name", "new_name", default=None, help="Name for the imported workspace.")
@click.option("--into", "target", default=None, help="Merge into an existing workspace.")
@click.option("--on-conflict", type=click.Choice(["skip", "overwrite", "rename", "fail"]), default=None)
@pass_state
def ws_import(state: State, file: str, new_name: str | None, target: str | None, on_conflict: str | None) -> None:
    p = Path(file)
    res = state.client.workspaces.import_zip(p.read_bytes(), filename=p.name, target_workspace=target, new_name=new_name, on_conflict=on_conflict)
    state.out.emit(res, lambda d: state.out.success("import finished: " + ", ".join(f"{k}={v}" for k, v in d.items() if isinstance(v, int))))


@workspace_group.group("git", help="Git integration of a workspace (needs `git` on the SERVER; see `olcf workspace git capabilities`).")
def ws_git() -> None:
    pass


@ws_git.command("capabilities")
@click.argument("workspace")
@pass_state
def ws_git_caps(state: State, workspace: str) -> None:
    state.out.emit(state.client.workspaces.git_capabilities(workspace), lambda d: state.out.kv(list(d.items())))


@ws_git.command("status")
@click.argument("workspace")
@click.option("--fetch", is_flag=True)
@pass_state
def ws_git_status(state: State, workspace: str, fetch: bool) -> None:
    state.out.emit(state.client.workspaces.git_status(workspace, fetch=fetch), None)


@ws_git.command("connect")
@click.argument("workspace")
@click.option("--repo", "repo_url", required=True)
@click.option("--branch", default="main")
@click.option("--folder", default="")
@click.option("--connection", default=None, help="Connection holding the Git credentials.")
@pass_state
def ws_git_connect(state: State, workspace: str, repo_url: str, branch: str, folder: str, connection: str | None) -> None:
    state.out.emit(state.client.workspaces.git_connect(workspace, repo_url=repo_url, branch=branch, folder=folder, connection=connection), None)


@ws_git.command("commit")
@click.argument("workspace")
@click.option("-m", "--message", required=True)
@pass_state
def ws_git_commit(state: State, workspace: str, message: str) -> None:
    state.out.emit(state.client.workspaces.git_commit(workspace, message), None)


@ws_git.command("pull")
@click.argument("workspace")
@click.option("--dry-run", is_flag=True)
@click.option("--resolution", type=click.Choice(["ours", "theirs"]), default=None)
@pass_state
def ws_git_pull(state: State, workspace: str, dry_run: bool, resolution: str | None) -> None:
    state.out.emit(state.client.workspaces.git_pull(workspace, dry_run=dry_run, resolution=resolution), None)


@ws_git.command("disconnect")
@click.argument("workspace")
@pass_state
def ws_git_disconnect(state: State, workspace: str) -> None:
    state.out.emit(state.client.workspaces.git_disconnect(workspace), None)


# --------------------------------------------------------------------------------------------- item
@click.group("item", help="Generic workspace items (notebooks, SQL queries, pipelines, environments ...).")
def item_group() -> None:
    pass


ITEM_TYPES = ["notebook", "lakehouse", "sql_query", "pipeline", "environment", "spark_job_definition", "variable_set", "schedule", "report", "extension"]


@item_group.command("list")
@workspace_option
@click.option("--type", "type_", type=click.Choice(ITEM_TYPES), default=None)
@click.option("-q", "--query", "q", default=None)
@click.option("--folder", default=None)
@click.option("--deleted", is_flag=True)
@pass_state
def item_list(state: State, workspace: str | None, type_: str | None, q: str | None, folder: str | None, deleted: bool) -> None:
    c = state.client
    rows = list(c.paginate(c.items.list, state.default_workspace(workspace), type=type_, q=q, folder=folder, deleted=deleted))
    state.out.table(rows, [("ID", "id"), ("TYPE", "type"), ("NAME", "name", 34), ("FOLDER", "folder"), ("VERSION", "version"), ("UPDATED", "updated_at")], empty="no items")


@item_group.command("get")
@click.argument("item")
@workspace_option
@click.option("--definition", "show_def", is_flag=True, help="Print only the definition JSON.")
@pass_state
def item_get(state: State, item: str, workspace: str | None, show_def: bool) -> None:
    it = resolve_item(state, workspace, item)
    full = state.client.items.get(it["id"])
    state.out.emit(full.get("definition", {}) if show_def else full, None if show_def else (lambda d: state.out.kv([(k, v) for k, v in d.items() if k != "definition"])))


@item_group.command("create")
@click.argument("name")
@click.option("--type", "type_", type=click.Choice(ITEM_TYPES), required=True)
@click.option("--definition", "def_file", type=click.Path(dir_okay=False, allow_dash=True), default=None, help="JSON definition file ('-' = stdin).")
@click.option("--description", default="")
@click.option("--folder", default="")
@workspace_option
@pass_state
def item_create(state: State, name: str, type_: str, def_file: str | None, description: str, folder: str, workspace: str | None) -> None:
    it = state.client.items.create(state.default_workspace(workspace), type_, name,  # type: ignore[arg-type]  # validated by the click.Choice above
                                   definition=load_json_arg(def_file) if def_file else None, description=description, folder=folder)
    state.out.emit(it, lambda d: state.out.success(f"created {d['type']} {d['name']} ({d['id']})"))


@item_group.command("delete")
@click.argument("item")
@workspace_option
@yes_option
@pass_state
def item_delete(state: State, item: str, workspace: str | None, yes: bool) -> None:
    it = resolve_item(state, workspace, item)
    confirm(state, f"Delete {it['type']} '{it['name']}'?", yes)
    state.client.items.delete(it["id"])
    state.out.emit({"deleted": it["id"]}, lambda d: state.out.success(f"deleted {d['deleted']} (restorable: olcf item restore)"))


@item_group.command("restore")
@click.argument("item_id")
@pass_state
def item_restore(state: State, item_id: str) -> None:
    state.out.emit(state.client.items.restore(item_id), lambda d: state.out.success(f"restored {d.get('name', item_id)}"))


@item_group.command("versions")
@click.argument("item")
@workspace_option
@pass_state
def item_versions(state: State, item: str, workspace: str | None) -> None:
    it = resolve_item(state, workspace, item)
    rows = state.client.items.versions(it["id"])
    state.out.table(list(rows), [("VERSION", "version"), ("BY", "created_by"), ("AT", "created_at"), ("COMMENT", "comment", 50)])


@item_group.command("revert")
@click.argument("item")
@click.argument("version", type=int)
@workspace_option
@pass_state
def item_revert(state: State, item: str, version: int, workspace: str | None) -> None:
    it = resolve_item(state, workspace, item)
    state.out.emit(state.client.items.revert(it["id"], version), lambda d: state.out.success(f"{it['name']} reverted to version {version} (now v{d.get('version')})"))


# --------------------------------------------------------------------------------------------- notebook
@click.group("notebook", help="Notebooks: import, export, parameterised runs.")
def notebook_group() -> None:
    pass


@notebook_group.command("list")
@workspace_option
@pass_state
def nb_list(state: State, workspace: str | None) -> None:
    c = state.client
    rows = list(c.paginate(c.items.list, state.default_workspace(workspace), type="notebook"))
    state.out.table(rows, [("ID", "id"), ("NAME", "name", 40), ("FOLDER", "folder"), ("VERSION", "version"), ("UPDATED", "updated_at")], empty="no notebooks")


@notebook_group.command("import", help="Import .ipynb, .py or Fabric git-source notebooks.")
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True, dir_okay=False))
@workspace_option
@click.option("--on-conflict", type=click.Choice(["skip", "overwrite", "rename", "fail"]), default=None)
@click.option("--folder", default=None)
@click.option("--strip-outputs", is_flag=True)
@pass_state
def nb_import(state: State, files: tuple[str, ...], workspace: str | None, on_conflict: str | None, folder: str | None, strip_outputs: bool) -> None:
    payload = [(Path(f).name, Path(f).read_bytes()) for f in files]
    res = state.client.notebooks.import_files(state.default_workspace(workspace), payload, on_conflict=on_conflict, folder=folder, strip_outputs=strip_outputs)
    rows = res.get("items") or res.get("results") or []

    def human(d: dict[str, Any]) -> None:
        state.out.success("import finished")
        if rows:
            state.out.table(rows, [("FILE", "filename"), ("RESULT", "result"), ("NAME", "name"), ("ID", "id")])

    state.out.emit(res, human)


@notebook_group.command("export")
@click.argument("notebook")
@workspace_option
@click.option("-f", "--format", "fmt", type=click.Choice(["ipynb", "py", "fabric_git_source"]), default=None)
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None, help="Write to a file (default: stdout).")
@pass_state
def nb_export(state: State, notebook: str, workspace: str | None, fmt: str | None, out_file: str | None) -> None:
    it = resolve_item(state, workspace, notebook, "notebook")
    write_or_print(state, state.client.notebooks.export(it["id"], format=fmt), out_file)


@notebook_group.command("run", help="Run a notebook as a job.  Parameters override the cell tagged `parameters`.")
@click.argument("notebook")
@workspace_option
@click.option("-p", "--param", "params", multiple=True, help="k=v (JSON values allowed); repeatable.")
@click.option("--engine", type=click.Choice(["spark", "python"]), default=None)
@click.option("--runtime", "runtime_id", default=None)
@click.option("--wait", is_flag=True)
@click.option("-f", "--follow", is_flag=True)
@click.option("--timeout", type=float, default=None)
@click.option("--idempotency-key", default=None)
@pass_state
def nb_run(state: State, notebook: str, workspace: str | None, params: tuple[str, ...], engine: str | None, runtime_id: str | None, wait: bool, follow: bool,
           timeout: float | None, idempotency_key: str | None) -> None:
    it = resolve_item(state, workspace, notebook, "notebook")
    job = state.client.notebooks.run(it["id"], parameters=parse_kv(params), engine=engine, runtime={"runtime_id": runtime_id} if runtime_id else None, idempotency_key=idempotency_key)
    state.out.success(f"submitted {job['id']}")
    final = await_job(state, job, wait=wait or follow, follow=follow, timeout=timeout)
    state.out.emit(final, lambda j: render_job(state, j))


@notebook_group.command("runs", help="Recent runs of a notebook.")
@click.argument("notebook")
@workspace_option
@click.option("--state", "st", default=None)
@click.option("--limit", type=int, default=20)
@pass_state
def nb_runs(state: State, notebook: str, workspace: str | None, st: str | None, limit: int) -> None:
    it = resolve_item(state, workspace, notebook, "notebook")
    rows = state.client.notebooks.runs(it["id"], state=st, limit=limit)
    state.out.table(list(rows), [("JOB", "id"), ("STATE", "state"), ("CREATED", "created_at"), ("FINISHED", "finished_at")])


@notebook_group.command("parameters", help="Show the parameters a notebook accepts.")
@click.argument("notebook")
@workspace_option
@pass_state
def nb_parameters(state: State, notebook: str, workspace: str | None) -> None:
    it = resolve_item(state, workspace, notebook, "notebook")
    state.out.emit(state.client.notebooks.parameters(it["id"]), None)


# --------------------------------------------------------------------------------------------- lakehouse
def resolve_lakehouse(state: State, ref: str, workspace: str | None) -> str:
    if ref.startswith("itm_"):
        return ref
    ws = state.default_workspace(workspace)
    c = state.client
    for lh in c.paginate(c.lakehouses.list_in_workspace, ws, q=ref):
        if lh.get("name") == ref:
            return lh["id"]
    raise click.ClickException(f"no lakehouse named '{ref}' in workspace '{ws}'")


@click.group("lakehouse", help="Lakehouses: files and Delta/Iceberg tables.")
def lakehouse_group() -> None:
    pass


@lakehouse_group.command("list")
@workspace_option
@click.option("--all-workspaces", is_flag=True)
@pass_state
def lh_list(state: State, workspace: str | None, all_workspaces: bool) -> None:
    c = state.client
    rows = list(c.paginate(c.lakehouses.list)) if all_workspaces else list(c.paginate(c.lakehouses.list_in_workspace, state.default_workspace(workspace)))
    state.out.table(rows, [("ID", "id"), ("NAME", "name", 30), ("UPDATED", "updated_at")], empty="no lakehouses")


@lakehouse_group.command("create")
@click.argument("name")
@workspace_option
@click.option("--root-uri", default=None, help="Storage location (default: managed local storage). e.g. s3://bucket/prefix, abfss://fs@acct.dfs.core.windows.net/p")
@click.option("--connection", "connection_id", default=None, help="Connection with credentials for --root-uri.")
@click.option("--format", "default_format", type=click.Choice(["delta", "iceberg", "parquet"]), default="delta")
@click.option("--description", default="")
@pass_state
def lh_create(state: State, name: str, workspace: str | None, root_uri: str | None, connection_id: str | None, default_format: str, description: str) -> None:
    lh = state.client.lakehouses.create(state.default_workspace(workspace), name, root_uri=root_uri, connection_id=connection_id, default_format=default_format, description=description)
    state.out.emit(lh, lambda d: state.out.success(f"created lakehouse {d['name']} ({d['id']})"))


@lakehouse_group.command("get")
@click.argument("lakehouse")
@workspace_option
@pass_state
def lh_get(state: State, lakehouse: str, workspace: str | None) -> None:
    state.out.emit(state.client.lakehouses.get(resolve_lakehouse(state, lakehouse, workspace)), lambda d: state.out.kv(list(d.items())))


@lakehouse_group.command("delete")
@click.argument("lakehouse")
@workspace_option
@yes_option
@pass_state
def lh_delete(state: State, lakehouse: str, workspace: str | None, yes: bool) -> None:
    confirm(state, f"Delete lakehouse '{lakehouse}'? (files stay in storage; the registration is removed)", yes)
    state.client.lakehouses.delete(resolve_lakehouse(state, lakehouse, workspace))
    state.out.emit({"deleted": lakehouse}, lambda d: state.out.success(f"deleted {d['deleted']}"))


@lakehouse_group.command("test", help="Check that the storage location is reachable and writable.")
@click.argument("lakehouse")
@workspace_option
@pass_state
def lh_test(state: State, lakehouse: str, workspace: str | None) -> None:
    res = state.client.lakehouses.test(resolve_lakehouse(state, lakehouse, workspace))
    state.out.emit(res, lambda d: state.out.table(d.get("steps") or [d], [("STEP", "name"), ("OK", "ok"), ("DETAIL", "detail", 70)]))
    if res.get("ok") is False:
        raise SystemExit(1)


@lakehouse_group.group("files", help="Browse and transfer files.")
def lh_files() -> None:
    pass


@lh_files.command("ls")
@click.argument("lakehouse")
@click.argument("path", default="")
@workspace_option
@click.option("-r", "--recursive", is_flag=True)
@pass_state
def files_ls(state: State, lakehouse: str, path: str, workspace: str | None, recursive: bool) -> None:
    lh = resolve_lakehouse(state, lakehouse, workspace)
    c = state.client
    rows = list(c.paginate(c.lakehouses.list_files, lh, path, recursive=recursive))
    shown = [{**r, "kind": "dir" if r.get("is_dir") else "file", "size_h": "-" if r.get("is_dir") else human_bytes(r.get("size"))} for r in rows] if state.out.fmt in ("table", "csv") else rows
    state.out.table(shown, [("TYPE", "kind"), ("SIZE", "size_h"), ("MODIFIED", "modified"), ("PATH", "path", 80)], empty="(empty)")


@lh_files.command("cat", help="Write a file to stdout (or -o FILE).")
@click.argument("lakehouse")
@click.argument("path")
@workspace_option
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None)
@pass_state
def files_cat(state: State, lakehouse: str, path: str, workspace: str | None, out_file: str | None) -> None:
    write_or_print(state, state.client.lakehouses.read_file(resolve_lakehouse(state, lakehouse, workspace), path), out_file)


@lh_files.command("put", help="Upload a local file ('-' = stdin).")
@click.argument("lakehouse")
@click.argument("local", type=click.Path(allow_dash=True))
@click.argument("remote")
@workspace_option
@click.option("--overwrite", is_flag=True)
@pass_state
def files_put(state: State, lakehouse: str, local: str, remote: str, workspace: str | None, overwrite: bool) -> None:
    data = read_input(local, binary=True)
    res = state.client.lakehouses.write_file(resolve_lakehouse(state, lakehouse, workspace), remote, data, overwrite=overwrite)
    state.out.emit(res, lambda d: state.out.success(f"uploaded {len(data)} bytes to {remote}"))


@lh_files.command("rm")
@click.argument("lakehouse")
@click.argument("path")
@workspace_option
@click.option("-r", "--recursive", is_flag=True)
@yes_option
@pass_state
def files_rm(state: State, lakehouse: str, path: str, workspace: str | None, recursive: bool, yes: bool) -> None:
    confirm(state, f"Delete {path}{' recursively' if recursive else ''}?", yes)
    state.client.lakehouses.delete_file(resolve_lakehouse(state, lakehouse, workspace), path, recursive=recursive)
    state.out.emit({"deleted": path}, lambda d: state.out.success(f"deleted {d['deleted']}"))


@lh_files.command("mkdir")
@click.argument("lakehouse")
@click.argument("path")
@workspace_option
@pass_state
def files_mkdir(state: State, lakehouse: str, path: str, workspace: str | None) -> None:
    state.out.emit(state.client.lakehouses.mkdir(resolve_lakehouse(state, lakehouse, workspace), path), lambda d: state.out.success(f"created {path}"))


@lh_files.command("mv")
@click.argument("lakehouse")
@click.argument("src")
@click.argument("dst")
@workspace_option
@click.option("--overwrite", is_flag=True)
@pass_state
def files_mv(state: State, lakehouse: str, src: str, dst: str, workspace: str | None, overwrite: bool) -> None:
    state.out.emit(state.client.lakehouses.move_file(resolve_lakehouse(state, lakehouse, workspace), src, dst, overwrite=overwrite), lambda d: state.out.success(f"moved to {dst}"))


@lakehouse_group.group("tables", help="Delta / Iceberg tables.")
def lh_tables() -> None:
    pass


@lh_tables.command("list")
@click.argument("lakehouse")
@workspace_option
@pass_state
def tables_list(state: State, lakehouse: str, workspace: str | None) -> None:
    rows = state.client.lakehouses.list_tables(resolve_lakehouse(state, lakehouse, workspace))
    rows = rows if isinstance(rows, list) else rows.get("items", [])
    state.out.table(rows, [("NAME", "name", 36), ("FORMAT", "format"), ("PATH", "path", 60)], empty="no tables")


@lh_tables.command("show", help="Schema, format and statistics of a table.")
@click.argument("lakehouse")
@click.argument("table")
@workspace_option
@pass_state
def tables_show(state: State, lakehouse: str, table: str, workspace: str | None) -> None:
    state.out.emit(state.client.lakehouses.table(resolve_lakehouse(state, lakehouse, workspace), table), None)


@lh_tables.command("preview", help="First rows of a table (optionally an older version).")
@click.argument("lakehouse")
@click.argument("table")
@workspace_option
@click.option("--rows", type=int, default=20)
@click.option("--version", type=int, default=None, help="Delta time travel by version.")
@click.option("--as-of", default=None, help="Delta time travel by timestamp.")
@pass_state
def tables_preview(state: State, lakehouse: str, table: str, workspace: str | None, rows: int, version: int | None, as_of: str | None) -> None:
    res = state.client.lakehouses.table_preview(resolve_lakehouse(state, lakehouse, workspace), table, rows=rows, version=version, as_of=as_of)
    render_result(state, res)


@lh_tables.command("history")
@click.argument("lakehouse")
@click.argument("table")
@workspace_option
@click.option("--limit", type=int, default=20)
@pass_state
def tables_history(state: State, lakehouse: str, table: str, workspace: str | None, limit: int) -> None:
    rows = state.client.lakehouses.table_history(resolve_lakehouse(state, lakehouse, workspace), table, limit=limit)
    rows = rows if isinstance(rows, list) else rows.get("items", [])
    state.out.table(rows, [("VERSION", "version"), ("TIMESTAMP", "timestamp"), ("OPERATION", "operation"), ("PARAMS", "operationParameters", 60)])


@lh_tables.command("create", help="Create a table from files or a query: SOURCE is a lakehouse path (Files/data.csv) or a SQL query.")
@click.argument("lakehouse")
@click.argument("name")
@click.option("--from", "source", required=True)
@click.option("--format", "fmt", type=click.Choice(["csv", "parquet", "json", "sql"]), default=None)
@click.option("--mode", type=click.Choice(["overwrite", "append", "error"]), default="overwrite")
@workspace_option
@pass_state
def tables_create(state: State, lakehouse: str, name: str, source: str, fmt: str | None, mode: str, workspace: str | None) -> None:
    res = state.client.lakehouses.create_table(resolve_lakehouse(state, lakehouse, workspace), name, source, format=fmt, mode=mode)
    state.out.emit(res, lambda d: state.out.success(f"table {name} written ({d.get('rows', '?')} rows)"))


@lh_tables.command("drop")
@click.argument("lakehouse")
@click.argument("table")
@workspace_option
@yes_option
@pass_state
def tables_drop(state: State, lakehouse: str, table: str, workspace: str | None, yes: bool) -> None:
    confirm(state, f"Drop table '{table}'?", yes)
    state.client.lakehouses.drop_table(resolve_lakehouse(state, lakehouse, workspace), table)
    state.out.emit({"dropped": table}, lambda d: state.out.success(f"dropped {d['dropped']}"))


@lh_tables.command("optimize")
@click.argument("lakehouse")
@click.argument("table")
@workspace_option
@click.option("--z-order", "z_order", multiple=True)
@pass_state
def tables_optimize(state: State, lakehouse: str, table: str, workspace: str | None, z_order: tuple[str, ...]) -> None:
    state.out.emit(state.client.lakehouses.optimize_table(resolve_lakehouse(state, lakehouse, workspace), table, z_order_by=list(z_order) or None), None)


@lh_tables.command("vacuum")
@click.argument("lakehouse")
@click.argument("table")
@workspace_option
@click.option("--retention-hours", type=int, default=None)
@click.option("--apply/--dry-run", default=False, help="Actually delete files (default: only report).")
@pass_state
def tables_vacuum(state: State, lakehouse: str, table: str, workspace: str | None, retention_hours: int | None, apply: bool) -> None:
    state.out.emit(state.client.lakehouses.vacuum_table(resolve_lakehouse(state, lakehouse, workspace), table, retention_hours=retention_hours, dry_run=not apply), None)


# --------------------------------------------------------------------------------------------- sql
def render_result(state: State, res: dict[str, Any]) -> None:
    """A tabular result (``columns`` + ``rows``) as table / csv / json.  Cell values are *data*: only credentials inside strings are masked."""
    cols = [c["name"] if isinstance(c, dict) else str(c) for c in res.get("columns", [])]
    rows = res.get("rows", [])
    fmt = state.out.fmt
    if fmt in ("json", "yaml"):
        state.out.emit({k: v for k, v in res.items()}, None)
        return
    if fmt == "csv":
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(cols)
        for r in rows:
            w.writerow(["" if v is None else v for v in (r if isinstance(r, (list, tuple)) else [r.get(c) for c in cols])])
        state.out.out.write(redact_text(buf.getvalue()))
        return
    keyed = [{f"c{i}": v for i, v in enumerate(r if isinstance(r, (list, tuple)) else [r.get(c) for c in cols])} for r in rows]
    state.out.table(keyed, [(c, f"c{i}", 40) for i, c in enumerate(cols)], empty="(no rows)", raw_headers=True)
    meta = [f"{res.get('row_count', len(rows))} row(s)"]
    if res.get("truncated"):
        meta.append("truncated (raise --max-rows / --rows)")
    ms = (res.get("stats") or {}).get("elapsed_ms", res.get("elapsed_ms"))
    if ms is not None:
        meta.append(f"{ms} ms")
    state.out.info(state.out.style(", ".join(meta), dim=True))


@click.group("sql", help="Run SQL against lakehouses (DuckDB by default, Spark when a runtime is installed).")
def sql_group() -> None:
    pass


def _sql_text(query: str | None, file: str | None) -> str:
    if file:
        return read_input(file)
    if query in (None, "-"):
        if sys.stdin.isatty():
            raise click.UsageError("give a query, or --file FILE, or pipe SQL on stdin")
        return sys.stdin.read()
    return query


@sql_group.command("run", help="Execute a statement: olcf sql run 'select 1'   |   olcf sql run -f report.sql -o csv")
@click.argument("query", required=False)
@click.option("-f", "--file", type=click.Path(dir_okay=False, allow_dash=True), default=None)
@workspace_option
@click.option("--lakehouse", default=None, help="Default lakehouse (name or id) for unqualified tables.")
@click.option("--engine", default="duckdb", show_default=True)
@click.option("--max-rows", type=int, default=1000, show_default=True)
@click.option("-p", "--param", "params", multiple=True, help="Named parameter k=v.")
@click.option("--timeout-seconds", type=int, default=None)
@click.option("--save", "save_to", type=click.Path(dir_okay=False), default=None, help="Export the full result to a file instead (format from --format).")
@click.option("--format", "export_format", type=click.Choice(["csv", "json", "parquet", "xlsx"]), default="csv")
@pass_state
def sql_run(state: State, query: str | None, file: str | None, workspace: str | None, lakehouse: str | None, engine: str, max_rows: int, params: tuple[str, ...],
            timeout_seconds: int | None, save_to: str | None, export_format: str) -> None:
    text = _sql_text(query, file)
    c = state.client
    wid = None
    try:
        wid = ws_id(state, workspace)
    except click.UsageError:
        wid = None
    lh = resolve_lakehouse(state, lakehouse, workspace) if lakehouse else None
    if save_to:
        data = c.sql.export(text, format=export_format, engine=engine, workspace_id=wid, lakehouse=lh, parameters=parse_kv(params), max_rows=None)
        write_or_print(state, data, save_to)
        return
    res = c.sql.execute(text, engine=engine, workspace_id=wid, lakehouse=lh, parameters=parse_kv(params), max_rows=max_rows, timeout_seconds=timeout_seconds)
    render_result(state, res)


@sql_group.command("explain")
@click.argument("query", required=False)
@click.option("-f", "--file", type=click.Path(dir_okay=False, allow_dash=True), default=None)
@click.option("--engine", default="duckdb")
@workspace_option
@pass_state
def sql_explain(state: State, query: str | None, file: str | None, engine: str, workspace: str | None) -> None:
    res = state.client.sql.explain(_sql_text(query, file), engine=engine, workspace_id=ws_id(state, workspace))
    state.out.emit(res, lambda d: print(d.get("plan") or json.dumps(d, indent=2, default=str), file=state.out.out))


@sql_group.command("history")
@click.option("--limit", type=int, default=20)
@click.option("--mine", is_flag=True)
@pass_state
def sql_history(state: State, limit: int, mine: bool) -> None:
    rows = state.client.sql.history(mine=mine or None, limit=limit)
    state.out.table(list(rows), [("ID", "id"), ("STATUS", "status"), ("ENGINE", "engine"), ("ROWS", "row_count"), ("MS", "elapsed_ms"), ("SQL", "sql_text", 60)])


@sql_group.command("engines", help="SQL engines available on this server.")
@pass_state
def sql_engines(state: State) -> None:
    rows = state.client.sql.engines()
    state.out.table(rows if isinstance(rows, list) else rows.get("items", []), [("ENGINE", "id"), ("AVAILABLE", "available"), ("DETAIL", "detail", 70)])


# --------------------------------------------------------------------------------------------- connection / secret
@click.group("connection", help="Connections to storage accounts, databases and Git hosts.")
def connection_group() -> None:
    pass


@connection_group.command("list")
@workspace_option
@click.option("--all-workspaces", is_flag=True)
@click.option("--type", "type_", default=None)
@pass_state
def conn_list(state: State, workspace: str | None, all_workspaces: bool, type_: str | None) -> None:
    c = state.client
    wid = None if all_workspaces else (ws_id(state, workspace) if (workspace or state.resolve_connection().get("workspace")) else None)
    rows = list(c.paginate(c.connections.list, workspace_id=wid, type=type_))
    state.out.table(rows, [("ID", "id"), ("NAME", "name", 28), ("TYPE", "type"), ("SHARED", "shared"), ("UPDATED", "updated_at")], empty="no connections")


@connection_group.command("get")
@click.argument("connection")
@pass_state
def conn_get(state: State, connection: str) -> None:
    state.out.emit(state.client.connections.get(_conn_id(state, connection)), lambda d: state.out.kv(list(d.items())))


def _conn_id(state: State, ref: str) -> str:
    if ref.startswith("con_"):
        return ref
    c = state.client
    for x in c.paginate(c.connections.list):
        if x.get("name") == ref:
            return x["id"]
    raise click.ClickException(f"no connection named '{ref}'")


@connection_group.command("create", help="Create a connection.  Credentials come from stdin / OLCF_CONNECTION_SECRET and are stored as an encrypted secret.")
@click.argument("name")
@click.option("--type", "type_", required=True, help="s3, adls, azure_blob, gcs, postgres, mysql, sqlserver, git, http, onelake, ...")
@workspace_option
@click.option("--config", "config", multiple=True, help="Non-secret setting k=v (endpoint, bucket, host, database ...); repeatable.")
@click.option("--config-file", type=click.Path(dir_okay=False, allow_dash=True), default=None, help="JSON object with settings.")
@click.option("--secret-stdin", is_flag=True, help="Read the credential (text, or a JSON object) from stdin.")
@click.option("--shared", is_flag=True, help="Usable by every workspace.")
@pass_state
def conn_create(state: State, name: str, type_: str, workspace: str | None, config: tuple[str, ...], config_file: str | None, secret_stdin: bool, shared: bool) -> None:
    c = state.client
    cfg = dict(load_json_arg(config_file)) if config_file else {}
    cfg.update(parse_kv(config))
    wid = ws_id(state, workspace) if (workspace or state.resolve_connection().get("workspace")) and not shared else None
    secret_id = None
    if secret_stdin:
        raw = sys.stdin.read().strip()
    elif os.environ.get("OLCF_CONNECTION_SECRET"):
        raw = os.environ["OLCF_CONNECTION_SECRET"]
    elif sys.stdin.isatty():
        raw = click.prompt("Credential (leave empty for none)", hide_input=True, default="", show_default=False)
    else:
        raw = ""
    if raw:
        try:
            val: Any = json.loads(raw)
            if not isinstance(val, dict):
                val = raw
        except ValueError:
            val = raw
        sec = c.connections.create_secret(f"connection/{name}", val, scope_type="workspace" if wid else "org", scope_id=wid or "")
        secret_id = sec["id"]
    conn = c.connections.create(name, type_, workspace_id=wid, config=cfg, secret_id=secret_id, shared=shared)
    state.out.emit(conn, lambda d: state.out.success(f"created connection {d['name']} ({d['id']})"))


@connection_group.command("test", help="Check the connection end to end (DNS, TLS, login, permission).")
@click.argument("connection")
@pass_state
def conn_test(state: State, connection: str) -> None:
    res = state.client.connections.test(_conn_id(state, connection))

    def human(d: dict[str, Any]) -> None:
        steps = d.get("steps") or []
        if steps:
            state.out.table(steps, [("STEP", "name"), ("OK", "ok"), ("MS", "ms"), ("DETAIL", "detail", 60), ("FIX", "remediation", 60)])
        (state.out.success if d.get("ok") else state.out.error)("connection works" if d.get("ok") else "connection test failed")

    state.out.emit(res, human)
    if not res.get("ok"):
        raise SystemExit(1)


@connection_group.command("delete")
@click.argument("connection")
@yes_option
@pass_state
def conn_delete(state: State, connection: str, yes: bool) -> None:
    confirm(state, f"Delete connection '{connection}'?", yes)
    state.client.connections.delete(_conn_id(state, connection))
    state.out.emit({"deleted": connection}, lambda d: state.out.success(f"deleted {d['deleted']}"))


@click.group("secret", help="Encrypted secrets (values are write-only).")
def secret_group() -> None:
    pass


@secret_group.command("list")
@click.option("--scope", "scope_type", type=click.Choice(["org", "workspace", "connection"]), default=None)
@click.option("--scope-id", default=None)
@pass_state
def secret_list(state: State, scope_type: str | None, scope_id: str | None) -> None:
    rows = state.client.connections.list_secrets(scope_type=scope_type, scope_id=scope_id)
    state.out.table(list(rows), [("ID", "id"), ("NAME", "name", 32), ("SCOPE", "scope_type"), ("PROVIDER", "provider"), ("VERSION", "version"), ("UPDATED", "updated_at")], empty="no secrets")


@secret_group.command("set", help="Create a secret.  The value is read from a hidden prompt, stdin or $OLCF_SECRET_VALUE - never from arguments.")
@click.argument("name")
@click.option("--scope", "scope_type", type=click.Choice(["org", "workspace", "connection"]), default="org")
@click.option("--scope-id", default="", help="Workspace/connection id for non-org scopes.")
@workspace_option
@click.option("--value-stdin", is_flag=True)
@pass_state
def secret_set(state: State, name: str, scope_type: str, scope_id: str, workspace: str | None, value_stdin: bool) -> None:
    if scope_type == "workspace" and not scope_id:
        scope_id = ws_id(state, workspace)
    value = secret_prompt(f"Value for {name}", from_env="OLCF_SECRET_VALUE", stdin=value_stdin)
    res = state.client.connections.create_secret(name, value, scope_type=scope_type, scope_id=scope_id)
    state.out.emit(res, lambda d: state.out.success(f"stored secret {d.get('name', name)} ({d.get('id')})"))


@secret_group.command("rotate")
@click.argument("secret_id")
@click.option("--value-stdin", is_flag=True)
@pass_state
def secret_rotate(state: State, secret_id: str, value_stdin: bool) -> None:
    value = secret_prompt("New value", from_env="OLCF_SECRET_VALUE", stdin=value_stdin)
    state.out.emit(state.client.connections.rotate_secret(secret_id, value), lambda d: state.out.success(f"rotated {secret_id}"))


@secret_group.command("delete")
@click.argument("secret_id")
@yes_option
@pass_state
def secret_delete(state: State, secret_id: str, yes: bool) -> None:
    confirm(state, f"Delete secret {secret_id}? Jobs that reference it will fail.", yes)
    state.client.connections.delete_secret(secret_id)
    state.out.emit({"deleted": secret_id}, lambda d: state.out.success(f"deleted {d['deleted']}"))


# --------------------------------------------------------------------------------------------- catalog / search
@click.group("catalog", help="Table catalog, lineage and search.")
def catalog_group() -> None:
    pass


@catalog_group.command("tables")
@workspace_option
@click.option("-q", "--query", "q", default=None)
@pass_state
def cat_tables(state: State, workspace: str | None, q: str | None) -> None:
    c = state.client
    wid = ws_id(state, workspace) if (workspace or state.resolve_connection().get("workspace")) else None
    rows = list(c.paginate(c.catalog.tables, workspace_id=wid, q=q))
    state.out.table(rows, [("ID", "id"), ("TABLE", "name", 30), ("FORMAT", "format"), ("VERSION", "version"), ("STATUS", "status"), ("TAGS", "tags", 40), ("MODIFIED", "last_modified")], empty="no tables")


@catalog_group.command("lineage", help="Upstream/downstream of a dataset (lakehouse.table).")
@click.argument("dataset")
@click.option("--direction", type=click.Choice(["up", "down", "both"]), default=None)
@click.option("--depth", type=int, default=None)
@pass_state
def cat_lineage(state: State, dataset: str, direction: str | None, depth: int | None) -> None:
    state.out.emit(state.client.catalog.lineage(dataset, direction=direction, depth=depth), None)


@click.command("search", help="Search workspaces, items, tables and jobs.")
@click.argument("query")
@click.option("--limit", type=int, default=20)
@pass_state
def search_cmd(state: State, query: str, limit: int) -> None:
    res = state.client.admin.search(query, limit=limit)
    rows = list(res) if isinstance(res, list) else (res.get("items") or res.get("results") or [])
    state.out.emit(rows if isinstance(res, list) else res, lambda _d: state.out.table(rows, [("TYPE", "type"), ("NAME", "name", 40), ("ID", "id"), ("WORKSPACE", "workspace_id"), ("FOLDER", "folder")], empty="nothing found"))


def register(group: click.Group) -> None:
    for cmd in (workspace_group, item_group, notebook_group, lakehouse_group, sql_group, connection_group, secret_group, catalog_group, search_cmd):
        group.add_command(cmd)
