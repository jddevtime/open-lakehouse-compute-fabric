# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Job and pipeline commands: ``job submit|list|get|status|logs|cancel|retry|wait|output|artifacts``, ``pipeline ...``.

``--wait`` / ``-f`` turn a submission into a blocking, script-friendly call: the exit status is 0 only when the job succeeded
(10 when it ended otherwise, 9 when ``--timeout`` ran out first - the job keeps running).
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import click

from olcf.cli.common import (
    load_json_arg,
    parse_kv,
    pass_state,
    read_input,
    resolve_item,
    workspace_option,
    write_or_print,
)
from olcf.cli.state import State
from olcf.core.redaction import redact_text
from olcf.sdk import JobFailedError, OlcfTimeoutError
from olcf.sdk.client import TERMINAL_JOB_STATES

JOB_COLUMNS = [("ID", "id"), ("STATE", "state"), ("KIND", "kind"), ("NAME", "name", 30), ("QUEUE", "queue"), ("WORKER", "driver_worker_id"), ("CREATED", "created_at")]
RUN_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "timed_out", "skipped"})


def ws_id(state: State, workspace: str | None) -> str:
    """Workspace id for an explicit/default workspace name or id."""
    ref = state.default_workspace(workspace)
    return ref if ref.startswith("wks_") else state.client.workspaces.get(ref)["id"]


def render_job(state: State, job: dict[str, Any]) -> None:
    o = state.out
    pairs: list[tuple[str, Any]] = [("job", job.get("id")), ("state", o.state(str(job.get("state")))), ("kind", job.get("kind")), ("name", job.get("name")),
                                    ("queue", job.get("queue")), ("runtime", job.get("runtime_id")), ("worker", job.get("driver_worker_id")),
                                    ("created", job.get("created_at")), ("finished", job.get("finished_at"))]
    o.kv([(k, v) for k, v in pairs if v not in (None, "")])
    qr = job.get("queue_reason")
    if qr:
        o.info(o.style("waiting:", fg="yellow", bold=True) + f" [{qr.get('code')}] {qr.get('message')}")
        if qr.get("remediation"):
            o.info(f"  fix: {qr['remediation']}")
    fail = job.get("failure")
    if fail:
        o.info(o.style("failure:", fg="red", bold=True) + f" [{fail.get('category')}] {fail.get('message')}")
        if fail.get("remediation"):
            o.info(f"  fix: {fail['remediation']}")
        if fail.get("line") or fail.get("cell") is not None:
            o.info(f"  at cell {fail.get('cell')} line {fail.get('line')}")


def await_job(state: State, job: dict[str, Any], *, wait: bool, follow: bool, timeout: float | None) -> dict[str, Any]:
    """Wait for ``job`` (printing log lines when ``follow``).  Raises the SDK errors that map to exit codes 9 and 10."""
    c = state.client
    jid = job["id"]
    if not (wait or follow):
        return job
    deadline = None if timeout is None else time.monotonic() + timeout
    if follow:
        state.out.step(f"following {jid}")
        for chunk in c.jobs.logs(jid, follow=True, timeout=timeout):
            sys.stdout.write(redact_text(chunk))
            sys.stdout.flush()
    left = None if deadline is None else max(0.1, deadline - time.monotonic())
    seen: list[str] = []

    def upd(j: dict[str, Any]) -> None:
        if j.get("state") not in seen:
            seen.append(j["state"])
            state.out.detail(f"{jid}: {j['state']}")
        qr = j.get("queue_reason")
        if qr and qr.get("code") not in seen:  # a queued job says why it waits, once per reason, instead of silently sitting there
            seen.append(qr["code"])
            state.out.detail(f"{jid}: waiting [{qr['code']}] {qr.get('message')}")
            if qr.get("remediation"):
                state.out.detail(f"fix: {qr['remediation']}")

    final = c.jobs.wait(jid, timeout=left, on_update=None if follow else upd)
    if final.get("state") != "succeeded":
        raise JobFailedError(final)
    return final


# --------------------------------------------------------------------------------------------- job
@click.group("job", help="Submit and follow jobs (notebooks, Spark jobs, SQL, scripts).")
def job_group() -> None:
    pass


@job_group.command("list", help="Recent jobs.")
@workspace_option
@click.option("--all-workspaces", is_flag=True, help="Do not limit to the default workspace.")
@click.option("--state", "st", default=None, help="queued, running, succeeded, failed, cancelled, timed_out ...")
@click.option("--kind", default=None)
@click.option("--limit", type=int, default=25, show_default=True)
@pass_state
def job_list(state: State, workspace: str | None, all_workspaces: bool, st: str | None, kind: str | None, limit: int) -> None:
    c = state.client
    wid = None
    if not all_workspaces:
        try:
            wid = ws_id(state, workspace)
        except click.UsageError:
            wid = None
    rows: list[dict[str, Any]] = []
    for j in c.jobs.iter(workspace_id=wid, state=st, kind=kind, limit=min(limit, 100)):
        rows.append(j)
        if len(rows) >= limit:
            break
    state.out.table(rows, JOB_COLUMNS, empty="no jobs")


@job_group.command("get", help="Full record of a job.")
@click.argument("job_id")
@pass_state
def job_get(state: State, job_id: str) -> None:
    state.out.emit(state.client.jobs.get(job_id), lambda j: render_job(state, j))


@job_group.command("status", help="State of a job (exit 0 succeeded, 10 failed/cancelled, 1 still running).")
@click.argument("job_id")
@pass_state
def job_status(state: State, job_id: str) -> None:
    j = state.client.jobs.get(job_id)
    state.out.emit({"id": j["id"], "state": j["state"], "failure": j.get("failure"), "queue_reason": j.get("queue_reason"), "runtime_id": j.get("runtime_id")},
                   lambda _d: render_job(state, j))
    if j["state"] not in TERMINAL_JOB_STATES:
        raise SystemExit(1)
    if j["state"] != "succeeded":
        raise SystemExit(10)


@job_group.command("submit", help="Submit a job.  Use --wait/-f to block until it ends.")
@workspace_option
@click.option("--kind", type=click.Choice(["notebook", "spark_job", "sql", "script"]), default=None, help="Default: notebook with --item, sql with --sql, else from the file type.")
@click.option("--item", default=None, help="Notebook or Spark job definition (name or id).")
@click.option("--file", "src_file", type=click.Path(dir_okay=False, allow_dash=True), default=None, help="Source file (.ipynb, .py, .sql, .scala); '-' reads stdin.")
@click.option("--sql", "sql_text", default=None, help="SQL text to run as a job.")
@click.option("--name", default=None)
@click.option("-p", "--param", "params", multiple=True, help="Parameter k=v (JSON values allowed); repeatable.")
@click.option("--env", "env", multiple=True, help="Environment variable k=v; repeatable.")
@click.option("--secret", "secret_refs", multiple=True, help="ENV_NAME=secret-id (resolved on the worker, never printed); repeatable.")
@click.option("--engine", type=click.Choice(["spark", "python"]), default=None)
@click.option("--family", default=None, help="Runtime family, e.g. fabric-2.0-compatible or olcf-native.")
@click.option("--runtime", "runtime_id", default=None, help="Exact runtime id, e.g. olcf-rt-2.0.0.")
@click.option("--queue", default=None)
@click.option("--priority", type=click.IntRange(0, 100), default=None)
@click.option("--job-timeout", "timeout_seconds", type=int, default=None, help="Kill the job after this many seconds.")
@click.option("--driver-memory-mb", type=int, default=None)
@click.option("--executor-memory-mb", type=int, default=None)
@click.option("--executor-cores", type=int, default=None)
@click.option("--max-executors", type=int, default=None)
@click.option("--pool", default=None, help="Worker pool to run on.")
@click.option("--body", "body_file", type=click.Path(dir_okay=False, allow_dash=True), default=None, help="Complete JobSubmission JSON (other options are merged over it).")
@click.option("--idempotency-key", default=None, help="Safe retries: the same key never creates a second job.")
@click.option("--wait", "wait", is_flag=True, help="Block until the job ends.")
@click.option("-f", "--follow", is_flag=True, help="Stream the log until the job ends.")
@click.option("--timeout", "wait_timeout", type=float, default=None, help="Give up waiting after this many seconds (exit 9; the job continues).")
@pass_state
def job_submit(state: State, workspace: str | None, kind: str | None, item: str | None, src_file: str | None, sql_text: str | None, name: str | None,
               params: tuple[str, ...], env: tuple[str, ...], secret_refs: tuple[str, ...], engine: str | None, family: str | None, runtime_id: str | None,
               queue: str | None, priority: int | None, timeout_seconds: int | None, driver_memory_mb: int | None, executor_memory_mb: int | None,
               executor_cores: int | None, max_executors: int | None, pool: str | None, body_file: str | None, idempotency_key: str | None, wait: bool,
               follow: bool, wait_timeout: float | None) -> None:
    c = state.client
    body: dict[str, Any] = load_json_arg(body_file) if body_file else {}
    body.setdefault("workspace_id", ws_id(state, workspace))
    if item:
        it = resolve_item(state, workspace, item)
        body["item_id"] = it["id"]
        kind = kind or ("spark_job" if it.get("type") == "spark_job_definition" else "notebook")
    if sql_text is not None:
        body["source"] = {"format": "sql", "content": sql_text}
        kind = kind or "sql"
    if src_file:
        text = read_input(src_file)
        ext = Path(src_file).suffix.lower() if src_file != "-" else ".ipynb"
        fmt = {".ipynb": "ipynb", ".py": "py", ".sql": "sql", ".scala": "scala", ".sh": "script"}.get(ext)
        if not fmt:
            raise click.BadParameter(f"cannot tell the source type of '{src_file}' (use .ipynb, .py, .sql, .scala or .sh)")
        body["source"] = {"format": fmt, "content": text, "filename": None if src_file == "-" else Path(src_file).name}
        kind = kind or {"ipynb": "notebook", "py": "script", "sql": "sql", "scala": "spark_job", "script": "script"}[fmt]
    if not body.get("kind") and not kind:
        raise click.UsageError("say what to run: --item, --file, --sql or --body")
    if kind:
        body["kind"] = kind
    if name:
        body["name"] = name
    if params:
        body.setdefault("parameters", {}).update(parse_kv(params))
    if env:
        body.setdefault("env", {}).update({k: str(v) for k, v in parse_kv(env).items()})
    if secret_refs:
        body.setdefault("secret_refs", {}).update({k: str(v) for k, v in parse_kv(secret_refs).items()})
    if engine:
        body["engine"] = engine
    rt = {k: v for k, v in (("family", family), ("runtime_id", runtime_id)) if v}
    if rt:
        body.setdefault("runtime", {}).update(rt)
    pol: dict[str, Any] = {k: v for k, v in (("queue", queue), ("priority", priority), ("timeout_seconds", timeout_seconds)) if v is not None}
    if pool:
        pol["placement"] = {"pool": pool}
    if pol:
        body.setdefault("policy", {}).update(pol)
    res = {k: v for k, v in (("driver_memory_mb", driver_memory_mb), ("executor_memory_mb", executor_memory_mb), ("executor_cores", executor_cores),
                             ("max_executors", max_executors)) if v is not None}
    if res:
        body.setdefault("resources", {}).update(res)
    job = c.jobs.submit(body, idempotency_key=idempotency_key)
    if not (wait or follow):

        def _submitted(j: dict[str, Any]) -> None:
            state.out.success(f"submitted {j['id']}")
            render_job(state, j)

        state.out.emit(job, _submitted)
        return
    state.out.success(f"submitted {job['id']}")
    final = await_job(state, job, wait=True, follow=follow, timeout=wait_timeout)
    state.out.emit(final, lambda j: render_job(state, j))


@job_group.command("logs", help="Print the log of a job; -f follows it until the job ends.")
@click.argument("job_id")
@click.option("-f", "--follow", is_flag=True)
@click.option("--stream", type=click.Choice(["combined", "stdout", "stderr", "driver", "events"]), default="combined")
@click.option("--tail", "tail_bytes", type=int, default=None, help="Only the last N bytes.")
@click.option("--timeout", type=float, default=None)
@pass_state
def job_logs(state: State, job_id: str, follow: bool, stream: str, tail_bytes: int | None, timeout: float | None) -> None:
    c = state.client
    offset = 0
    if tail_bytes:
        page = c.jobs.logs_page(job_id, stream=stream, tail=True, limit=tail_bytes)
        sys.stdout.write(redact_text(page.get("data", "")))
        offset = int(page.get("next_offset", 0))
        if not follow:
            return
    for chunk in c.jobs.logs(job_id, follow=follow, stream=stream, offset=offset, timeout=timeout):
        sys.stdout.write(redact_text(chunk))
        sys.stdout.flush()


@job_group.command("wait", help="Block until a job ends (exit 0 only if it succeeded).")
@click.argument("job_id")
@click.option("--timeout", type=float, default=None)
@pass_state
def job_wait(state: State, job_id: str, timeout: float | None) -> None:
    job = await_job(state, state.client.jobs.get(job_id), wait=True, follow=False, timeout=timeout)
    state.out.emit(job, lambda j: render_job(state, j))


@job_group.command("cancel", help="Request cancellation.")
@click.argument("job_id")
@pass_state
def job_cancel(state: State, job_id: str) -> None:
    j = state.client.jobs.cancel(job_id)
    state.out.emit(j, lambda d: state.out.success(f"{job_id}: {d.get('state')}"))


@job_group.command("retry", help="Run a finished job again with the same inputs.")
@click.argument("job_id")
@pass_state
def job_retry(state: State, job_id: str) -> None:
    j = state.client.jobs.retry(job_id)
    state.out.emit(j, lambda d: state.out.success(f"new attempt: {d.get('id')} ({d.get('state')})"))


@job_group.command("output", help="The result of a job (the executed notebook by default).")
@click.argument("job_id")
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None, help="Write to a file instead of stdout.")
@click.option("--name", default=None)
@pass_state
def job_output(state: State, job_id: str, out_file: str | None, name: str | None) -> None:
    write_or_print(state, state.client.jobs.output(job_id, name=name), out_file)


@job_group.command("artifacts", help="List artifacts, or download one with NAME.")
@click.argument("job_id")
@click.argument("name", required=False)
@click.option("-o", "--out", "out_file", type=click.Path(dir_okay=False), default=None)
@pass_state
def job_artifacts(state: State, job_id: str, name: str | None, out_file: str | None) -> None:
    c = state.client
    if name:
        write_or_print(state, c.jobs.artifact(job_id, name), out_file or name)
        return
    rows = c.jobs.artifacts(job_id)
    state.out.table(rows if isinstance(rows, list) else rows.get("items", []), [("NAME", "name"), ("SIZE", "size"), ("TYPE", "content_type"), ("CREATED", "created_at")])


@job_group.command("timeline", help="State transitions and events of a job.")
@click.argument("job_id")
@pass_state
def job_timeline(state: State, job_id: str) -> None:
    rows = state.client.jobs.timeline(job_id)
    rows = rows if isinstance(rows, list) else rows.get("items", [])
    shown = [{"at": r.get("ts"), "event": r.get("event") or r.get("kind"), "state": " -> ".join(x for x in (r.get("from"), r.get("to")) if x) or "",
              "detail": r.get("reason") or ""} for r in rows]
    state.out.table(shown, [("AT", "at"), ("EVENT", "event"), ("STATE", "state"), ("DETAIL", "detail", 90)])


# --------------------------------------------------------------------------------------------- pipeline
@click.group("pipeline", help="Pipelines: definitions, runs, schedules.")
def pipeline_group() -> None:
    pass


@pipeline_group.command("list")
@workspace_option
@pass_state
def pipeline_list(state: State, workspace: str | None) -> None:
    c = state.client
    rows = list(c.paginate(c.items.list, state.default_workspace(workspace), type="pipeline"))
    state.out.table(rows, [("ID", "id"), ("NAME", "name", 32), ("VERSION", "version"), ("UPDATED", "updated_at")], empty="no pipelines")


@pipeline_group.command("get", help="The pipeline definition (JSON).")
@click.argument("pipeline")
@workspace_option
@pass_state
def pipeline_get(state: State, pipeline: str, workspace: str | None) -> None:
    it = resolve_item(state, workspace, pipeline, "pipeline")
    state.out.emit(state.client.pipelines.definition(it["id"]), None)


@pipeline_group.command("create", help="Create a pipeline from a JSON definition file ('-' = stdin).")
@click.argument("name")
@click.option("--file", "def_file", type=click.Path(dir_okay=False, allow_dash=True), required=True)
@workspace_option
@pass_state
def pipeline_create(state: State, name: str, def_file: str, workspace: str | None) -> None:
    definition = load_json_arg(def_file)
    it = state.client.items.create(state.default_workspace(workspace), "pipeline", name, definition=definition)
    state.out.emit(it, lambda d: state.out.success(f"created pipeline {d['name']} ({d['id']})"))


@pipeline_group.command("validate", help="Validate a definition file without saving anything.")
@click.argument("file", type=click.Path(dir_okay=False, allow_dash=True))
@pass_state
def pipeline_validate(state: State, file: str) -> None:
    res = state.client.pipelines.validate(load_json_arg(file))
    ok = res.get("ok", res.get("valid", not res.get("errors")))

    def human(r: dict[str, Any]) -> None:
        if ok:
            state.out.success("the pipeline is valid")
        for key in ("errors", "warnings"):
            for e in r.get(key) or []:
                state.out.info(f"  {key[:-1]}: {e.get('message', e) if isinstance(e, dict) else e}")

    state.out.emit(res, human)
    if not ok:
        raise SystemExit(7)


@pipeline_group.command("import-fabric", help="Convert a Microsoft Fabric / Data Factory pipeline JSON export.")
@click.argument("file", type=click.Path(dir_okay=False, allow_dash=True))
@click.option("--strict", is_flag=True, help="Fail on activities that have no OLCF equivalent.")
@pass_state
def pipeline_import_fabric(state: State, file: str, strict: bool) -> None:
    state.out.emit(state.client.pipelines.import_fabric(load_json_arg(file), strict=strict), None)


def await_run(state: State, run: dict[str, Any], timeout: float | None) -> dict[str, Any]:
    c = state.client
    deadline = None if timeout is None else time.monotonic() + timeout
    seen = ""
    interval = 1.0
    while True:
        r = c.pipelines.get_run(run["id"])
        if r.get("state") != seen:
            seen = r.get("state", "")
            state.out.detail(f"{run['id']}: {seen}")
        if seen in RUN_TERMINAL:
            return r
        if deadline is not None and time.monotonic() >= deadline:
            raise OlcfTimeoutError(f"run {run['id']} still {seen} after {timeout:g}s", last=r)
        time.sleep(interval)
        interval = min(5.0, interval * 1.3)


@pipeline_group.command("run", help="Start a run.  --wait blocks (exit 10 when the run does not succeed).")
@click.argument("pipeline")
@workspace_option
@click.option("-p", "--param", "params", multiple=True)
@click.option("--wait", is_flag=True)
@click.option("--timeout", type=float, default=None)
@click.option("--idempotency-key", default=None)
@pass_state
def pipeline_run(state: State, pipeline: str, workspace: str | None, params: tuple[str, ...], wait: bool, timeout: float | None, idempotency_key: str | None) -> None:
    c = state.client
    it = resolve_item(state, workspace, pipeline, "pipeline")
    run = c.pipelines.run(it["id"], parse_kv(params), idempotency_key=idempotency_key)
    state.out.success(f"started run {run['id']}")
    if wait:
        run = await_run(state, run, timeout)
    state.out.emit(run, lambda r: state.out.kv([("run", r.get("id")), ("state", state.out.state(str(r.get("state")))), ("pipeline", r.get("pipeline_id")), ("started", r.get("started_at")),
                                                  ("finished", r.get("finished_at"))]))
    if wait and run.get("state") != "succeeded":
        raise SystemExit(10)


@pipeline_group.command("runs", help="Runs of a pipeline (or of the whole workspace).")
@click.argument("pipeline", required=False)
@workspace_option
@click.option("--state", "st", default=None)
@click.option("--limit", type=int, default=25)
@pass_state
def pipeline_runs(state: State, pipeline: str | None, workspace: str | None, st: str | None, limit: int) -> None:
    c = state.client
    if pipeline:
        pid = resolve_item(state, workspace, pipeline, "pipeline")["id"]
        rows = c.pipelines.runs_of(pid, state=st, limit=limit)
    else:
        rows = c.pipelines.list_runs(workspace_id=ws_id(state, workspace), state=st, limit=limit)
    state.out.table(list(rows), [("RUN", "id"), ("STATE", "state"), ("PIPELINE", "pipeline_id"), ("TRIGGER", "trigger_type"), ("STARTED", "started_at"), ("FINISHED", "finished_at")])


@pipeline_group.command("run-status", help="One run with its activities.")
@click.argument("run_id")
@pass_state
def pipeline_run_status(state: State, run_id: str) -> None:
    c = state.client
    run = c.pipelines.get_run(run_id)
    acts = c.pipelines.run_activities(run_id)
    rows = list(acts) if isinstance(acts, list) else acts.get("items", [])

    def human(_d: Any) -> None:
        state.out.kv([("run", run.get("id")), ("state", state.out.state(str(run.get("state")))), ("started", run.get("started_at")), ("finished", run.get("finished_at"))])
        state.out.table(rows, [("ACTIVITY", "name", 30), ("TYPE", "type"), ("STATE", "state"), ("ATTEMPT", "attempt"), ("DURATION MS", "duration_ms")])

    state.out.emit({"run": run, "activities": rows}, human)


@pipeline_group.command("cancel")
@click.argument("run_id")
@pass_state
def pipeline_cancel(state: State, run_id: str) -> None:
    state.out.emit(state.client.pipelines.cancel_run(run_id), lambda d: state.out.success(f"{run_id}: {d.get('state')}"))


@pipeline_group.command("resume", help="Continue a failed run from the failed activities.")
@click.argument("run_id")
@pass_state
def pipeline_resume(state: State, run_id: str) -> None:
    state.out.emit(state.client.pipelines.resume_run(run_id), lambda d: state.out.success(f"resumed as {d.get('id', run_id)}"))


@pipeline_group.command("rerun", help="Run again with the same parameters.")
@click.argument("run_id")
@pass_state
def pipeline_rerun(state: State, run_id: str) -> None:
    state.out.emit(state.client.pipelines.rerun(run_id), lambda d: state.out.success(f"new run {d.get('id')}"))


@pipeline_group.command("schedules", help="Schedules of the workspace.")
@workspace_option
@pass_state
def pipeline_schedules(state: State, workspace: str | None) -> None:
    rows = state.client.pipelines.list_schedules(ws_id(state, workspace))
    state.out.table(list(rows), [("ID", "id"), ("NAME", "name", 28), ("CRON", "cron"), ("TZ", "timezone"), ("ENABLED", "enabled"), ("NEXT", "next_run_at")])


@pipeline_group.command("schedule", help="enable | disable | pause | resume | run-now | next  for a schedule id.")
@click.argument("action", type=click.Choice(["enable", "disable", "pause", "resume", "run-now", "next"]))
@click.argument("schedule_id")
@pass_state
def pipeline_schedule(state: State, action: str, schedule_id: str) -> None:
    p = state.client.pipelines
    fn = {"enable": p.enable_schedule, "disable": p.disable_schedule, "pause": p.pause_schedule, "resume": p.resume_schedule, "run-now": p.run_schedule_now,
          "next": p.schedule_next}[action]
    state.out.emit(fn(schedule_id), None)


def register(group: click.Group) -> None:
    group.add_command(job_group)
    group.add_command(pipeline_group)
