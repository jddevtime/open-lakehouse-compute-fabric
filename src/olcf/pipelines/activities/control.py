# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Control-flow activities: ``wait``, ``set_variable``, ``fail``, ``if``, ``switch``, ``for_each``, ``until``, ``child_pipeline`` and ``webhook_wait``.

Containers (``if`` / ``switch`` / ``for_each`` / ``until``) only *decide* which nested scopes run; the engine owns the rows, retries, cancellation
and durability of the nested activities. ``child_pipeline`` and ``webhook_wait`` are asynchronous: they persist a handle in the activity state
(child run id / callback token) so nothing is held in memory while they wait and a crashed or failed-over control plane simply continues polling.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from datetime import datetime, timedelta
from typing import Any, ClassVar

from olcf.core.errors import NotFound, OlcfError
from olcf.core.timeutil import now_iso
from olcf.pipelines.activities.base import (
    Activity,
    ActivityCancelled,
    ActivityContext,
    ActivityError,
    Container,
    ValidationIssue,
    X,
    obj,
    s_arr,
    s_bool,
    s_int,
    s_num,
    s_obj,
    s_str,
)
from olcf.pipelines.expressions import equal, evaluate_value, to_bool, to_str

MAX_WAIT_SECONDS = 7 * 86400
MAX_FOREACH_ITEMS = 100_000
MAX_STATE_BYTES = 8 * 1024 * 1024
MAX_CALLBACK_BYTES = 1024 * 1024
MAX_CHILD_DEPTH = 8


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class WaitActivity(Activity):
    """Sleep for ``waitTimeInSeconds`` without holding a thread: the wake-up time is persisted, so it survives a restart."""

    type = "wait"
    title = "Wait"
    description = "Pause the pipeline for a fixed number of seconds (up to 7 days)."
    mode = "async"
    reusable = False
    properties_schema = obj({"waitTimeInSeconds": s_num(0)}, ["waitTimeInSeconds"])

    def start(self, actx: ActivityContext) -> None:
        secs = actx.props.get("waitTimeInSeconds")
        if isinstance(secs, bool) or not isinstance(secs, (int, float)) or secs < 0:
            raise ActivityError(f"waitTimeInSeconds must be a non-negative number but got {secs!r}", category="configuration", retryable=False)
        if secs > MAX_WAIT_SECONDS:
            raise ActivityError(f"waitTimeInSeconds is limited to {MAX_WAIT_SECONDS} (7 days)", category="configuration", retryable=False)
        until = actx.now() + timedelta(seconds=float(secs))
        actx.save_state(until=until.isoformat(), seconds=float(secs))

    def poll(self, actx: ActivityContext) -> dict[str, Any] | None:
        if actx.is_cancelled():
            raise ActivityCancelled()
        until = _parse_iso(actx.state["until"])
        if actx.now() >= until:
            return {"waitedSeconds": actx.state.get("seconds", 0)}
        return None

    def poll_interval(self, actx: ActivityContext) -> float:
        try:
            remaining = (_parse_iso(actx.state["until"]) - actx.now()).total_seconds()
        except (KeyError, ValueError):
            return 1.0
        return max(0.05, min(1.0, remaining))


class FailActivity(Activity):
    """Fail the pipeline on purpose with a custom message and error code (never retried)."""

    type = "fail"
    title = "Fail"
    description = "Fail the pipeline with a custom message and error code."
    mode = "inline"
    reusable = False
    properties_schema = obj({"message": s_str(), "errorCode": s_str()}, ["message"])

    def execute(self, actx: ActivityContext) -> dict[str, Any]:
        raise ActivityError(to_str(actx.props.get("message", "")) or "pipeline failed by a fail activity", category="user",
                            code=str(actx.props.get("errorCode") or "UserFail"), retryable=False)


class SetVariableActivity(Activity):
    """Set (or append to) a pipeline variable. Runs inline; the engine applies the effect atomically with the activity result."""

    type = "set_variable"
    title = "Set variable"
    description = "Assign a pipeline variable (mode 'set') or append to an array variable (mode 'append')."
    mode = "inline"
    reusable = True
    properties_schema = obj({"variable": {"type": "string", "minLength": 1}, "value": {}, "mode": {"enum": ["set", "append"]}}, ["variable", "value"])

    def validate(self, props: dict[str, Any], path: str) -> list[ValidationIssue]:
        return []

    def execute(self, actx: ActivityContext) -> dict[str, Any]:
        from olcf.pipelines.model import coerce_typed

        name = actx.props.get("variable")
        decl = (actx.pipeline_def.get("variables") or {}).get(name)
        if decl is None:
            raise ActivityError(f"variable '{name}' is not declared", category="configuration", retryable=False)
        mode = actx.props.get("mode") or "set"
        value = actx.props.get("value")
        if mode == "append":
            if decl["type"] != "array":
                raise ActivityError(f"cannot append to variable '{name}' of type {decl['type']}; only array variables support mode 'append'", category="configuration", retryable=False)
            current = list(actx.expr.get_variable(name) or [])
            value = [*current, value]
        else:
            try:
                value = coerce_typed(value, decl["type"], name, what="variable")
            except ValueError as exc:
                raise ActivityError(str(exc), category="configuration", retryable=False) from None
        if len(json.dumps(value, default=str)) > 1024 * 1024:
            raise ActivityError(f"value of variable '{name}' is larger than 1 MiB", category="limit", retryable=False)
        return {"variable": name, "value": value, "_effects": {"variables": {name: value}}}


# ---------------------------------------------------------------------------------------------------------------------------------------- containers
class IfActivity(Container):
    """Run one of two nested activity lists depending on a boolean expression."""

    type = "if"
    title = "If condition"
    description = "Evaluate a boolean expression and run ifTrueActivities or ifFalseActivities."
    scope_keys: ClassVar[tuple[str, ...]] = ("ifTrueActivities", "ifFalseActivities")
    properties_schema = obj({"expression": X({"type": "boolean"}), "ifTrueActivities": {"type": "array"}, "ifFalseActivities": {"type": "array"}}, ["expression"])

    def scopes(self, props: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
        return [(k, props.get(k) or []) for k in self.scope_keys]

    def begin(self, actx: ActivityContext) -> dict[str, Any]:
        cond = actx.props.get("expression")
        try:
            branch = to_bool(cond, "if")
        except Exception:
            raise ActivityError(f"if expression must evaluate to a boolean but got {type(cond).__name__} {json.dumps(cond, default=str)[:60]}", category="expression", retryable=False) from None
        return {"branch": "true" if branch else "false"}

    def scope_defs(self, props: dict[str, Any], state: dict[str, Any]) -> list[dict[str, Any]]:
        return list(props.get("ifTrueActivities" if state["branch"] == "true" else "ifFalseActivities") or [])

    def finalize(self, state: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        return {"branch": state["branch"] == "true", "iterations": len(results)}


class SwitchActivity(Container):
    """Run the nested list of the first case whose value equals the evaluated ``on`` expression, else ``defaultActivities``."""

    type = "switch"
    title = "Switch"
    description = "Evaluate 'on' and run the activities of the matching case (string comparison is case-insensitive like the expression language) or the default."
    scope_keys: ClassVar[tuple[str, ...]] = ("cases", "defaultActivities")
    properties_schema = obj({
        "on": {},
        "cases": {"type": "array", "items": obj({"value": {}, "activities": {"type": "array"}}, ["value"])},
        "defaultActivities": {"type": "array"},
    }, ["on"])

    def scopes(self, props: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
        out: list[tuple[str, list[dict[str, Any]]]] = []
        for i, c in enumerate(props.get("cases") or []):
            if isinstance(c, dict):
                out.append((f"cases[{i}].activities", c.get("activities") or []))
        out.append(("defaultActivities", props.get("defaultActivities") or []))
        return out

    def begin(self, actx: ActivityContext) -> dict[str, Any]:
        on = actx.props.get("on")
        for i, c in enumerate(actx.definition.get("typeProperties", {}).get("cases") or []):
            try:
                val = evaluate_value(c.get("value"), actx.expr)
            except Exception as exc:  # noqa: BLE001
                raise ActivityError(f"switch case {i} value could not be evaluated: {exc}", category="expression", retryable=False) from None
            if equal(on, val):
                return {"case": i, "on": on if isinstance(on, (str, int, float, bool)) or on is None else to_str(on)}
        return {"case": "default", "on": on if isinstance(on, (str, int, float, bool)) or on is None else to_str(on)}

    def scope_defs(self, props: dict[str, Any], state: dict[str, Any]) -> list[dict[str, Any]]:
        if state["case"] == "default":
            return list(props.get("defaultActivities") or [])
        cases = props.get("cases") or []
        return list((cases[state["case"]] or {}).get("activities") or [])

    def finalize(self, state: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        return {"case": state["case"], "on": state.get("on")}


class ForEachActivity(Container):
    """Run the nested activities once per element of an array, sequentially or with bounded parallelism (``batchCount``)."""

    type = "for_each"
    title = "For each"
    description = "Iterate over an array; inside, item() and itemIndex() give the element. 'collect' names an inner activity whose outputs are gathered into output.results."
    scope_keys: ClassVar[tuple[str, ...]] = ("activities",)
    properties_schema = obj({
        "items": s_arr(), "isSequential": s_bool(), "batchCount": s_int(1, 50), "activities": {"type": "array"}, "collect": {"type": "string"},
        "continueOnFailure": s_bool(),
    }, ["items", "activities"])

    def scopes(self, props: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
        return [("activities", props.get("activities") or [])]

    def validate(self, props: dict[str, Any], path: str) -> list[ValidationIssue]:
        out: list[ValidationIssue] = []
        collect = props.get("collect")
        if isinstance(collect, str) and collect:
            names = {a.get("name") for a in props.get("activities") or [] if isinstance(a, dict)}
            if collect not in names:
                out.append(ValidationIssue(f"{path}.collect", f"'collect' names '{collect}' which is not an activity inside this for_each (inner activities: {', '.join(sorted(n for n in names if n)) or 'none'})", "unknown_activity"))
        return out

    def begin(self, actx: ActivityContext) -> dict[str, Any]:
        items = actx.props.get("items")
        if not isinstance(items, list):
            raise ActivityError(f"for_each 'items' must evaluate to an array but got {type(items).__name__}", category="expression", retryable=False)
        if len(items) > MAX_FOREACH_ITEMS:
            raise ActivityError(f"for_each is limited to {MAX_FOREACH_ITEMS} items but 'items' has {len(items)}", category="limit", retryable=False)
        if len(json.dumps(items, default=str)) > MAX_STATE_BYTES:
            raise ActivityError(f"the items of for_each are larger than {MAX_STATE_BYTES // 1024 // 1024} MiB; pass references (paths, ids) instead of data", category="limit", retryable=False)
        sequential = bool(actx.props.get("isSequential"))
        batch = actx.props.get("batchCount")
        par = 1 if sequential else int(batch) if isinstance(batch, int) and not isinstance(batch, bool) else 20
        collect = actx.props.get("collect")
        return {"items": items, "n": len(items), "parallel": max(1, min(50, par)), "continue": bool(actx.props.get("continueOnFailure", True)),
                "collect": collect if isinstance(collect, str) and collect else None}

    def scope_defs(self, props: dict[str, Any], state: dict[str, Any]) -> list[dict[str, Any]]:
        return list(props.get("activities") or [])

    def iterations(self, state: dict[str, Any]) -> int | None:
        return int(state["n"])

    def parallelism(self, state: dict[str, Any]) -> int:
        return int(state["parallel"])

    def iteration_context(self, state: dict[str, Any], iteration: int) -> dict[str, Any]:
        return {"item": state["items"][iteration], "item_index": iteration, "has_item": True}

    def finalize(self, state: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        out: dict[str, Any] = {"iterations": len(results), "succeeded": sum(1 for r in results if r["state"] == "succeeded"),
                               "failed": sum(1 for r in results if r["state"] == "failed")}
        collect = state.get("collect")
        if collect:
            out["results"] = [r["outputs"].get(collect) for r in results]
        return out


class UntilActivity(Container):
    """Repeat the nested activities until ``expression`` is true (checked after every iteration, i.e. do-while)."""

    type = "until"
    title = "Until"
    description = "Run the activities, then evaluate the expression; stop when it is true. Guarded by maxIterations (default 1000)."
    scope_keys: ClassVar[tuple[str, ...]] = ("activities",)
    lazy_keys: ClassVar[tuple[str, ...]] = ("expression",)
    properties_schema = obj({"expression": {}, "activities": {"type": "array"}, "maxIterations": s_int(1, 100000)}, ["expression", "activities"])

    def scopes(self, props: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
        return [("activities", props.get("activities") or [])]

    def begin(self, actx: ActivityContext) -> dict[str, Any]:
        mx = actx.props.get("maxIterations")
        return {"max": int(mx) if isinstance(mx, int) and not isinstance(mx, bool) else 1000, "done": 0}

    def scope_defs(self, props: dict[str, Any], state: dict[str, Any]) -> list[dict[str, Any]]:
        return list(props.get("activities") or [])

    def iterations(self, state: dict[str, Any]) -> int | None:
        return None

    def after_iteration(self, actx: ActivityContext, state: dict[str, Any], iteration: int, ok: bool) -> bool:
        if not ok:
            return False
        try:
            stop = to_bool(evaluate_value(actx.definition.get("typeProperties", {}).get("expression"), actx.expr), "until")
        except Exception as exc:  # noqa: BLE001
            raise ActivityError(f"until expression failed after iteration {iteration}: {exc}", category="expression", retryable=False) from None
        state["done"] = iteration + 1
        if stop:
            state["stopped_by"] = "condition"
            return False
        if iteration + 1 >= state["max"]:
            raise ActivityError(f"until reached maxIterations={state['max']} without the condition becoming true", category="limit", code="MaxIterations", retryable=False)
        return True

    def finalize(self, state: dict[str, Any], results: list[dict[str, Any]]) -> dict[str, Any]:
        return {"iterations": len(results)}


# ---------------------------------------------------------------------------------------------------------------------------------------- child pipeline
class ChildPipelineActivity(Activity):
    """Start another pipeline of the same workspace as a child run (own run record, linked by ``parent_activity_id``) and optionally wait for it."""

    type = "child_pipeline"
    title = "Execute pipeline"
    description = "Run another pipeline (by id or name) with parameters. Depth is limited to 8 and recursion (a pipeline calling itself, directly or not) is refused."
    mode = "async"
    default_timeout_seconds = None
    properties_schema = obj({"pipeline": s_str(minLength=1), "parameters": s_obj(), "waitOnCompletion": s_bool()}, ["pipeline"])

    def start(self, actx: ActivityContext) -> None:
        ref = actx.props.get("pipeline")
        if not isinstance(ref, str) or not ref:
            raise ActivityError("child_pipeline needs a pipeline id or name", category="configuration", retryable=False)
        params = actx.props.get("parameters") or {}
        if not isinstance(params, dict):
            raise ActivityError("child_pipeline 'parameters' must be an object", category="configuration", retryable=False)
        try:
            child = actx.engine.start_child(actx, ref, params)
        except OlcfError as exc:
            retry = exc.status >= 500 or exc.code in ("unavailable",)
            raise ActivityError(exc.message, category=exc.code, retryable=retry, details=exc.details) from None
        actx.save_state(child_run_id=child["id"], pipeline=child["pipeline_name"], wait=bool(actx.props.get("waitOnCompletion", True)))
        actx.log(f"started child run {child['id']} of pipeline '{child['pipeline_name']}'")

    def poll(self, actx: ActivityContext) -> dict[str, Any] | None:
        rid = actx.state.get("child_run_id")
        if not rid:
            raise ActivityError("child run id missing from the activity state", category="internal", retryable=False)
        if not actx.state.get("wait", True):
            return {"runId": rid, "pipeline": actx.state.get("pipeline"), "state": "started"}
        try:
            row = actx.engine.run_row(rid)
        except NotFound:
            raise ActivityError(f"child run {rid} disappeared", category="internal", retryable=False) from None
        st = row["state"]
        if st in ("queued", "running", "cancel_requested"):
            return None
        if st == "succeeded":
            return {"runId": rid, "pipeline": row["pipeline_name"], "state": st, "outputs": row.get("outputs") or {}}
        err = row.get("error") or {}
        if st == "cancelled" and not actx.is_cancelled():
            raise ActivityError(f"child run {rid} was cancelled: {row.get('state_reason') or ''}".strip(), category="cancelled", retryable=False, details={"child_run_id": rid})
        if st == "cancelled":
            raise ActivityCancelled()
        raise ActivityError(f"child run {rid} {st}: {err.get('message') or row.get('state_reason') or 'see the child run'}", category="child_failed", retryable=True,
                            details={"child_run_id": rid, "child_state": st})

    def poll_interval(self, actx: ActivityContext) -> float:
        return 0.5

    def cancel(self, actx: ActivityContext) -> None:
        rid = actx.state.get("child_run_id")
        if rid and actx.state.get("wait", True):
            try:
                actx.engine.cancel_run(rid, f"parent run {actx.run_id} was cancelled")
            except Exception:  # noqa: BLE001
                actx.log(f"could not cancel child run {rid}")


# ---------------------------------------------------------------------------------------------------------------------------------------- webhook
class WebhookWaitActivity(Activity):
    """Optionally call an external URL, then wait until something POSTs to this activity's callback URL (human approval, external system...).

    The callback URL is ``<base>/v1/hooks/pipelines/callbacks/<activity_run_id>/<token>``; the body ``{"output": ..., "statusCode": 200}`` completes the activity,
    ``{"error": {"message": ..., "errorCode": ...}}`` or a ``statusCode >= 400`` fails it. The outbound call goes through the SSRF-guarded client.
    """

    type = "webhook_wait"
    title = "Webhook (wait for callback)"
    description = "POST to a URL with a callBackUri in the body and wait for the callback; without a URL, only wait for the callback (callback URL is visible to pipeline.run holders)."
    mode = "async"
    default_timeout_seconds = 600
    reusable = False
    properties_schema = obj({
        "url": s_str(), "method": {"enum": ["POST", "PUT", "PATCH", "GET"]}, "headers": s_obj(), "body": {}, "timeoutSeconds": s_int(1, 30 * 86400),
        "requestTimeoutSeconds": s_int(1, 120),
    })
    sensitive_props = ("headers",)

    def timeout_for(self, props: dict[str, Any]) -> float | None:
        v = props.get("timeoutSeconds")
        return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0 else self.default_timeout_seconds

    def start(self, actx: ActivityContext) -> None:
        token = secrets.token_urlsafe(32)
        base = str(actx.ctx.settings.base_url).rstrip("/")
        callback = f"{base}/v1/hooks/pipelines/callbacks/{actx.activity_run_id}/{token}"
        actx.save_state(token=token, callback_url=callback, call_started=False, called=False)
        url = actx.props.get("url")
        if not url:
            return
        from olcf.pipelines import ssrf

        policy = ssrf.policy_from_ctx(actx.ctx)
        try:
            ssrf.validate_url(str(url), policy)
        except OlcfError as exc:
            raise ActivityError(exc.message, category="ssrf", retryable=False) from None
        body = actx.props.get("body")
        payload = {"callBackUri": callback, "runId": actx.run_id, "activityRunId": actx.activity_run_id, "pipeline": actx.run.get("pipeline_name")}
        if isinstance(body, dict):
            payload = {**body, **payload}
        elif body is not None:
            payload["data"] = body
        headers = {str(k): to_str(v) for k, v in (actx.props.get("headers") or {}).items()}
        headers.setdefault("Content-Type", "application/json")
        actx.save_state(call_started=True)
        method = actx.props.get("method") or "POST"
        client = ssrf.guarded_client(policy, timeout=float(actx.props.get("requestTimeoutSeconds") or 30), follow_redirects=False)
        try:
            with client:
                resp = client.request(method, str(url), content=json.dumps(payload).encode(), headers=headers)
        except OlcfError as exc:
            raise ActivityError(exc.message, category="ssrf", retryable=False) from None
        except Exception as exc:  # noqa: BLE001
            raise ActivityError(f"could not call {_host(str(url))}: {type(exc).__name__}: {exc}", category="network", retryable=True) from None
        if resp.status_code >= 400:
            raise ActivityError(f"the webhook target answered HTTP {resp.status_code}", category="http", retryable=resp.status_code >= 500, details={"status": resp.status_code})
        actx.save_state(called=True, call_status=resp.status_code)

    def resume(self, actx: ActivityContext) -> dict[str, Any] | None:
        st = actx.state
        if st.get("call_started") and not st.get("called"):
            raise ActivityError("the control plane restarted while the outbound webhook call was in flight; it is not repeated automatically (it may have reached the target)",
                                category="interrupted", code="interrupted", retryable=False)
        if not st.get("token"):
            self.start(actx)
        return self.poll(actx)

    def poll(self, actx: ActivityContext) -> dict[str, Any] | None:
        sig = actx.signal
        if sig is None:
            if actx.is_cancelled():
                raise ActivityCancelled()
            return None
        body = sig.get("body") if isinstance(sig, dict) and "body" in sig else sig
        if isinstance(body, dict):
            code = body.get("statusCode", 200)
            err = body.get("error")
            if err or (isinstance(code, int) and code >= 400):
                e = err if isinstance(err, dict) else {"message": str(err or f"callback reported statusCode {code}")}
                raise ActivityError(str(e.get("message") or "callback reported a failure"), category="callback", code=str(e.get("errorCode") or "CallbackFailed"), retryable=False)
            return {"callback": True, "statusCode": code, "output": body.get("output", {k: v for k, v in body.items() if k not in ("statusCode", "error")})}
        return {"callback": True, "statusCode": 200, "output": body}

    def poll_interval(self, actx: ActivityContext) -> float:
        return 0.5

    def public_state(self, state: dict[str, Any]) -> dict[str, Any]:
        url = state.get("callback_url")
        return {"callback_url": url} if url else {}

    def describe(self) -> dict[str, Any]:
        d = super().describe()
        d["callback"] = "POST /v1/hooks/pipelines/callbacks/{activity_run_id}/{token}"
        return d


def _host(url: str) -> str:
    from urllib.parse import urlsplit

    try:
        return urlsplit(url).hostname or "target"
    except ValueError:
        return "target"


def verify_callback_token(state: dict[str, Any], token: str) -> bool:
    """Constant-time comparison of a presented callback token with the one stored in the activity state."""
    stored = state.get("token") or ""
    return bool(stored) and hmac.compare_digest(hashlib.sha256(stored.encode()).digest(), hashlib.sha256(token.encode()).digest())


def callback_body(raw: bytes) -> Any:
    """Parse a callback body (size-capped JSON; an empty body counts as ``{}``)."""
    if len(raw) > MAX_CALLBACK_BYTES:
        raise ActivityError(f"callback body is larger than {MAX_CALLBACK_BYTES // 1024} KiB", category="limit", retryable=False)
    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except ValueError:
        raise ActivityError("callback body must be JSON", category="callback", retryable=False) from None


CONTROL_ACTIVITIES: list[type[Activity]] = [WaitActivity, FailActivity, SetVariableActivity, IfActivity, SwitchActivity, ForEachActivity, UntilActivity, ChildPipelineActivity,
                                            WebhookWaitActivity]
__all__ = ["CONTROL_ACTIVITIES", "callback_body", "now_iso", "verify_callback_token"]
