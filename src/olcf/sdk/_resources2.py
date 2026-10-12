# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Second half of the REST wrappers: orchestration, compute fabric, runtimes, Fabric integration, MCP and sample data.

See :mod:`olcf.sdk._resources` for the conventions; the classes here follow them exactly.
"""

from __future__ import annotations

import builtins
from collections.abc import Callable
from typing import Any

from olcf.sdk._resources import JSON, Body, _Res


class Pipelines(_Res):
    """Pipelines, their runs, schedules, triggers, webhooks and alerts."""

    def activity_types(self) -> JSON:
        return self._call("GET", "/pipelines/activity-types")

    def schema(self) -> JSON:
        return self._call("GET", "/pipelines/schema")

    def validate(self, definition: Body | str) -> JSON:
        """Validate a pipeline definition (JSON object or text)."""
        body = {"definition": definition} if isinstance(definition, dict) else {"text": definition}
        return self._call("POST", "/pipelines/validate", json=body)

    def import_fabric(self, document: Body, *, strict: bool = False, name: str | None = None, item_map: dict[str, str] | None = None,
                      workspace_id: str | None = None, create: bool = False) -> JSON:
        """Translate a Fabric / Data Factory pipeline document; ``strict`` fails on activities without an OLCF equivalent instead of inserting placeholders."""
        body: dict[str, Any] = {"document": document, "on_unsupported": "error" if strict else "placeholder"}
        for key, val in (("name", name), ("item_map", item_map), ("workspace_id", workspace_id)):
            if val:
                body[key] = val
        if create:
            body["create"] = True
        return self._call("POST", "/pipelines/import-fabric", json=body)

    def list_pipelines(self, ws: str, *, q: str | None = None, include_definition: bool = False, limit: int = 50, cursor: str | None = None) -> JSON:
        """``GET /workspaces/{ws}/pipelines``: the workspace's pipelines with last run state and trigger summary; ``include_definition`` embeds the (size-capped) definitions."""
        return self._call("GET", "/workspaces/{ws}/pipelines", path={"ws": ws}, params={"q": q, "include": "definition" if include_definition else None, "limit": limit, "cursor": cursor})

    def definition(self, pipeline_id: str, *, format: str | None = None) -> JSON:  # noqa: A002
        return self._call("GET", "/pipelines/{id}/definition", path={"id": pipeline_id}, params={"format": format})

    def run(self, pipeline_id: str, parameters: Body | None = None, *, idempotency_key: str | None = None, **extra: Any) -> JSON:
        return self._call("POST", "/pipelines/{id}/runs", path={"id": pipeline_id}, json={"parameters": dict(parameters or {}), **extra},
                          idem=True, idempotency_key=idempotency_key)

    def runs_of(self, pipeline_id: str, *, state: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/pipelines/{id}/runs", path={"id": pipeline_id}, params={"state": state, "limit": limit, "cursor": cursor})

    def triggers_of(self, pipeline_id: str) -> JSON:
        return self._call("GET", "/pipelines/{id}/triggers", path={"id": pipeline_id})

    def list_runs(self, *, workspace_id: str | None = None, pipeline_id: str | None = None, state: str | None = None, trigger_type: str | None = None,
                  parent_run_id: str | None = None, group_id: str | None = None, since: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/pipeline-runs", params={"workspace_id": workspace_id, "pipeline_id": pipeline_id, "state": state, "trigger_type": trigger_type,
                                                            "parent_run_id": parent_run_id, "group_id": group_id, "since": since, "limit": limit, "cursor": cursor})

    def get_run(self, run_id: str) -> JSON:
        return self._call("GET", "/pipeline-runs/{id}", path={"id": run_id})

    def run_graph(self, run_id: str, *, include_io: bool = False) -> JSON:
        return self._call("GET", "/pipeline-runs/{id}/graph", path={"id": run_id}, params={"include_io": include_io or None})

    def run_activities(self, run_id: str, *, include_io: bool = False) -> JSON:
        return self._call("GET", "/pipeline-runs/{id}/activities", path={"id": run_id}, params={"include_io": include_io or None})

    def run_activity(self, run_id: str, activity_id: str) -> JSON:
        return self._call("GET", "/pipeline-runs/{id}/activities/{aid}", path={"id": run_id, "aid": activity_id})

    def run_children(self, run_id: str) -> JSON:
        return self._call("GET", "/pipeline-runs/{id}/children", path={"id": run_id})

    def cancel_run(self, run_id: str, **body: Any) -> JSON:
        return self._call("POST", "/pipeline-runs/{id}/cancel", path={"id": run_id}, json=body)

    def resume_run(self, run_id: str, **body: Any) -> JSON:
        return self._call("POST", "/pipeline-runs/{id}/resume", path={"id": run_id}, json=body)

    def rerun(self, run_id: str, **body: Any) -> JSON:
        return self._call("POST", "/pipeline-runs/{id}/rerun", path={"id": run_id}, json=body, idem=True)

    # schedules
    def list_schedules(self, workspace_id: str) -> JSON:
        return self._call("GET", "/schedules", params={"workspace_id": workspace_id})

    def create_schedule(self, body: Body) -> JSON:
        return self._call("POST", "/schedules", json=body)

    def preview_schedule(self, body: Body) -> JSON:
        return self._call("POST", "/schedules/preview", json=body)

    def get_schedule(self, item_id: str) -> JSON:
        return self._call("GET", "/schedules/{id}", path={"id": item_id})

    def enable_schedule(self, item_id: str) -> JSON:
        return self._call("POST", "/schedules/{id}/enable", path={"id": item_id})

    def disable_schedule(self, item_id: str) -> JSON:
        return self._call("POST", "/schedules/{id}/disable", path={"id": item_id})

    def pause_schedule(self, item_id: str, **body: Any) -> JSON:
        return self._call("POST", "/schedules/{id}/pause", path={"id": item_id}, json=body)

    def resume_schedule(self, item_id: str) -> JSON:
        return self._call("POST", "/schedules/{id}/resume", path={"id": item_id})

    def schedule_next(self, item_id: str, *, count: int | None = None) -> JSON:
        return self._call("GET", "/schedules/{id}/next", path={"id": item_id}, params={"count": count})

    def run_schedule_now(self, item_id: str) -> JSON:
        return self._call("POST", "/schedules/{id}/run-now", path={"id": item_id}, idem=True)

    def schedule_runs(self, item_id: str, *, limit: int | None = None) -> JSON:
        return self._call("GET", "/schedules/{id}/runs", path={"id": item_id}, params={"limit": limit})

    # triggers
    def list_triggers(self, workspace_id: str, *, type: str | None = None, target_id: str | None = None) -> JSON:  # noqa: A002
        return self._call("GET", "/triggers", params={"workspace_id": workspace_id, "type": type, "target_id": target_id})

    def create_trigger(self, body: Body) -> JSON:
        return self._call("POST", "/triggers", json=body)

    def get_trigger(self, trigger_id: str) -> JSON:
        return self._call("GET", "/triggers/{id}", path={"id": trigger_id})

    def update_trigger(self, trigger_id: str, if_match: Any = None, **fields: Any) -> JSON:
        return self._call("PATCH", "/triggers/{id}", path={"id": trigger_id}, json=fields, if_match=if_match)

    def delete_trigger(self, trigger_id: str) -> None:
        return self._call("DELETE", "/triggers/{id}", path={"id": trigger_id})

    def enable_trigger(self, trigger_id: str) -> JSON:
        return self._call("POST", "/triggers/{id}/enable", path={"id": trigger_id})

    def disable_trigger(self, trigger_id: str) -> JSON:
        return self._call("POST", "/triggers/{id}/disable", path={"id": trigger_id})

    def pause_trigger(self, trigger_id: str, **body: Any) -> JSON:
        return self._call("POST", "/triggers/{id}/pause", path={"id": trigger_id}, json=body)

    def resume_trigger(self, trigger_id: str) -> JSON:
        return self._call("POST", "/triggers/{id}/resume", path={"id": trigger_id})

    def trigger_next(self, trigger_id: str, *, count: int | None = None) -> JSON:
        return self._call("GET", "/triggers/{id}/next", path={"id": trigger_id}, params={"count": count})

    def run_trigger_now(self, trigger_id: str) -> JSON:
        return self._call("POST", "/triggers/{id}/run-now", path={"id": trigger_id}, idem=True)

    def rotate_trigger_secret(self, trigger_id: str) -> JSON:
        return self._call("POST", "/triggers/{id}/rotate-secret", path={"id": trigger_id})

    def trigger_runs(self, trigger_id: str, *, limit: int | None = None) -> JSON:
        return self._call("GET", "/triggers/{id}/runs", path={"id": trigger_id}, params={"limit": limit})

    # public hooks (credential travels in the request, not in a bearer token)
    def fire_webhook_trigger(self, trigger_id: str, payload: Body | None = None, *, headers: dict[str, str] | None = None) -> JSON:
        """``POST /hooks/pipelines/{trigger_id}``: pass the HMAC ``X-OLCF-Signature`` (or ``Authorization``) header the trigger expects."""
        return self._call("POST", "/hooks/pipelines/{id}", path={"id": trigger_id}, json=dict(payload or {}), headers=headers, auth=False)

    def complete_callback(self, activity_run_id: str, token: str, payload: Body | None = None) -> JSON:
        return self._call("POST", "/hooks/pipelines/callbacks/{aid}/{token}", path={"aid": activity_run_id, "token": token}, json=dict(payload or {}), auth=False)

    # webhooks
    def list_webhooks(self, *, workspace_id: str | None = None) -> JSON:
        return self._call("GET", "/webhooks", params={"workspace_id": workspace_id})

    def webhook_event_types(self) -> JSON:
        return self._call("GET", "/webhooks/event-types")

    def create_webhook(self, body: Body) -> JSON:
        """The signing secret is returned once."""
        return self._call("POST", "/webhooks", json=body)

    def get_webhook(self, webhook_id: str) -> JSON:
        return self._call("GET", "/webhooks/{id}", path={"id": webhook_id})

    def update_webhook(self, webhook_id: str, if_match: Any = None, **fields: Any) -> JSON:
        return self._call("PATCH", "/webhooks/{id}", path={"id": webhook_id}, json=fields, if_match=if_match)

    def delete_webhook(self, webhook_id: str) -> None:
        return self._call("DELETE", "/webhooks/{id}", path={"id": webhook_id})

    def test_webhook(self, webhook_id: str) -> JSON:
        return self._call("POST", "/webhooks/{id}/test", path={"id": webhook_id})

    def rotate_webhook_secret(self, webhook_id: str, **body: Any) -> JSON:
        return self._call("POST", "/webhooks/{id}/rotate-secret", path={"id": webhook_id}, json=body)

    def webhook_deliveries(self, webhook_id: str, *, state: str | None = None, event_type: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/webhooks/{id}/deliveries", path={"id": webhook_id}, params={"state": state, "event_type": event_type, "limit": limit, "cursor": cursor})

    def delivery(self, delivery_id: str) -> JSON:
        return self._call("GET", "/webhooks/deliveries/{id}", path={"id": delivery_id})

    def redeliver(self, delivery_id: str) -> JSON:
        return self._call("POST", "/webhooks/deliveries/{id}/redeliver", path={"id": delivery_id})

    # alerts
    def list_alerts(self, *, state: str | None = None, severity: str | None = None, rule_id: str | None = None, since: str | None = None,
                    limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/alerts", params={"state": state, "severity": severity, "rule_id": rule_id, "since": since, "limit": limit, "cursor": cursor})

    def get_alert(self, alert_id: str) -> JSON:
        return self._call("GET", "/alerts/{id}", path={"id": alert_id})

    def ack_alert(self, alert_id: str) -> JSON:
        return self._call("POST", "/alerts/{id}/ack", path={"id": alert_id})

    def silence_alert(self, alert_id: str, **body: Any) -> JSON:
        return self._call("POST", "/alerts/{id}/silence", path={"id": alert_id}, json=body)

    def evaluate_alerts(self) -> JSON:
        return self._call("POST", "/alerts/evaluate")

    def alert_rule_types(self) -> JSON:
        return self._call("GET", "/alerts/rule-types")

    def list_alert_rules(self, *, workspace_id: str | None = None) -> JSON:
        return self._call("GET", "/alerts/rules", params={"workspace_id": workspace_id})

    def create_alert_rule(self, body: Body) -> JSON:
        return self._call("POST", "/alerts/rules", json=body)

    def get_alert_rule(self, rule_id: str) -> JSON:
        return self._call("GET", "/alerts/rules/{id}", path={"id": rule_id})

    def update_alert_rule(self, rule_id: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/alerts/rules/{id}", path={"id": rule_id}, json=fields)

    def delete_alert_rule(self, rule_id: str) -> None:
        return self._call("DELETE", "/alerts/rules/{id}", path={"id": rule_id})

    def silence_alert_rule(self, rule_id: str, **body: Any) -> JSON:
        return self._call("POST", "/alerts/rules/{id}/silence", path={"id": rule_id}, json=body)

    def test_alert_rule(self, rule_id: str) -> JSON:
        return self._call("POST", "/alerts/rules/{id}/test", path={"id": rule_id})

    def list_alert_channels(self) -> JSON:
        return self._call("GET", "/alerts/channels")

    def create_alert_channel(self, body: Body) -> JSON:
        return self._call("POST", "/alerts/channels", json=body)

    def update_alert_channel(self, channel_id: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/alerts/channels/{id}", path={"id": channel_id}, json=fields)

    def delete_alert_channel(self, channel_id: str) -> None:
        return self._call("DELETE", "/alerts/channels/{id}", path={"id": channel_id})

    def test_alert_channel(self, channel_id: str) -> JSON:
        return self._call("POST", "/alerts/channels/{id}/test", path={"id": channel_id})


class Environments(_Res):
    """Environments (dependency sets), variable sets and deployment pipelines."""

    def policy(self) -> JSON:
        return self._call("GET", "/environments/policy")

    def lock(self, item_id: str) -> JSON:
        return self._call("GET", "/environments/{id}/lock", path={"id": item_id})

    def resolve(self, item_id: str, *, version: int | None = None) -> JSON:
        return self._call("GET", "/environments/{id}/resolve", path={"id": item_id}, params={"version": version})

    def verify(self, item_id: str, *, version: int | None = None) -> JSON:
        return self._call("POST", "/environments/{id}/verify", path={"id": item_id}, params={"version": version})

    def publish(self, item_id: str, *, comment: str = "", dry_run: bool = False, force: bool = False, verify: bool = False, wait: bool = False) -> JSON:
        return self._call("POST", "/environments/{id}/publish", path={"id": item_id}, json={"comment": comment, "dry_run": dry_run, "force": force, "verify": verify, "wait": wait})

    def publications(self, item_id: str) -> JSON:
        return self._call("GET", "/environments/{id}/publications", path={"id": item_id})

    def publication(self, item_id: str, version: int) -> JSON:
        return self._call("GET", "/environments/{id}/publications/{version}", path={"id": item_id, "version": version})

    def diff(self, item_id: str, a: int, b: int) -> JSON:
        return self._call("GET", "/environments/{id}/diff", path={"id": item_id}, params={"a": a, "b": b})

    def requirements_from_notebook(self, item_id: str, notebook_id: str, *, apply: bool = False) -> JSON:
        return self._call("POST", "/environments/{id}/requirements/from-notebook", path={"id": item_id}, json={"notebook_id": notebook_id, "apply": apply})

    def resolve_variable_set(self, item_id: str, **params: Any) -> JSON:
        return self._call("GET", "/variable-sets/{id}/resolve", path={"id": item_id}, params=params)

    def list_deployment_pipelines(self, *, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/deployment-pipelines", params={"limit": limit, "cursor": cursor})

    def create_deployment_pipeline(self, name: str, stages: list[Body], *, description: str = "", rules: Body | None = None) -> JSON:
        return self._call("POST", "/deployment-pipelines", json={"name": name, "description": description, "stages": stages, "rules": dict(rules or {})})

    def get_deployment_pipeline(self, pid: str) -> JSON:
        return self._call("GET", "/deployment-pipelines/{pid}", path={"pid": pid})

    def update_deployment_pipeline(self, pid: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/deployment-pipelines/{pid}", path={"pid": pid}, json=fields)

    def delete_deployment_pipeline(self, pid: str) -> None:
        return self._call("DELETE", "/deployment-pipelines/{pid}", path={"pid": pid})

    def deploy(self, pid: str, **body: Any) -> JSON:
        return self._call("POST", "/deployment-pipelines/{pid}/deploy", path={"pid": pid}, json=body)

    def deployments(self, pid: str) -> JSON:
        return self._call("GET", "/deployment-pipelines/{pid}/deployments", path={"pid": pid})

    def deployment(self, deployment_id: str) -> JSON:
        return self._call("GET", "/deployments/{did}", path={"did": deployment_id})


class Compute(_Res):
    """Workers, pools, queues, clusters, providers, runtimes and the compute topology."""

    def topology(self) -> JSON:
        return self._call("GET", "/compute/topology")

    def test(self) -> JSON:
        """Probe matrix executed from every node's point of view."""
        return self._call("POST", "/compute/test")

    def spark_smoke(self, *, runtime_id: str | None = None, partitions: int | None = None, executors: int | None = None,
                    timeout_seconds: int | None = None, pool: str | None = None) -> JSON:
        body = {k: v for k, v in (("runtime_id", runtime_id), ("partitions", partitions), ("executors", executors), ("timeout_seconds", timeout_seconds), ("pool", pool)) if v is not None}
        return self._call("POST", "/compute/spark-smoke", json=body or None)

    def spark_smoke_status(self, smoke_id: str) -> JSON:
        """The report of a smoke test started with :meth:`spark_smoke` (``finished`` is false while it runs; ``ok`` is only set once it has finished)."""
        return self._call("GET", "/compute/spark-smoke/{id}", path={"id": smoke_id})

    def spark_smoke_cancel(self, smoke_id: str) -> JSON:
        """Stop a running or queued smoke test (idempotent); returns its current report."""
        return self._call("POST", "/compute/spark-smoke/{id}/cancel", path={"id": smoke_id})

    def spark_smoke_wait(self, smoke_id: str, *, timeout: float = 600.0, interval: float = 2.0, on_progress: Callable[[JSON], None] | None = None) -> JSON:
        """Poll until the smoke test has finished and return its report; ``on_progress`` is called with every poll. Raises ``TimeoutError`` after ``timeout`` seconds
        (the controller enforces its own deadline, so this is a client-side bound only)."""
        import time

        stop = time.monotonic() + timeout
        while True:
            doc = self.spark_smoke_status(smoke_id)
            if on_progress is not None:
                on_progress(doc)
            if doc.get("finished"):
                return doc
            if time.monotonic() >= stop:
                raise TimeoutError(f"Spark smoke test {smoke_id} did not finish within {timeout:.0f} s (phase {doc.get('phase')}; it keeps running on the controller until its deadline)")
            time.sleep(interval)

    # workers
    def list_workers(self, **params: Any) -> JSON:
        return self._call("GET", "/workers", params=params)

    def get_worker(self, worker_id: str) -> JSON:
        return self._call("GET", "/workers/{id}", path={"id": worker_id})

    def create_enrollment_token(self, *, name: str | None = None, ttl_seconds: int | None = None, pool: str | None = None, labels: Body | None = None,
                                replace_existing: bool = False) -> JSON:
        """Single-use ``olcf_enr_...`` token plus the ready-to-paste ``olcf-worker run`` command."""
        body: dict[str, Any] = {"name": name, "pool": pool, "labels": dict(labels or {}), "replace_existing": replace_existing}
        if ttl_seconds is not None:
            body["ttl_seconds"] = ttl_seconds
        return self._call("POST", "/workers/enrollment-tokens", json=body)

    def enroll_worker(self, body: Body) -> JSON:
        """Agent-side enrolment call (``POST /workers/enroll``); normally made by ``olcf-worker``."""
        return self._call("POST", "/workers/enroll", json=body, auth=False)

    def update_worker(self, worker_id: str, if_match: Any = None, **fields: Any) -> JSON:
        return self._call("PATCH", "/workers/{id}", path={"id": worker_id}, json=fields, if_match=if_match)

    def delete_worker(self, worker_id: str) -> None:
        return self._call("DELETE", "/workers/{id}", path={"id": worker_id})

    def cordon(self, worker_id: str, reason: str = "") -> JSON:
        return self._call("POST", "/workers/{id}/cordon", path={"id": worker_id}, json={"reason": reason})

    def uncordon(self, worker_id: str, reason: str = "") -> JSON:
        return self._call("POST", "/workers/{id}/uncordon", path={"id": worker_id}, json={"reason": reason})

    def drain(self, worker_id: str, reason: str = "") -> JSON:
        return self._call("POST", "/workers/{id}/drain", path={"id": worker_id}, json={"reason": reason})

    def revoke(self, worker_id: str, reason: str = "") -> JSON:
        return self._call("POST", "/workers/{id}/revoke", path={"id": worker_id}, json={"reason": reason})

    def set_worker_labels(self, worker_id: str, labels: Body) -> JSON:
        return self._call("PUT", "/workers/{id}/labels", path={"id": worker_id}, json=dict(labels))

    def set_worker_pool(self, worker_id: str, pool: str) -> JSON:
        return self._call("PUT", "/workers/{id}/pool", path={"id": worker_id}, json={"pool": pool})

    def test_worker(self, worker_id: str) -> JSON:
        return self._call("POST", "/workers/{id}/test", path={"id": worker_id})

    def renew_worker(self, worker_id: str, *, certificate: bool = True, token: bool = True) -> JSON:
        """Make a connected worker renew its machine certificate and/or worker token now; returns ``{"renewed": [...], "worker": {...}}``."""
        return self._call("POST", "/workers/{id}/renew", path={"id": worker_id}, json={"certificate": certificate, "token": token})

    # pools & queues
    def list_pools(self) -> JSON:
        return self._call("GET", "/worker-pools")

    def create_pool(self, **body: Any) -> JSON:
        return self._call("POST", "/worker-pools", json=body)

    def get_pool(self, pool: str) -> JSON:
        return self._call("GET", "/worker-pools/{pool}", path={"pool": pool})

    def update_pool(self, pool: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/worker-pools/{pool}", path={"pool": pool}, json=fields)

    def delete_pool(self, pool: str) -> None:
        return self._call("DELETE", "/worker-pools/{pool}", path={"pool": pool})

    def list_queues(self) -> JSON:
        return self._call("GET", "/queues")

    def create_queue(self, **body: Any) -> JSON:
        return self._call("POST", "/queues", json=body)

    def get_queue(self, queue: str) -> JSON:
        return self._call("GET", "/queues/{queue}", path={"queue": queue})

    def update_queue(self, queue: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/queues/{queue}", path={"queue": queue}, json=fields)

    def delete_queue(self, queue: str) -> None:
        return self._call("DELETE", "/queues/{queue}", path={"queue": queue})

    def pause_queue(self, queue: str) -> JSON:
        return self._call("POST", "/queues/{queue}/pause", path={"queue": queue})

    # clusters & providers
    def list_clusters(self) -> JSON:
        return self._call("GET", "/clusters")

    def create_cluster(self, runtime_id: str, *, pool: str = "default", provider: str = "standalone") -> JSON:
        return self._call("POST", "/clusters", json={"runtime_id": runtime_id, "pool": pool, "provider": provider})

    def get_cluster(self, cluster_id: str) -> JSON:
        return self._call("GET", "/clusters/{id}", path={"id": cluster_id})

    def refresh_cluster(self, cluster_id: str) -> JSON:
        return self._call("POST", "/clusters/{id}/refresh", path={"id": cluster_id})

    def stop_cluster(self, cluster_id: str) -> JSON:
        return self._call("POST", "/clusters/{id}/stop", path={"id": cluster_id})

    def provider_types(self) -> JSON:
        return self._call("GET", "/providers/types")

    def list_providers(self) -> JSON:
        return self._call("GET", "/providers")

    def create_provider(self, **body: Any) -> JSON:
        return self._call("POST", "/providers", json=body)

    def get_provider(self, provider: str) -> JSON:
        return self._call("GET", "/providers/{provider}", path={"provider": provider})

    def update_provider(self, provider: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/providers/{provider}", path={"provider": provider}, json=fields)

    def delete_provider(self, provider: str) -> None:
        return self._call("DELETE", "/providers/{provider}", path={"provider": provider})

    def discover_provider(self, provider: str) -> JSON:
        return self._call("POST", "/providers/{provider}/discover", path={"provider": provider})

    # spark UI proxy (HEAD/GET) is intentionally not wrapped: open the URL from `spark_application()['ui_url']` in a browser.


class Runtimes(_Res):
    """Spark runtimes: catalog, per-node install / verify / repair / retire / rollback, offline bundles."""

    def list(self, *, include_retired: bool = False) -> JSON:
        return self._call("GET", "/runtimes", params={"include_retired": include_retired or None})

    def matrix(self) -> JSON:
        return self._call("GET", "/runtimes/matrix")

    def get(self, runtime_id: str) -> JSON:
        return self._call("GET", "/runtimes/{id}", path={"id": runtime_id})

    def register(self, manifest: Body) -> JSON:
        """Register a signed manifest (untrusted/modified manifests are refused)."""
        return self._call("POST", "/runtimes", json={"manifest": dict(manifest)})

    def resolve(self, runtime: Body) -> JSON:
        return self._call("POST", "/runtimes/resolve", json={"runtime": dict(runtime)})

    def install(self, runtime_id: str, *, worker_ids: builtins.list[str] | None = None, pool: str | None = None, bundle_id: str | None = None, force: bool = False) -> JSON:
        return self._call("POST", "/runtimes/{id}/install", path={"id": runtime_id}, json={"worker_ids": worker_ids, "pool": pool, "bundle_id": bundle_id, "force": force})

    def verify(self, runtime_id: str, *, worker_ids: builtins.list[str] | None = None, pool: str | None = None) -> JSON:
        return self._call("POST", "/runtimes/{id}/verify", path={"id": runtime_id}, json={"worker_ids": worker_ids, "pool": pool})

    def repair(self, runtime_id: str, *, worker_ids: builtins.list[str] | None = None, pool: str | None = None, force: bool = False) -> JSON:
        return self._call("POST", "/runtimes/{id}/repair", path={"id": runtime_id}, json={"worker_ids": worker_ids, "pool": pool, "force": force})

    def retire(self, runtime_id: str, *, worker_ids: builtins.list[str] | None = None, pool: str | None = None) -> JSON:
        return self._call("POST", "/runtimes/{id}/retire", path={"id": runtime_id}, json={"worker_ids": worker_ids, "pool": pool})

    def rollback(self, runtime_id: str, *, worker_ids: builtins.list[str] | None = None, pool: str | None = None) -> JSON:
        return self._call("POST", "/runtimes/{id}/rollback", path={"id": runtime_id}, json={"worker_ids": worker_ids, "pool": pool})

    def list_bundles(self) -> JSON:
        return self._call("GET", "/runtimes/bundles")

    def upload_bundle(self, data: bytes, *, filename: str = "bundle.tar.gz", runtime_id: str | None = None) -> JSON:
        return self._call("POST", "/runtimes/bundles", params={"runtime_id": runtime_id}, files={"file": (filename, data, "application/gzip")})

    def download_bundle(self, bundle_id: str) -> bytes:
        return self._call("GET", "/runtimes/bundles/{id}/download", path={"id": bundle_id}, as_bytes=True)

    def delete_bundle(self, bundle_id: str) -> None:
        return self._call("DELETE", "/runtimes/bundles/{id}", path={"id": bundle_id})


class Fabric(_Res):
    """Microsoft Fabric Compute Extension: configuration, diagnostics, submissions, analyzer, notebook snippet."""

    def status(self) -> JSON:
        return self._call("GET", "/integrations/fabric")

    def configure(self, config: Body) -> JSON:
        return self._call("PUT", "/integrations/fabric", json=dict(config))

    def test(self) -> JSON:
        """Stepwise DNS -> TLS -> proxy -> Entra token -> Fabric API -> OneLake diagnostics."""
        return self._call("POST", "/integrations/fabric/test")

    def handshake(self, body: Body | None = None) -> JSON:
        return self._call("POST", "/integrations/fabric/handshake", json=dict(body or {}))

    def challenge(self) -> JSON:
        return self._call("POST", "/integrations/fabric/challenge")

    def validate(self, capsule: Body) -> JSON:
        return self._call("POST", "/integrations/fabric/validate", json=dict(capsule))

    def submit(self, capsule: Body) -> JSON:
        return self._call("POST", "/integrations/fabric/submit", json=dict(capsule), idem=True)

    def submissions(self, *, limit: int = 50, state: str | None = None, notebook_id: str | None = None, fabric_workspace_id: str | None = None,
                    before: str | None = None) -> JSON:
        return self._call("GET", "/integrations/fabric/submissions", params={"limit": limit, "state": state, "notebook_id": notebook_id,
                                                                              "fabric_workspace_id": fabric_workspace_id, "before": before})

    def submission(self, submission_id: str) -> JSON:
        return self._call("GET", "/integrations/fabric/submissions/{id}", path={"id": submission_id})

    def submission_logs(self, submission_id: str, *, offset: int = 0, limit: int = 65536) -> JSON:
        return self._call("GET", "/integrations/fabric/submissions/{id}/logs", path={"id": submission_id}, params={"offset": offset, "limit": limit})

    def submission_output(self, submission_id: str) -> JSON:
        return self._call("GET", "/integrations/fabric/submissions/{id}/output", path={"id": submission_id})

    def cancel_submission(self, submission_id: str) -> JSON:
        return self._call("POST", "/integrations/fabric/submissions/{id}/cancel", path={"id": submission_id})

    def metrics(self, *, window: str = "24h") -> JSON:
        return self._call("GET", "/integrations/fabric/metrics", params={"window": window})

    def rotate_signing_key(self) -> JSON:
        return self._call("POST", "/integrations/fabric/signing/rotate")

    def compat_rules(self) -> JSON:
        return self._call("GET", "/integrations/fabric/compat/rules")

    def snippet(self, *, workspace: str | None = None, auth: str = "keyvault") -> JSON:
        """Ready-to-paste Fabric notebook cell."""
        return self._call("GET", "/integrations/fabric/snippet", params={"workspace": workspace, "auth": auth})

    def analyze(self, content: str, *, format: str | None = None, lakehouse: str | None = None, runtime_family: str | None = None) -> JSON:  # noqa: A002
        """Compatibility report for notebook text (``ipynb``, ``fabric_git_source`` or ``py``); creates no job."""
        body = {k: v for k, v in (("content", content), ("format", format), ("lakehouse", lakehouse), ("runtime_family", runtime_family)) if v is not None}
        return self._call("POST", "/integrations/fabric/analyze", json=body)


class Mcp(_Res):
    """The Model Context Protocol endpoint (info and call log; use any MCP client against ``/mcp``)."""

    def info(self) -> JSON:
        return self._call("GET", "/mcp/info")

    def calls(self, **params: Any) -> JSON:
        return self._call("GET", "/mcp/calls", params=params)

    def endpoint_get(self) -> Any:
        """``GET /mcp`` (the server answers 405: it never initiates messages); exposed for completeness."""
        return self._call("GET", "/mcp", raw=True, expect_error=True)

    def endpoint_post(self, message: Body) -> Any:
        """One JSON-RPC message to the Streamable HTTP endpoint."""
        return self._call("POST", "/mcp", json=dict(message), headers={"Accept": "application/json, text/event-stream"}, raw=True)

    def endpoint_delete(self, session_id: str) -> Any:
        return self._call("DELETE", "/mcp", headers={"Mcp-Session-Id": session_id}, raw=True, expect_error=True)


class Sample(_Res):
    """The sample retail workspace."""

    def status(self, *, workspace_name: str | None = None, lakehouse_name: str | None = None) -> JSON:
        return self._call("GET", "/sample/status", params={"workspace_name": workspace_name, "lakehouse_name": lakehouse_name})

    def install(self, *, workspace_name: str | None = None, lakehouse_name: str | None = None, reinstall: bool = False, size: str = "standard") -> JSON:
        body: dict[str, Any] = {"reinstall": reinstall, "size": size}
        if workspace_name:
            body["workspace_name"] = workspace_name
        if lakehouse_name:
            body["lakehouse_name"] = lakehouse_name
        return self._call("POST", "/sample/install", json=body)
