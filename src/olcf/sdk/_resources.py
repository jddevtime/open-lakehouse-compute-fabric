# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Typed method wrappers for the OLCF REST surface (``/v1``), written once and shared by the sync and async clients.

Every method is a thin, documented call of ``self._call(METHOD, "/template/{var}", path={...}, ...)``.  In :class:`olcf.sdk.Client`
``_call`` performs the request and returns the decoded result; in :class:`olcf.sdk.AsyncClient` it returns an awaitable, so the very same
classes serve both - ``client.jobs.get(id)`` or ``await aclient.jobs.get(id)``.  Methods that need a loop (pagination, waiting,
log following) live in :mod:`olcf.sdk.client` / :mod:`olcf.sdk.aio` because they differ between sync and async.

Return values: JSON objects are :class:`~olcf.sdk._core.Resource` (a ``dict`` with ``.etag``), list endpoints are
:class:`~olcf.sdk._core.Page` (a ``list`` with ``.next_cursor``), downloads are ``bytes`` and ``204`` answers are ``None``.
``tests/tooling/test_sdk_coverage.py`` checks that every operation of the generated OpenAPI document is bound here (or explicitly
excluded), so the SDK cannot silently fall behind the API.
"""

from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from typing import Any, Literal

JSON = Any
Body = Mapping[str, Any]
ItemTypeName = Literal["notebook", "lakehouse", "sql_query", "pipeline", "environment", "spark_job_definition", "variable_set", "schedule", "report", "extension"]
JobKindName = Literal["notebook", "spark_job", "sql", "capsule", "script"]


class _Res:
    """Base of all resource groups."""

    def __init__(self, client: Any) -> None:
        self._c = client

    def _call(self, method: str, template: str, **kw: Any) -> Any:
        return self._c.call(method, template, **kw)


class Identity(_Res):
    """Login, current principal, users, role bindings and personal access tokens."""

    def login(self, username: str, password: str) -> JSON:
        """Raw ``POST /identity/login``; use :meth:`Client.login` to also store the tokens on the client."""
        return self._call("POST", "/identity/login", json={"username": username, "password": password}, auth=False)

    def refresh(self, refresh_token: str) -> JSON:
        return self._call("POST", "/identity/refresh", json={"refresh_token": refresh_token}, auth=False)

    def logout(self, refresh_token: str | None = None) -> None:
        """End the login session: the refresh token (when given) and - because the current access token is sent along - every access token of the session."""
        return self._call("POST", "/identity/logout", json={"refresh_token": refresh_token} if refresh_token else {})

    def me(self) -> JSON:
        """The authenticated principal, its role bindings, token scopes and organisation-level permissions."""
        return self._call("GET", "/identity/me")

    def change_password(self, new_password: str, current_password: str | None = None) -> None:
        return self._call("POST", "/identity/password", json={"current_password": current_password, "new_password": new_password})

    def list_users(self, *, kind: str | None = None, q: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/identity/users", params={"kind": kind, "q": q, "limit": limit, "cursor": cursor})

    def get_user(self, user_id: str) -> JSON:
        return self._call("GET", "/identity/users/{id}", path={"id": user_id})

    def create_user(self, name: str, *, kind: str = "user", password: str | None = None, display_name: str | None = None,
                    email: str | None = None, roles: Sequence[Body] | None = None, must_change_password: bool = False) -> JSON:
        """Create a user or service principal. ``must_change_password`` forces the person to choose a new password at first sign-in (their session can call only ``change_password``)."""
        return self._call("POST", "/identity/users", json={"name": name, "kind": kind, "password": password, "display_name": display_name,
                                                          "email": email, "roles": list(roles or []), "must_change_password": must_change_password})

    def update_user(self, user_id: str, **fields: Any) -> JSON:
        """``display_name``, ``email``, ``is_active``."""
        return self._call("PATCH", "/identity/users/{id}", path={"id": user_id}, json=fields)

    def reset_password(self, user_id: str) -> JSON:
        return self._call("POST", "/identity/users/{id}/reset-password", path={"id": user_id})

    def list_bindings(self, *, principal_id: str | None = None, scope_type: str | None = None, scope_id: str | None = None) -> JSON:
        return self._call("GET", "/identity/bindings", params={"principal_id": principal_id, "scope_type": scope_type, "scope_id": scope_id})

    def bind(self, principal_id: str, role: str, *, scope_type: str = "org", scope_id: str = "") -> JSON:
        return self._call("POST", "/identity/bindings", json={"principal_id": principal_id, "role": role, "scope_type": scope_type, "scope_id": scope_id})

    def unbind(self, binding_id: str) -> None:
        return self._call("DELETE", "/identity/bindings/{id}", path={"id": binding_id})

    def list_tokens(self, *, all: bool = False) -> JSON:  # noqa: A002
        return self._call("GET", "/identity/tokens", params={"all": all or None})

    def create_token(self, name: str, *, scopes: list[str] | None = None, ttl_days: int | None = None, principal_id: str | None = None) -> JSON:
        """The plaintext token is in the answer's ``token`` field and is shown exactly once."""
        return self._call("POST", "/identity/tokens", json={"name": name, "scopes": scopes or ["*"], "ttl_days": ttl_days, "principal_id": principal_id})

    def revoke_token(self, token_id: str) -> None:
        return self._call("DELETE", "/identity/tokens/{id}", path={"id": token_id})

    # ---- federation (OIDC / Entra / LDAP) and local login policy
    def list_providers(self) -> JSON:
        return self._call("GET", "/identity/providers")

    def create_provider(self, name: str, type: str, **fields: Any) -> JSON:  # noqa: A002
        """``type`` is ``oidc`` / ``entra`` / ``ldap``; ``client_secret`` is write-only."""
        return self._call("POST", "/identity/providers", json={"name": name, "type": type, **fields})

    def get_provider(self, provider_id: str) -> JSON:
        return self._call("GET", "/identity/providers/{id}", path={"id": provider_id})

    def update_provider(self, provider_id: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/identity/providers/{id}", path={"id": provider_id}, json=fields)

    def delete_provider(self, provider_id: str) -> None:
        return self._call("DELETE", "/identity/providers/{id}", path={"id": provider_id})

    def preview_mapping(self, provider_id: str, claims: Body) -> JSON:
        """Which roles would a user with these claims receive?"""
        return self._call("POST", "/identity/providers/{id}/preview-mapping", path={"id": provider_id}, json={"claims": dict(claims)})

    def test_provider(self, provider_id: str, *, client_credentials: bool = False, scope: str | None = None) -> JSON:
        return self._call("POST", "/identity/providers/{id}/test", path={"id": provider_id}, json={"client_credentials": client_credentials, "scope": scope})

    def oidc_providers(self) -> JSON:
        """Providers offered on the login page (public)."""
        return self._call("GET", "/identity/oidc/providers", auth=False)

    def oidc_exchange(self, code: str, ui_verifier: str | None = None) -> JSON:
        """Trade the one-time code from the OIDC redirect for OLCF tokens."""
        return self._call("POST", "/identity/oidc/exchange", json={"code": code, "ui_verifier": ui_verifier}, auth=False)

    def ldap_login(self, provider: str, username: str, password: str) -> JSON:
        return self._call("POST", "/identity/ldap/{provider}/login", path={"provider": provider}, json={"username": username, "password": password}, auth=False)

    def local_login_enabled(self) -> JSON:
        return self._call("GET", "/identity/local-login", auth=False)

    def set_local_login(self, enabled: bool) -> JSON:
        return self._call("PUT", "/identity/local-login", json={"enabled": enabled})

    def roles(self) -> JSON:
        return self._call("GET", "/roles")


class Admin(_Res):
    """Health, info, settings, audit log, events, search, operations."""

    def health(self) -> JSON:
        return self._call("GET", "/health", auth=False)

    def ready(self) -> JSON:
        return self._call("GET", "/health/ready", auth=False)

    def info(self) -> JSON:
        return self._call("GET", "/admin/info")

    def health_report(self, *, force: bool = False) -> JSON:
        return self._call("GET", "/admin/health", params={"force": force or None})

    def health_checks(self) -> JSON:
        return self._call("GET", "/admin/health/checks")

    def test_health_check(self, name: str) -> JSON:
        """Run the *deep* variant of one health check (powers every "Test connectivity" button)."""
        return self._call("POST", "/admin/health/checks/{name}/test", path={"name": name})

    def ports(self) -> str:
        return self._call("GET", "/admin/ports")

    def settings(self) -> JSON:
        return self._call("GET", "/admin/settings")

    def set_setting(self, key: str, value: Any) -> JSON:
        return self._call("PUT", "/admin/settings/{key}", path={"key": key}, json={"value": value})

    def audit(self, *, limit: int = 100, before_seq: int | None = None, action: str | None = None, actor: str | None = None,
              workspace_id: str | None = None, resource_id: str | None = None, since: str | None = None, until: str | None = None,
              outcome: str | None = None) -> JSON:
        return self._call("GET", "/audit", params={"limit": limit, "before_seq": before_seq, "action": action, "actor": actor, "workspace_id": workspace_id,
                                                    "resource_id": resource_id, "since": since, "until": until, "outcome": outcome})

    def export_audit(self, *, format: str = "jsonl") -> bytes:  # noqa: A002
        return self._call("GET", "/audit/export", params={"format": format}, as_bytes=True)

    def verify_audit(self) -> JSON:
        return self._call("GET", "/audit/verify")

    def events(self, *, after_seq: int = 0, limit: int = 100, type: str | None = None, subject: str | None = None, descending: bool = False) -> JSON:  # noqa: A002
        return self._call("GET", "/events", params={"after_seq": after_seq, "limit": limit, "type": type, "subject": subject, "descending": descending or None})

    def search(self, q: str, *, limit: int = 20) -> JSON:
        return self._call("GET", "/search", params={"q": q, "limit": limit})

    def operation(self, operation_id: str) -> JSON:
        return self._call("GET", "/operations/{oid}", path={"oid": operation_id})

    def metrics_text(self) -> str:
        """Prometheus exposition (needs ``platform.admin``)."""
        return self._call("GET", "/metrics")

    def metric_names(self) -> JSON:
        return self._call("GET", "/metrics/names")

    def metric_series(self, **params: Any) -> JSON:
        return self._call("GET", "/metrics/series", params=params)

    def metrics_snapshot(self) -> JSON:
        return self._call("GET", "/metrics/snapshot")

    def test_otlp(self) -> JSON:
        return self._call("POST", "/metrics/otlp/test")


class Workspaces(_Res):
    """Workspaces, membership, stage, Git integration, export/import."""

    def list(self, *, q: str | None = None, deleted: bool = False, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/workspaces", params={"q": q, "deleted": deleted or None, "limit": limit, "cursor": cursor})

    def get(self, ws: str) -> JSON:
        """``ws`` is an id (``wks_...``) or a unique name."""
        return self._call("GET", "/workspaces/{ws}", path={"ws": ws})

    def create(self, name: str, *, display_name: str | None = None, description: str = "", tags: builtins.list[str] | None = None,
               settings: Body | None = None, quota: Body | None = None) -> JSON:
        return self._call("POST", "/workspaces", json={"name": name, "display_name": display_name, "description": description, "tags": tags or [],
                                                      "settings": dict(settings or {}), "quota": dict(quota or {})}, idem=True)

    def update(self, ws: str, if_match: Any = None, **fields: Any) -> JSON:
        return self._call("PATCH", "/workspaces/{ws}", path={"ws": ws}, json=fields, if_match=if_match)

    def delete(self, ws: str) -> None:
        return self._call("DELETE", "/workspaces/{ws}", path={"ws": ws})

    def restore(self, ws: str) -> JSON:
        return self._call("POST", "/workspaces/{ws}/restore", path={"ws": ws})

    def members(self, ws: str) -> JSON:
        return self._call("GET", "/workspaces/{ws}/members", path={"ws": ws})

    def export_zip(self, ws: str) -> bytes:
        """The workspace as a zip (items, notebooks, connections metadata); secrets are never included."""
        return self._call("GET", "/workspaces/{ws}/export", path={"ws": ws}, as_bytes=True)

    def import_zip(self, data: bytes, *, filename: str = "workspace.zip", target_workspace: str | None = None, new_name: str | None = None,
                   on_conflict: str | None = None) -> JSON:
        return self._call("POST", "/workspaces/import", params={"target_workspace": target_workspace, "new_name": new_name, "on_conflict": on_conflict},
                          files={"file": (filename, data, "application/zip")})

    def get_stage(self, ws: str) -> JSON:
        return self._call("GET", "/workspaces/{ws}/stage", path={"ws": ws})

    def set_stage(self, ws: str, stage: str | None) -> JSON:
        return self._call("PUT", "/workspaces/{ws}/stage", path={"ws": ws}, json={"stage": stage})

    # ---- Git integration
    def git_status(self, ws: str, *, fetch: bool = False) -> JSON:
        return self._call("GET", "/workspaces/{ws}/git/status", path={"ws": ws}, params={"fetch": fetch or None})

    def git_connect(self, ws: str, *, repo_url: str | None = None, branch: str = "main", folder: str = "", connection: str | None = None,
                    notebook_format: str = "ipynb", create_branch: bool = False) -> JSON:
        return self._call("POST", "/workspaces/{ws}/git/connect", path={"ws": ws}, json={
            "repo_url": repo_url, "branch": branch, "folder": folder, "connection": connection, "notebook_format": notebook_format, "create_branch": create_branch})

    def git_disconnect(self, ws: str) -> JSON:
        return self._call("POST", "/workspaces/{ws}/git/disconnect", path={"ws": ws})

    def git_branches(self, ws: str) -> JSON:
        return self._call("GET", "/workspaces/{ws}/git/branches", path={"ws": ws})

    def git_create_branch(self, ws: str, name: str, *, source: str | None = None, switch: bool = False) -> JSON:
        return self._call("POST", "/workspaces/{ws}/git/branches", path={"ws": ws}, json={"name": name, "source": source, "switch": switch})

    def git_switch_branch(self, ws: str, name: str) -> JSON:
        return self._call("PUT", "/workspaces/{ws}/git/branch", path={"ws": ws}, json={"name": name})

    def git_commit(self, ws: str, message: str, *, items: builtins.list[str] | None = None) -> JSON:
        return self._call("POST", "/workspaces/{ws}/git/commit", path={"ws": ws}, json={"message": message, "items": items})

    def git_capabilities(self, ws: str) -> JSON:
        """Is a usable ``git`` available on the control plane (the sync features need it)?"""
        return self._call("GET", "/workspaces/{ws}/git/capabilities", path={"ws": ws})

    def git_pull(self, ws: str, *, resolution: str | None = None, resolutions: Body | None = None, dry_run: bool = False) -> JSON:
        return self._call("POST", "/workspaces/{ws}/git/pull", path={"ws": ws}, json={"resolution": resolution, "resolutions": dict(resolutions or {}), "dry_run": dry_run})


class Items(_Res):
    """Generic workspace items (notebooks, SQL queries, pipelines, environments, reports, ...)."""

    def list(self, ws: str, *, type: ItemTypeName | None = None, folder: str | None = None, q: str | None = None, tag: str | None = None,  # noqa: A002
             deleted: bool = False, with_definition: bool = False, include: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        """``include="definition"`` (or ``with_definition=True``) embeds each definition; the server caps the total size (items past the cap carry ``definition_omitted``)."""
        return self._call("GET", "/workspaces/{ws}/items", path={"ws": ws}, params={
            "type": type, "folder": folder, "q": q, "tag": tag, "deleted": deleted or None, "with_definition": with_definition or None, "include": include, "limit": limit,
            "cursor": cursor})

    def create(self, ws: str, type: ItemTypeName, name: str, *, definition: Body | None = None, description: str = "", folder: str = "",  # noqa: A002
               tags: builtins.list[str] | None = None, sensitivity: str | None = None) -> JSON:
        return self._call("POST", "/workspaces/{ws}/items", path={"ws": ws}, json={
            "type": type, "name": name, "description": description, "folder": folder, "tags": tags or [], "sensitivity": sensitivity,
            "definition": dict(definition or {})}, idem=True)

    def get(self, item_id: str) -> JSON:
        return self._call("GET", "/items/{id}", path={"id": item_id})

    def update(self, item_id: str, if_match: Any = None, **fields: Any) -> JSON:
        """``name, description, folder, tags, sensitivity, definition, comment``.  Pass the resource you read as ``if_match`` for optimistic concurrency."""
        return self._call("PUT", "/items/{id}", path={"id": item_id}, json=fields, if_match=if_match)

    def delete(self, item_id: str) -> None:
        return self._call("DELETE", "/items/{id}", path={"id": item_id})

    def restore(self, item_id: str) -> JSON:
        return self._call("POST", "/items/{id}/restore", path={"id": item_id})

    def versions(self, item_id: str, *, with_definition: bool = False) -> JSON:
        return self._call("GET", "/items/{id}/versions", path={"id": item_id}, params={"with_definition": with_definition or None})

    def version(self, item_id: str, version: int) -> JSON:
        return self._call("GET", "/items/{id}/versions/{version}", path={"id": item_id, "version": version})

    def revert(self, item_id: str, version: int) -> JSON:
        return self._call("POST", "/items/{id}/versions/{version}/revert", path={"id": item_id, "version": version})


class Notebooks(_Res):
    """Notebook content, parameters, import/export, runs and interactive sessions."""

    def content(self, item_id: str) -> JSON:
        return self._call("GET", "/notebooks/{id}/content", path={"id": item_id})

    def put_content(self, item_id: str, content: Body, if_match: Any = None) -> JSON:
        return self._call("PUT", "/notebooks/{id}/content", path={"id": item_id}, json=content, if_match=if_match)

    def export(self, item_id: str, *, format: str | None = None) -> bytes:  # noqa: A002
        """Notebook source as bytes (UTF-8 ``ipynb`` JSON by default, ``py`` / Fabric git source with ``format=``)."""
        return self._call("GET", "/notebooks/{id}/export", path={"id": item_id}, params={"format": format}, as_bytes=True)

    def import_files(self, ws: str, files: list[tuple[str, bytes]], *, on_conflict: str | None = None, format: str | None = None,  # noqa: A002
                     folder: str | None = None, strip_outputs: bool = False) -> JSON:
        """Upload ``.ipynb`` / ``.py`` / Fabric git-source notebooks as ``[(filename, bytes), ...]``."""
        return self._call("POST", "/workspaces/{ws}/notebooks/import", path={"ws": ws},
                          params={"on_conflict": on_conflict, "format": format, "folder": folder, "strip_outputs": strip_outputs or None},
                          files=[("files", (n, b, "application/octet-stream")) for n, b in files])

    def parameters(self, item_id: str) -> JSON:
        return self._call("GET", "/notebooks/{id}/parameters", path={"id": item_id})

    def preview_parameters(self, item_id: str, parameters: Body) -> JSON:
        return self._call("POST", "/notebooks/{id}/parameters/preview", path={"id": item_id}, json={"parameters": dict(parameters)})

    def pip_requirements(self, item_id: str) -> JSON:
        return self._call("GET", "/notebooks/{id}/pip", path={"id": item_id})

    def diff(self, item_id: str, **params: Any) -> JSON:
        return self._call("GET", "/notebooks/{id}/diff", path={"id": item_id}, params=params)

    def versions(self, item_id: str) -> JSON:
        return self._call("GET", "/notebooks/{id}/versions", path={"id": item_id})

    def version(self, item_id: str, version: int) -> JSON:
        return self._call("GET", "/notebooks/{id}/versions/{version}", path={"id": item_id, "version": version})

    def restore_version(self, item_id: str, version: int) -> JSON:
        return self._call("POST", "/notebooks/{id}/versions/{version}/restore", path={"id": item_id, "version": version})

    def run(self, item_id: str, *, parameters: Body | None = None, name: str | None = None, engine: str | None = None, runtime: Body | None = None,
            resources: Body | None = None, policy: Body | None = None, default_lakehouse: Body | None = None, environment_id: str | None = None,
            variable_set: str | None = None, stage: str | None = None, tags: Body | None = None, idempotency_key: str | None = None) -> JSON:
        """Submit the notebook as a job (``202``); returns the job record.  Pair with ``client.jobs.wait``."""
        body = {"parameters": dict(parameters or {}), "name": name, "engine": engine, "runtime": runtime, "resources": resources, "policy": policy,
                "default_lakehouse": default_lakehouse, "environment_id": environment_id, "variable_set": variable_set, "stage": stage,
                "tags": dict(tags or {}), "idempotency_key": idempotency_key}
        return self._call("POST", "/notebooks/{id}/run", path={"id": item_id}, json=body, idem=True, idempotency_key=idempotency_key)

    def runs(self, item_id: str, *, state: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/notebooks/{id}/runs", path={"id": item_id}, params={"state": state, "limit": limit, "cursor": cursor})

    def create_session(self, item_id: str, **body: Any) -> JSON:
        return self._call("POST", "/notebooks/{id}/session", path={"id": item_id}, json=body)


class Lakehouses(_Res):
    """Lakehouses, their files and Delta/Iceberg tables."""

    def list(self, *, workspace_id: str | None = None, q: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/lakehouses", params={"workspace_id": workspace_id, "q": q, "limit": limit, "cursor": cursor})

    def list_in_workspace(self, ws: str, *, q: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/workspaces/{ws}/lakehouses", path={"ws": ws}, params={"q": q, "limit": limit, "cursor": cursor})

    def create(self, ws: str, name: str, *, description: str = "", connection_id: str | None = None, root_uri: str | None = None,
               default_format: str = "delta", schema_enabled: bool = False, catalog: str = "default", folder: str = "",
               tags: builtins.list[str] | None = None, fabric: Body | None = None) -> JSON:
        return self._call("POST", "/workspaces/{ws}/lakehouses", path={"ws": ws}, json={
            "name": name, "description": description, "connection_id": connection_id, "root_uri": root_uri, "default_format": default_format,
            "schema_enabled": schema_enabled, "catalog": catalog, "folder": folder, "tags": tags or [], "fabric": fabric}, idem=True)

    def get(self, lh: str) -> JSON:
        return self._call("GET", "/lakehouses/{lh}", path={"lh": lh})

    def delete(self, lh: str) -> None:
        return self._call("DELETE", "/lakehouses/{lh}", path={"lh": lh})

    def test(self, lh: str) -> JSON:
        return self._call("POST", "/lakehouses/{lh}/test", path={"lh": lh})

    def refresh(self, lh: str) -> JSON:
        return self._call("POST", "/lakehouses/{lh}/refresh", path={"lh": lh})

    # ---- files
    def list_files(self, lh: str, path: str = "", *, recursive: bool = False, limit: int = 200, cursor: str | None = None) -> JSON:
        return self._call("GET", "/lakehouses/{lh}/files", path={"lh": lh}, params={"path": path, "recursive": recursive or None, "limit": limit, "cursor": cursor})

    def stat_file(self, lh: str, path: str) -> JSON:
        return self._call("GET", "/lakehouses/{lh}/files/stat", path={"lh": lh}, params={"path": path})

    def read_file(self, lh: str, path: str) -> bytes:
        return self._call("GET", "/lakehouses/{lh}/files/content", path={"lh": lh}, params={"path": path}, as_bytes=True)

    def write_file(self, lh: str, path: str, data: bytes, *, overwrite: bool = False) -> JSON:
        return self._call("PUT", "/lakehouses/{lh}/files/content", path={"lh": lh}, params={"path": path, "overwrite": overwrite},
                          content=data, headers={"Content-Type": "application/octet-stream"})

    def upload_file(self, lh: str, path: str, filename: str, data: bytes, *, overwrite: bool = False) -> JSON:
        """Multipart upload into the directory ``path``."""
        return self._call("POST", "/lakehouses/{lh}/files/upload", path={"lh": lh}, params={"path": path, "overwrite": overwrite},
                          files={"file": (filename, data, "application/octet-stream")})

    def delete_file(self, lh: str, path: str, *, recursive: bool = False) -> JSON:
        return self._call("DELETE", "/lakehouses/{lh}/files", path={"lh": lh}, params={"path": path, "recursive": recursive or None})

    def mkdir(self, lh: str, path: str) -> JSON:
        return self._call("POST", "/lakehouses/{lh}/files/mkdir", path={"lh": lh}, json={"path": path})

    def move_file(self, lh: str, src: str, dst: str, *, overwrite: bool = False) -> JSON:
        return self._call("POST", "/lakehouses/{lh}/files/move", path={"lh": lh}, json={"src": src, "dst": dst, "overwrite": overwrite})

    def copy_file(self, lh: str, src: str, dst: str, *, overwrite: bool = False) -> JSON:
        return self._call("POST", "/lakehouses/{lh}/files/copy", path={"lh": lh}, json={"src": src, "dst": dst, "overwrite": overwrite})

    def preview_file(self, lh: str, path: str, *, rows: int | None = None, bytes: int | None = None) -> JSON:  # noqa: A002
        return self._call("GET", "/lakehouses/{lh}/files/preview", path={"lh": lh}, params={"path": path, "rows": rows, "bytes": bytes})

    # ---- tables
    def list_tables(self, lh: str) -> JSON:
        return self._call("GET", "/lakehouses/{lh}/tables", path={"lh": lh})

    def create_table(self, lh: str, name: str, source: str, *, format: str | None = None, mode: str = "overwrite",  # noqa: A002
                     partition_by: builtins.list[str] | None = None, options: Body | None = None) -> JSON:
        """Register a table from a file below ``Files/`` (CSV, JSON, Parquet)."""
        return self._call("POST", "/lakehouses/{lh}/tables", path={"lh": lh}, json={
            "name": name, "source": source, "format": format, "mode": mode, "partition_by": partition_by or [], "options": dict(options or {})}, idem=True)

    def table(self, lh: str, name: str) -> JSON:
        return self._call("GET", "/lakehouses/{lh}/tables/{name}", path={"lh": lh, "name": name})

    def drop_table(self, lh: str, name: str) -> None:
        return self._call("DELETE", "/lakehouses/{lh}/tables/{name}", path={"lh": lh, "name": name})

    def table_history(self, lh: str, name: str, *, limit: int | None = None) -> JSON:
        return self._call("GET", "/lakehouses/{lh}/tables/{name}/history", path={"lh": lh, "name": name}, params={"limit": limit})

    def table_preview(self, lh: str, name: str, *, rows: int | None = None, version: int | None = None, as_of: str | None = None,
                      snapshot_id: str | None = None, columns: builtins.list[str] | str | None = None) -> JSON:
        cols = ",".join(columns) if isinstance(columns, list) else columns
        return self._call("GET", "/lakehouses/{lh}/tables/{name}/preview", path={"lh": lh, "name": name},
                          params={"rows": rows, "version": version, "as_of": as_of, "snapshot_id": snapshot_id, "columns": cols})

    def table_protocol(self, lh: str, name: str) -> JSON:
        return self._call("GET", "/lakehouses/{lh}/tables/{name}/protocol", path={"lh": lh, "name": name})

    def optimize_table(self, lh: str, name: str, *, z_order_by: builtins.list[str] | None = None, partition_filters: builtins.list[Any] | None = None,
                       target_size: int | None = None) -> JSON:
        return self._call("POST", "/lakehouses/{lh}/tables/{name}/optimize", path={"lh": lh, "name": name},
                          json={"z_order_by": z_order_by or [], "partition_filters": partition_filters, "target_size": target_size})

    def vacuum_table(self, lh: str, name: str, *, retention_hours: int | None = None, dry_run: bool = True) -> JSON:
        """Defaults to a dry run; the platform refuses retention below 168 h unless an administrator allows it."""
        return self._call("POST", "/lakehouses/{lh}/tables/{name}/vacuum", path={"lh": lh, "name": name}, json={"retention_hours": retention_hours, "dry_run": dry_run})

    def storage_providers(self) -> JSON:
        return self._call("GET", "/storage/providers")


class Sql(_Res):
    """SQL execution (DuckDB and Spark engines), history, saved queries."""

    def engines(self) -> JSON:
        return self._call("GET", "/sql/engines")

    def execute(self, sql: str, *, engine: str = "duckdb", workspace_id: str | None = None, lakehouse: str | None = None,
                parameters: Body | None = None, max_rows: int | None = None, timeout_seconds: int | None = None, query_id: str | None = None) -> JSON:
        """Result: ``{columns, rows, row_count, truncated, elapsed_ms, ...}``."""
        return self._call("POST", "/sql/execute", json={"sql": sql, "engine": engine, "workspace_id": workspace_id, "lakehouse": lakehouse,
                                                       "parameters": dict(parameters or {}), "max_rows": max_rows, "timeout_seconds": timeout_seconds, "query_id": query_id})

    def explain(self, sql: str, *, engine: str = "duckdb", workspace_id: str | None = None, lakehouse: str | None = None, parameters: Body | None = None) -> JSON:
        return self._call("POST", "/sql/explain", json={"sql": sql, "engine": engine, "workspace_id": workspace_id, "lakehouse": lakehouse, "parameters": dict(parameters or {})})

    def export(self, sql: str, *, format: str = "csv", engine: str = "duckdb", workspace_id: str | None = None, lakehouse: str | None = None,  # noqa: A002
               parameters: Body | None = None, max_rows: int | None = None, filename: str | None = None) -> bytes:
        return self._call("POST", "/sql/export", json={"sql": sql, "format": format, "engine": engine, "workspace_id": workspace_id, "lakehouse": lakehouse,
                                                      "parameters": dict(parameters or {}), "max_rows": max_rows, "filename": filename}, as_bytes=True)

    def cancel(self, query_id: str) -> JSON:
        return self._call("POST", "/sql/cancel/{qid}", path={"qid": query_id})

    def running(self) -> JSON:
        return self._call("GET", "/sql/running")

    def history(self, *, workspace_id: str | None = None, mine: bool | None = None, status: str | None = None, source: str | None = None,
                limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/sql/history", params={"workspace_id": workspace_id, "mine": mine, "status": status, "source": source, "limit": limit, "cursor": cursor})

    def history_entry(self, query_id: str) -> JSON:
        return self._call("GET", "/sql/history/{qid}", path={"qid": query_id})

    def schema(self, *, workspace_id: str | None = None, lakehouse: str | None = None) -> JSON:
        return self._call("GET", "/sql/schema", params={"workspace_id": workspace_id, "lakehouse": lakehouse})

    def run_saved(self, item_id: str, *, parameters: Body | None = None, max_rows: int | None = None, timeout_seconds: int | None = None) -> JSON:
        return self._call("POST", "/sql/queries/{id}/run", path={"id": item_id}, json={"parameters": dict(parameters or {}), "max_rows": max_rows, "timeout_seconds": timeout_seconds})


class Catalog(_Res):
    """Logical catalog, search, lineage and reports."""

    def catalogs(self) -> JSON:
        return self._call("GET", "/catalogs")

    def tables(self, *, workspace_id: str | None = None, lakehouse_id: str | None = None, q: str | None = None, format: str | None = None,  # noqa: A002
               tag: str | None = None, limit: int = 100, cursor: str | None = None) -> JSON:
        return self._call("GET", "/catalog/tables", params={"workspace_id": workspace_id, "lakehouse_id": lakehouse_id, "q": q, "format": format, "tag": tag,
                                                             "limit": limit, "cursor": cursor})

    def table(self, entry_id: str) -> JSON:
        return self._call("GET", "/catalog/tables/{id}", path={"id": entry_id})

    def update_table(self, entry_id: str, **fields: Any) -> JSON:
        return self._call("PATCH", "/catalog/tables/{id}", path={"id": entry_id}, json=fields)

    def table_stats(self, entry_id: str, *, compute: bool = False) -> JSON:
        return self._call("GET", "/catalog/tables/{id}/stats", path={"id": entry_id}, params={"compute": compute or None})

    def refresh_table_stats(self, entry_id: str) -> JSON:
        return self._call("POST", "/catalog/tables/{id}/stats/refresh", path={"id": entry_id})

    def sync(self, workspace_id: str) -> JSON:
        return self._call("POST", "/catalog/sync", params={"workspace_id": workspace_id})

    def search(self, q: str, *, kind: str | None = None, workspace_id: str | None = None, limit: int = 25) -> JSON:
        return self._call("GET", "/catalog/search", params={"q": q, "kind": kind, "workspace_id": workspace_id, "limit": limit})

    def lineage(self, dataset: str, *, direction: str | None = None, depth: int | None = None, column: str | None = None) -> JSON:
        return self._call("GET", "/catalog/lineage", params={"dataset": dataset, "direction": direction, "depth": depth, "column": column})

    def record_lineage(self, workspace_id: str, sources: list[Body], targets: list[Body], **extra: Any) -> JSON:
        return self._call("POST", "/catalog/lineage", json={"workspace_id": workspace_id, "sources": sources, "targets": targets, **extra})

    def parse_lineage(self, workspace_id: str, sql: str, *, dialect: str = "spark", default_lakehouse: str | None = None, record: bool = False) -> JSON:
        return self._call("POST", "/catalog/lineage/parse", json={"workspace_id": workspace_id, "sql": sql, "dialect": dialect,
                                                                  "default_lakehouse": default_lakehouse, "record": record})

    def reports(self, *, workspace_id: str | None = None) -> JSON:
        return self._call("GET", "/reports", params={"workspace_id": workspace_id})

    def run_report(self, report_id: str, *, filters: Body | None = None, refresh: bool = False) -> JSON:
        return self._call("POST", "/reports/{id}/run", path={"id": report_id}, json={"filters": dict(filters or {}), "refresh": refresh})


class Connections(_Res):
    """Connections (storage accounts, databases, Git, ...) and secrets."""

    def list(self, *, workspace_id: str | None = None, type: str | None = None, limit: int = 100, cursor: str | None = None) -> JSON:  # noqa: A002
        return self._call("GET", "/connections", params={"workspace_id": workspace_id, "type": type, "limit": limit, "cursor": cursor})

    def create(self, name: str, type: str, *, workspace_id: str | None = None, description: str = "", config: Body | None = None,  # noqa: A002
               secret_id: str | None = None, shared: bool = False, tags: builtins.list[str] | None = None) -> JSON:
        return self._call("POST", "/connections", json={"workspace_id": workspace_id, "name": name, "type": type, "description": description,
                                                       "config": dict(config or {}), "secret_id": secret_id, "shared": shared, "tags": tags or []}, idem=True)

    def get(self, cid: str) -> JSON:
        return self._call("GET", "/connections/{cid}", path={"cid": cid})

    def update(self, cid: str, if_match: Any = None, **fields: Any) -> JSON:
        return self._call("PATCH", "/connections/{cid}", path={"cid": cid}, json=fields, if_match=if_match)

    def delete(self, cid: str) -> None:
        return self._call("DELETE", "/connections/{cid}", path={"cid": cid})

    def test(self, cid: str) -> JSON:
        return self._call("POST", "/connections/{cid}/test", path={"cid": cid})

    # secrets
    def list_secrets(self, *, scope_type: str | None = None, scope_id: str | None = None) -> JSON:
        return self._call("GET", "/secrets", params={"scope_type": scope_type, "scope_id": scope_id})

    def create_secret(self, name: str, value: str | Body, *, provider: str | None = None, scope_type: str = "org", scope_id: str = "", meta: Body | None = None) -> JSON:
        """The value is write-only: no endpoint ever returns it."""
        return self._call("POST", "/secrets", json={"name": name, "value": value, "provider": provider, "scope_type": scope_type, "scope_id": scope_id, "meta": dict(meta or {})})

    def rotate_secret(self, secret_id: str, value: str | Body) -> JSON:
        return self._call("POST", "/secrets/{sid}/rotate", path={"sid": secret_id}, json={"value": value})

    def delete_secret(self, secret_id: str) -> None:
        return self._call("DELETE", "/secrets/{sid}", path={"sid": secret_id})


class Jobs(_Res):
    """Job submission, state, logs, output and artifacts (loops such as ``wait`` are added by the concrete clients)."""

    def submit(self, submission: Body | None = None, *, kind: JobKindName | None = None, workspace_id: str | None = None, idempotency_key: str | None = None,
               **fields: Any) -> JSON:
        """``POST /jobs``.  Either pass a full ``submission`` dict or ``kind=`` / ``workspace_id=`` plus any ``JobSubmission`` field
        (``name``, ``item_id``, ``source``, ``parameters``, ``runtime``, ``resources``, ``policy``, ``engine``, ``env``, ``secret_refs``, ...)."""
        body = dict(submission or {})
        body.update({k: v for k, v in (("kind", kind), ("workspace_id", workspace_id)) if v is not None})
        body.update(fields)
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        return self._call("POST", "/jobs", json=body, idem=True, idempotency_key=idempotency_key)

    def list(self, *, workspace_id: str | None = None, state: str | None = None, kind: str | None = None, item_id: str | None = None,
             origin: str | None = None, queue: str | None = None, since: str | None = None, until: str | None = None, text: str | None = None,
             parent_run_id: str | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/jobs", params={"workspace_id": workspace_id, "state": state, "kind": kind, "item_id": item_id, "origin": origin, "queue": queue,
                                                   "since": since, "until": until, "text": text, "parent_run_id": parent_run_id, "limit": limit, "cursor": cursor})

    def stats(self) -> JSON:
        return self._call("GET", "/jobs/stats")

    def get(self, job_id: str) -> JSON:
        return self._call("GET", "/jobs/{id}", path={"id": job_id})

    def cancel(self, job_id: str) -> JSON:
        return self._call("POST", "/jobs/{id}/cancel", path={"id": job_id})

    def retry(self, job_id: str) -> JSON:
        return self._call("POST", "/jobs/{id}/retry", path={"id": job_id}, idem=True)

    def timeline(self, job_id: str, *, limit: int | None = None) -> JSON:
        return self._call("GET", "/jobs/{id}/timeline", path={"id": job_id}, params={"limit": limit})

    def logs_page(self, job_id: str, *, offset: int = 0, limit: int = 65536, stream: str = "combined", tail: bool = False) -> JSON:
        """One page of the log (``offset``/``next_offset`` are byte offsets; ``eof`` is true when the job ended and everything was read)."""
        return self._call("GET", "/jobs/{id}/logs", path={"id": job_id}, params={"offset": offset, "limit": limit, "stream": stream, "tail": tail or None})

    def output(self, job_id: str, *, name: str | None = None, offset: int = 0, limit: int | None = None) -> bytes:
        """The executed notebook / primary result as bytes."""
        return self._call("GET", "/jobs/{id}/output", path={"id": job_id}, params={"name": name, "offset": offset or None, "limit": limit}, as_bytes=True)

    def artifacts(self, job_id: str) -> JSON:
        return self._call("GET", "/jobs/{id}/artifacts", path={"id": job_id})

    def artifact(self, job_id: str, name: str, *, offset: int | None = None, limit: int | None = None) -> bytes:
        return self._call("GET", "/jobs/{id}/artifacts/{name}", path={"id": job_id, "name": name}, params={"offset": offset, "limit": limit}, as_bytes=True)

    def upload_artifact(self, job_id: str, name: str, data: bytes) -> JSON:
        return self._call("POST", "/jobs/{id}/artifacts/{name}", path={"id": job_id, "name": name}, content=data, headers={"Content-Type": "application/octet-stream"})

    # Spark applications
    def spark_applications(self, **params: Any) -> JSON:
        return self._call("GET", "/spark-applications", params=params)

    def spark_application(self, app_id: str) -> JSON:
        return self._call("GET", "/spark-applications/{app}", path={"app": app_id})

    # Spark job definitions
    def run_spark_job_definition(self, item_id: str, **body: Any) -> JSON:
        return self._call("POST", "/spark-job-definitions/{id}/run", path={"id": item_id}, json=body, idem=True)

    def spark_job_definition_runs(self, item_id: str, **params: Any) -> JSON:
        return self._call("GET", "/spark-job-definitions/{id}/runs", path={"id": item_id}, params=params)

    def validate_spark_job_definition(self, item_id: str) -> JSON:
        return self._call("GET", "/spark-job-definitions/{id}/validate", path={"id": item_id})


class Sessions(_Res):
    """Interactive notebook sessions (REST half; the WebSocket protocol is documented in CONTRACTS 4.4)."""

    def list(self, *, workspace_id: str | None = None, state: str | None = None, mine: bool | None = None, limit: int = 50, cursor: str | None = None) -> JSON:
        return self._call("GET", "/sessions", params={"workspace_id": workspace_id, "state": state, "mine": mine, "limit": limit, "cursor": cursor})

    def create(self, workspace_id: str, *, notebook_id: str | None = None, engine: str = "spark", **fields: Any) -> JSON:
        return self._call("POST", "/sessions", json={"workspace_id": workspace_id, "notebook_id": notebook_id, "engine": engine, **fields}, idem=True)

    def get(self, session_id: str) -> JSON:
        return self._call("GET", "/sessions/{id}", path={"id": session_id})

    def delete(self, session_id: str) -> JSON:
        return self._call("DELETE", "/sessions/{id}", path={"id": session_id})

    def execute(self, session_id: str, code: str, *, cell_id: str = "c1") -> JSON:
        return self._call("POST", "/sessions/{id}/execute", path={"id": session_id}, json={"cell_id": cell_id, "code": code})

    def complete(self, session_id: str, code: str, cursor: int | None = None) -> JSON:
        return self._call("POST", "/sessions/{id}/complete", path={"id": session_id}, json={"code": code, "cursor": cursor})

    def events(self, session_id: str, *, after_seq: int = 0, limit: int = 200, wait: float | None = None, replay: bool | None = None) -> JSON:
        return self._call("GET", "/sessions/{id}/events", path={"id": session_id}, params={"after_seq": after_seq, "limit": limit, "wait": wait, "replay": replay})

    def interrupt(self, session_id: str) -> JSON:
        return self._call("POST", "/sessions/{id}/interrupt", path={"id": session_id})

    def restart(self, session_id: str) -> JSON:
        return self._call("POST", "/sessions/{id}/restart", path={"id": session_id})
