# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Role-based access control (spec section 22).

*Roles* are composable bundles of *permissions*. A *binding* grants a role to a principal at a
*scope* (org, workspace, item, connection, runtime, pool). Authorization is enforced
server-side on every API call through :meth:`Principal.require`; UI visibility is never a
security boundary. Tokens can further *narrow* a principal with ``scopes``.
"""

from __future__ import annotations

import fnmatch
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select

from olcf.core.errors import Forbidden
from olcf.core.models import Role, ScopeType
from olcf.db import schema as S
from olcf.db.engine import Database
from olcf.db.util import fetch_all

# --------------------------------------------------------------------------- permission catalog
PERMISSIONS: dict[str, str] = {
    "platform.admin": "Full platform administration",
    "identity.read": "List users, roles and bindings",
    "identity.manage": "Create/modify users, role bindings and other principals' tokens",
    "identity.federation.manage": "Configure OIDC/Entra identity providers",
    "audit.read": "Read and export the audit log",
    "secret.manage": "Create, rotate and delete secrets",
    "secret.use": "Reference secrets from connections/jobs",
    "runtime.read": "View installed runtimes and the compatibility matrix",
    "runtime.manage": "Install, verify, repair, retire and roll back runtimes",
    "compute.read": "View workers, pools, queues and providers",
    "compute.manage": "Enroll/drain/cordon workers; manage pools, queues, providers",
    "workspace.create": "Create workspaces",
    "workspace.read": "View a workspace",
    "workspace.update": "Change workspace settings",
    "workspace.delete": "Delete / restore workspaces",
    "workspace.members.manage": "Manage workspace role bindings",
    "workspace.export": "Export workspace definitions",
    "workspace.import": "Import workspace definitions",
    "item.read": "View items",
    "item.create": "Create items",
    "item.update": "Modify items",
    "item.delete": "Delete / restore items",
    "connection.read": "View connection metadata",
    "connection.manage": "Create / modify / delete connections",
    "connection.use": "Use a connection from items and jobs",
    "lakehouse.read": "Browse lakehouse files and tables",
    "lakehouse.write": "Write lakehouse files and tables",
    "lakehouse.maintain": "Optimize / compact tables",
    "lakehouse.destructive": "Vacuum / drop / expire snapshots / delete data",
    "sql.execute": "Run read-only SQL",
    "sql.write": "Run SQL that modifies data",
    "job.read": "View jobs and logs",
    "job.submit": "Submit jobs",
    "job.cancel": "Cancel jobs",
    "job.retry": "Retry jobs",
    "session.use": "Open and use interactive notebook sessions",
    "pipeline.run": "Run pipelines",
    "webhook.manage": "Manage webhooks and event subscriptions",
    "alert.manage": "Manage alert rules",
    "events.read": "Read the event stream",
    "integration.fabric.read": "View Microsoft Fabric integration status",
    "integration.fabric.manage": "Configure Microsoft Fabric integration",
    "integration.fabric.submit": "Submit Fabric execution capsules",
    "mcp.use": "Use the AI-agent (MCP) endpoint",
    "intelligence.read": "Browse the asset graph: assets, lineage, impact, documentation, discovery status",
    "intelligence.manage": "Configure discovery sources, sampling policies and scans of the Intelligence module",
    "intelligence.sample": "Fetch policy-limited, masked sample rows of an asset on demand (audited)",
    "governance.curate": "Describe, tag, classify, assign owners and propose glossary terms, domains and data products",
    "governance.approve": "Approve governance change requests, certify assets and manage governance policies",
    "admin.health": "View service health and run connectivity tests",
    "admin.diagnostics": "Run diagnostics and create support bundles",
    "admin.backup": "Manage backups and restores",
    "admin.update": "Manage core/agent/runtime updates",
    "admin.settings": "Change platform settings",
    "admin.keys": "View key rings and rotate the master key and the JWT signing key",
}

_READ_ITEM = ["workspace.read", "item.read", "connection.read", "lakehouse.read", "sql.execute", "job.read", "events.read", "runtime.read", "intelligence.read"]
_CONTRIB = _READ_ITEM + [
    "item.create", "item.update", "item.delete", "connection.use", "lakehouse.write", "lakehouse.maintain",
    "job.submit", "job.cancel", "job.retry", "session.use", "pipeline.run", "mcp.use", "secret.use", "sql.write", "governance.curate",
]

ROLE_PERMISSIONS: dict[str, set[str]] = {
    Role.PLATFORM_ADMIN: {"*"},
    Role.SECURITY_ADMIN: {
        "identity.read", "identity.manage", "identity.federation.manage", "audit.read", "secret.manage", "secret.use",
        "integration.fabric.read", "admin.health", "admin.keys", "workspace.read", "workspace.members.manage", "events.read",
        "intelligence.read", "intelligence.manage", "governance.approve",
    },
    Role.RUNTIME_ADMIN: {
        "runtime.read", "runtime.manage", "compute.read", "compute.manage", "admin.health", "admin.diagnostics",
        "admin.update", "job.read", "events.read", "workspace.read",
    },
    Role.WORKSPACE_ADMIN: set(_CONTRIB) | {
        "workspace.update", "workspace.delete", "workspace.members.manage", "workspace.export", "workspace.import",
        "connection.manage", "lakehouse.destructive", "webhook.manage", "alert.manage", "compute.read", "secret.manage",
        "intelligence.manage", "intelligence.sample", "governance.approve",
    },
    Role.CONTRIBUTOR: set(_CONTRIB) | {"workspace.export", "workspace.create"},
    Role.OPERATOR: {
        "workspace.read", "item.read", "job.read", "job.cancel", "job.retry", "pipeline.run", "compute.read",
        "events.read", "admin.health", "runtime.read",
    },
    Role.VIEWER: set(_READ_ITEM),
    Role.SERVICE_PRINCIPAL: {
        "workspace.read", "item.read", "item.create", "item.update", "connection.read", "connection.use",
        "lakehouse.read", "lakehouse.write", "sql.execute", "job.read", "job.submit", "job.cancel",
        "pipeline.run", "mcp.use", "integration.fabric.submit", "runtime.read", "events.read", "intelligence.read",
    },
}

#: Token scopes narrow what a credential may do regardless of the principal's roles.
TOKEN_SCOPES: dict[str, set[str]] = {
    "*": {"*"},
    "read": {p for p in PERMISSIONS if p.endswith(".read")} | {"sql.execute"},
    "write": {p for p in PERMISSIONS if not p.startswith(("platform.", "identity.", "audit.", "admin.", "secret.manage", "compute.manage", "runtime.manage"))},
    "mcp": {"mcp.use", "workspace.read", "item.read", "lakehouse.read", "sql.execute", "job.read", "intelligence.read"},
    "mcp:write": {"mcp.use", "workspace.read", "item.read", "lakehouse.read", "sql.execute", "job.read", "job.submit", "job.cancel", "intelligence.read"},
    "fabric": {"integration.fabric.submit", "job.read", "job.cancel", "workspace.read", "item.read", "lakehouse.read"},
    #: Added by FABRIC: meant for the *derived* in-process principal the Fabric adapter uses to hand a verified capsule to ``JobService.submit`` (which needs
    #: ``job.submit``), not for issued tokens; role bindings still limit it to the mapped workspaces.
    "fabric-internal": {"job.submit", "job.read", "job.cancel", "workspace.read", "item.read", "item.create", "lakehouse.read", "secret.use", "connection.use"},
    "worker": {"compute.read"},
    #: Short-lived credential handed to a running job (child runs, secrets API for notebookutils); revoked when the job ends.
    "job": {"job.read", "job.submit", "job.cancel", "workspace.read", "item.read", "lakehouse.read", "lakehouse.write", "secret.use",
            "connection.use", "sql.execute", "connection.read"},
}

#: Pseudo scope of a login session whose account must choose a new password (``principals.must_change_password``). It is deliberately **not** in ``TOKEN_SCOPES``
#: (nobody can mint it, and an unknown scope grants no permission at all); ``olcf.api.deps`` additionally refuses every route except ``POST /v1/identity/password``.
PASSWORD_CHANGE_SCOPE = "password-change"  # noqa: S105 - the name of a token scope, not a credential

WILDCARD_OK = {"platform.admin"}

#: Roles that carry platform-level (not workspace-scoped) authority; granting or taking over one needs the same authority (see ``Principal.covers_role``).
PLATFORM_ROLES = frozenset({Role.PLATFORM_ADMIN.value, Role.SECURITY_ADMIN.value, Role.RUNTIME_ADMIN.value})

#: Granted to *every* authenticated principal that holds at least one role binding at any scope (the runtime catalog and the compatibility matrix are
#: needed by the job / session forms of a workspace Viewer or Contributor, who has no org-level binding); token scopes still apply.
BASELINE_PERMISSIONS = frozenset({"runtime.read"})


def _intersect_perms(a: set[str], b: set[str]) -> set[str]:
    """Intersection of two permission sets in which ``"*"`` means *everything*."""
    if "*" in a:
        return set(b)
    if "*" in b:
        return set(a)
    return a & b


@dataclass(frozen=True)
class Binding:
    """A role granted at a scope: role name, scope type, scope id and the source that created it (``manual`` by default)."""
    role: str
    scope_type: str = ScopeType.ORG.value
    scope_id: str = ""
    source: str = "manual"


@dataclass
class Principal:
    """The authenticated caller attached to a request."""

    id: str
    name: str
    kind: str = "user"  # user | service | worker | fabric | anonymous
    bindings: list[Binding] = field(default_factory=list)
    token_scopes: list[str] = field(default_factory=lambda: ["*"])
    source: str = "local"
    token_id: str | None = None
    claims: dict[str, Any] = field(default_factory=dict)
    ip: str | None = None
    correlation_id: str | None = None
    display_name: str | None = None
    #: Derived credentials only (job / kernel / pipeline-run tokens; see :func:`derive_credential`): the permission ceiling inherited from the credential that caused
    #: the derivation (``None`` = none was recorded, i.e. an ordinary credential) and the single workspace the credential is bound to.
    ceiling: frozenset[str] | None = None
    workspace_lock: str | None = None
    #: The credential the derivation chain started from (a PAT id, or a login access token's ``jti``) and the login session (``fam``) it belongs to; a derived
    #: credential stops working when either is revoked. Plain credentials keep these empty and use their own ``token_id`` / the ``fam`` claim instead.
    origin_token_id: str | None = None
    origin_family: str | None = None

    # ------------------------------------------------------------------ evaluation
    def _scope_perms(self) -> set[str]:
        out: set[str] = set()
        for s in self.token_scopes:
            out |= TOKEN_SCOPES.get(s, set())
        return out

    def effective_permissions(self) -> set[str]:
        """The permissions this credential's *scopes* allow (``{"*"}`` = every permission): its own scopes intersected with the inherited ceiling, if any."""
        own = self._scope_perms()
        if self.ceiling is None:
            return own
        return _intersect_perms(own, set(self.ceiling))

    def _role_grants(self, role: str, perm: str) -> bool:
        perms = ROLE_PERMISSIONS.get(role, set())
        return "*" in perms or perm in perms

    def _scope_allows(self, perm: str) -> bool:
        allowed = self.effective_permissions()
        return "*" in allowed or perm in allowed or any(fnmatch.fnmatch(perm, a) for a in allowed if "*" in a)

    def can(
        self,
        perm: str,
        *,
        workspace_id: str | None = None,
        item_id: str | None = None,
        connection_id: str | None = None,
        runtime_id: str | None = None,
        pool_id: str | None = None,
    ) -> bool:
        if perm not in PERMISSIONS:
            raise KeyError(f"unknown permission {perm!r}")
        if not self._scope_allows(perm):
            return False
        if perm in BASELINE_PERMISSIONS and self.bindings:
            return True
        if self.workspace_lock is not None and not self._in_lock(workspace_id):
            return False
        wanted = {
            ScopeType.ORG.value: "",
            ScopeType.WORKSPACE.value: workspace_id,
            ScopeType.ITEM.value: item_id,
            ScopeType.CONNECTION.value: connection_id,
            ScopeType.RUNTIME.value: runtime_id,
            ScopeType.POOL.value: pool_id,
        }
        for b in self.bindings:
            if not self._role_grants(b.role, perm):
                continue
            if b.scope_type == ScopeType.ORG.value:
                return True
            target = wanted.get(b.scope_type)
            if target and target == b.scope_id:
                return True
        return False

    def _in_lock(self, workspace_id: str | None) -> bool:
        """A workspace-bound credential answers only for its own workspace. A question that names no workspace is an organisation-wide question and the answer is no
        (the baseline catalog permission is answered before this is asked): a job credential must not be able to do something to "any workspace" that its own
        workspace would not allow."""
        return workspace_id == self.workspace_lock

    def require(self, perm: str, **scope: str | None) -> None:
        if not self.can(perm, **scope):  # type: ignore[arg-type]
            raise Forbidden(
                f"permission '{perm}' is required",
                details={"permission": perm, **{k: v for k, v in scope.items() if v}},
            )

    def is_admin(self) -> bool:
        return self.can("platform.admin")

    def covers_role(self, role: str, scope_type: str = ScopeType.ORG.value, scope_id: str = "") -> bool:
        """The no-escalation rule behind granting roles, creating accounts with roles and acting on other accounts: nobody can hand out, or take over,
        a platform-level role (:data:`PLATFORM_ROLES`) whose permissions they do not hold themselves.

        Platform roles are covered only when every permission of the role is held by this principal (token scopes included): ``PlatformAdmin`` needs
        ``platform.admin``, so a SecurityAdmin can neither create PlatformAdmins nor RuntimeAdmins. Workspace-class roles (WorkspaceAdmin, Contributor,
        Operator, Viewer, ServicePrincipal) are not escalation targets here - *who may grant them* is the caller's ``identity.manage`` (organisation
        scope) or ``workspace.members.manage`` (workspace scope), checked by the route.
        """
        perms = ROLE_PERMISSIONS.get(role)
        if perms is None:
            return False
        if role not in PLATFORM_ROLES:
            return True
        if "*" in perms:
            return self.can("platform.admin")
        try:
            return all(self.can(p) for p in perms)
        except KeyError:  # a role mentions a permission that does not exist: refuse rather than guess
            return False

    def outranks(self, bindings: list[Binding]) -> bool:
        """True when every *platform-level* binding of another account is covered by this principal (see :meth:`covers_role`)."""
        return all(self.covers_role(b.role, b.scope_type, b.scope_id) for b in bindings)

    def visible_workspaces(self, perm: str = "workspace.read") -> set[str] | None:
        """Workspace ids where ``perm`` holds, or ``None`` meaning *all* workspaces (org-level grant)."""
        if not self._scope_allows(perm):
            return set()
        ids: set[str] = set()
        for b in self.bindings:
            if not self._role_grants(b.role, perm):
                continue
            if b.scope_type == ScopeType.ORG.value:
                return None if self.workspace_lock is None else {self.workspace_lock}
            if b.scope_type == ScopeType.WORKSPACE.value:
                ids.add(b.scope_id)
        return ids if self.workspace_lock is None else ids & {self.workspace_lock}

    def credential_ceiling(self, workspace_id: str) -> dict[str, Any]:
        """The document persisted with a job / kernel / pipeline run submitted by this principal: everything a credential *derived* for it later may do is bounded by it.

        ``permissions`` is what this credential's scopes allow *now* (its own scopes intersected with the ceiling it inherited, if it is itself derived - a job that
        submits a child job can never hand the child more than it holds); ``workspace_id`` is the single workspace the derived credential is bound to; ``origin_*``
        name the user credential at the root of the chain, so that revoking it (a PAT revoked, a login session ended) also ends every credential derived from it.
        Role bindings are deliberately **not** snapshotted: they are re-read on every request, so a later demotion applies to running jobs too.
        """
        perms = self.effective_permissions()
        return {
            "version": 1, "permissions": ["*"] if "*" in perms else sorted(perms), "scopes": list(self.token_scopes), "workspace_id": workspace_id,
            "origin_token_id": self.origin_token_id or self.token_id, "origin_family": self.origin_family or self.claims.get("fam"),
        }

    def summary(self) -> dict[str, Any]:
        return {
            "id": self.id, "name": self.name, "kind": self.kind, "display_name": self.display_name or self.name,
            "source": self.source, "token_scopes": self.token_scopes,
            "bindings": [b.__dict__ for b in self.bindings],
        }


class Authorizer:
    """Loads role bindings for principals with a tiny TTL cache (invalidated on writes)."""

    def __init__(self, db: Database, ttl: float = 3.0) -> None:
        self.db = db
        self.ttl = ttl
        self._cache: dict[str, tuple[float, list[Binding]]] = {}
        self._lock = threading.Lock()

    def invalidate(self, principal_id: str | None = None) -> None:
        with self._lock:
            if principal_id is None:
                self._cache.clear()
            else:
                self._cache.pop(principal_id, None)

    def bindings_for(self, principal_id: str) -> list[Binding]:
        now = time.monotonic()
        with self._lock:
            hit = self._cache.get(principal_id)
            if hit and now - hit[0] < self.ttl:
                return hit[1]
        with self.db.connect() as conn:
            rows = fetch_all(conn, select(S.role_bindings).where(S.role_bindings.c.principal_id == principal_id))
        out = [Binding(r["role"], r["scope_type"], r["scope_id"] or "", r["source"]) for r in rows]
        with self._lock:
            self._cache[principal_id] = (now, out)
        return out

    def principal_from_row(self, prow: dict[str, Any], *, token_scopes: list[str] | None = None, token_id: str | None = None,
                           claims: dict[str, Any] | None = None, credential: dict[str, Any] | None = None) -> Principal:
        """Build the request principal. ``credential`` is a persisted ceiling document (:meth:`Principal.credential_ceiling`) of a derived credential, applied as is."""
        p = Principal(
            id=prow["id"], name=prow["name"], kind=prow["kind"], bindings=self.bindings_for(prow["id"]),
            token_scopes=token_scopes or ["*"], source=prow.get("source", "local"), token_id=token_id,
            claims=claims or {}, display_name=prow.get("display_name"),
        )
        if credential is not None:
            apply_credential(p, credential)
        return p


def apply_credential(principal: Principal, credential: dict[str, Any]) -> Principal:
    """Bind ``principal`` to a persisted ceiling document: its permissions become (scopes ∩ ceiling) and it is locked to the document's workspace.

    A malformed document fails **closed** (empty ceiling), never open.
    """
    perms = credential.get("permissions")
    principal.ceiling = frozenset(str(p) for p in perms) if isinstance(perms, list) else frozenset()
    ws = credential.get("workspace_id")
    principal.workspace_lock = str(ws) if ws else "\x00no-workspace"  # a document without a workspace matches nothing
    principal.origin_token_id = credential.get("origin_token_id") or None
    principal.origin_family = credential.get("origin_family") or None
    return principal


#: What the kernel of a pooled SQL session may do through its callback credential. These kernels are shared by every user of a workspace, so their credential must not
#: carry anything of the user who happened to start the pool: read-only, one workspace.
SQL_KERNEL_PERMISSIONS = frozenset({"workspace.read", "item.read", "connection.read", "lakehouse.read", "sql.execute", "job.read"})

#: Ceiling for a job submitted before ceilings were persisted (an upgrade with queued jobs): read-only. Failing closed costs such a job its callbacks, not its data access.
LEGACY_JOB_PERMISSIONS = frozenset({"workspace.read", "item.read", "connection.read", "lakehouse.read", "sql.execute", "job.read"})


def fixed_ceiling(permissions: frozenset[str] | set[str], workspace_id: str, *, origin: dict[str, Any] | None = None) -> dict[str, Any]:
    """A ceiling document for credentials that do not derive from a user (pooled SQL kernels, legacy rows)."""
    o = origin or {}
    return {"version": 1, "permissions": sorted(permissions), "scopes": [], "workspace_id": workspace_id,
            "origin_token_id": o.get("origin_token_id"), "origin_family": o.get("origin_family")}


SYSTEM_PRINCIPAL = Principal(id="usr_system", name="system", kind="service", bindings=[Binding(Role.PLATFORM_ADMIN.value)], source="system")
