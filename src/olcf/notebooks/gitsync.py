# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Git integration of a workspace (``ctx.service("git")``).

A workspace is *connected* to one repository folder and branch.  The repository holds the same layout as the workspace export (see
:mod:`olcf.notebooks.workspace_io`): ``<folder>/items/<folder>/<name>.<type>/...`` and ``<folder>/connections/<name>.json`` (metadata only - **secrets never**).

Model
-----
* The unit of sync is an *item directory* (or a connection file).  Each unit has a **content hash** over its canonical files (identity that differs between
  workspaces - ``logicalId``, ``.platform`` - and notebook outputs are ignored, notebooks are compared after parsing, so formatting-only edits are not changes).
* Three trees are compared: **base** = the commit the workspace was last synchronised with (``git.synced_commit``, stored in ``workspaces.git``; empty at first
  connect), **local** = the workspace as it is now, **remote** = the branch tip after ``git fetch``.  Per unit: equal -> in sync; remote == base -> *outgoing*
  (local change); local == base -> *incoming*; otherwise a **conflict** (both sides changed differently).
* ``commit`` refuses when anything is incoming or in conflict ("update from Git first"), scans the files that would leave the platform for secrets and rejects
  the whole commit if anything matches, builds the commit with ``git fast-import`` on top of the remote tip (no work tree, so nothing can be written outside the
  repository) and pushes it without force; only after a successful push does the synchronised commit advance.
* ``pull`` applies incoming units through the foundation ``ItemService`` (validation, versions, audit), refuses unresolved conflicts (``resolution`` /
  ``resolutions`` = ``ours`` keeps the workspace version, ``theirs`` takes the repository version) and never leaves a half-applied tree because every unit is
  parsed and validated first.
* Local state is a bare repository per workspace under ``<data_dir>/git``; every git invocation is a subprocess without shell (:mod:`olcf.notebooks.gitrunner`),
  credentials come from the workspace's connection + secret through a throw-away ``GIT_ASKPASS`` file and are never in URLs, arguments or logs.  Admin settings
  (``git.*``) restrict hosts and whether ``file://`` is allowed.
"""

from __future__ import annotations

import contextlib
import re
import shutil
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse
from urllib.request import url2pathname

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update

from olcf.core.context import AppContext
from olcf.core.errors import BadRequest, Conflict, Forbidden, NotFound, OlcfError, ValidationFailed
from olcf.core.filelock import FileLock
from olcf.core.timeutil import now_iso
from olcf.db import schema as S
from olcf.db.util import new_etag
from olcf.notebooks import secretscan, treeapply, workspace_io
from olcf.notebooks.gitrunner import (
    Credentials,
    GitRunner,
    GitUnavailable,
    Repo,
    check_folder,
    check_ref,
    git_capabilities,
    git_unavailable_remediation,
)
from olcf.security.rbac import Principal

_SHA = re.compile(r"^[0-9a-f]{40,64}$")
_LOOPBACK = {"localhost", "127.0.0.1", "::1", "[::1]"}
Resolution = Literal["ours", "theirs"]


# ------------------------------------------------------------------------------ request models
class ConnectRequest(BaseModel):
    """Request body for connecting a workspace to a Git repository: HTTPS URL without credentials, branch and folder."""
    model_config = ConfigDict(extra="forbid")
    repo_url: str | None = Field(None, description="https:// repository URL (never with credentials); defaults to the connection's config.url")
    branch: str = "main"
    folder: str = Field("", description="folder inside the repository that holds this workspace ('' = repository root)")
    connection: str | None = Field(None, description="id or name of a connection of type 'git' whose secret is the token / password")
    notebook_format: Literal["ipynb", "py", "fabric"] = "ipynb"
    create_branch: bool = Field(False, description="allow a branch that does not exist yet (it is created by the first commit)")


class CommitRequest(BaseModel):
    """Request body for committing workspace changes: a message and an optional list of units to commit."""
    model_config = ConfigDict(extra="forbid")
    message: str = Field(min_length=1, max_length=4000)
    items: list[str] | None = Field(None, description="commit only these units (unit path, item id or item name); default all outgoing changes")


class PullRequest(BaseModel):
    """Request body for pulling from the repository: a default conflict resolution, per-unit resolutions and a dry-run switch."""
    model_config = ConfigDict(extra="forbid")
    resolution: Resolution | None = Field(None, description="default resolution of all conflicts")
    resolutions: dict[str, Resolution] = Field(default_factory=dict, description="per unit (unit path, item id or item name)")
    dry_run: bool = False


class BranchCreate(BaseModel):
    """Request body for creating a branch, optionally from another branch and switching to it."""
    model_config = ConfigDict(extra="forbid")
    name: str
    source: str | None = Field(None, description="branch to start from (default: the workspace's branch)")
    switch: bool = False


class BranchSwitch(BaseModel):
    """Request body for switching the workspace to another branch."""
    model_config = ConfigDict(extra="forbid")
    name: str


# ------------------------------------------------------------------------------ helpers
def check_repo_url(url: str, *, allow_file: bool = False, allowed_hosts: list[str] | None = None, allow_insecure_http: bool = False) -> str:
    """Validate a repository URL: ``https://`` (``http://`` only for loopback or when the admin allows it), ``file://`` only when the admin allows it, **never**
    credentials in the URL, no ssh / git / ext transports."""
    url = (url or "").strip()
    if not url or len(url) > 2048 or re.search(r"[\s\x00-\x1f\x7f\\]", url):
        raise ValidationFailed("invalid repository URL")
    try:
        p = urlparse(url)
        _ = p.port  # raises ValueError for a malformed port / bracketed host
    except ValueError:
        raise ValidationFailed("invalid repository URL") from None
    if p.scheme == "file":
        if not allow_file:
            raise ValidationFailed("file:// repositories are disabled", remediation="an administrator can enable git.allow_file_urls")
        if p.netloc not in ("", "localhost") or not p.path.startswith("/") or ".." in p.path.split("/"):
            raise ValidationFailed("a file:// URL must be file:///absolute/path")
        if not Path(url2pathname(p.path)).is_dir():
            raise ValidationFailed("the local repository path does not exist")
        return url
    if p.scheme not in ("https", "http"):
        raise ValidationFailed(f"unsupported repository URL scheme {p.scheme!r}; use https:// with a connection (ssh is not supported)")
    host = (p.hostname or "").lower()
    if not host:
        raise ValidationFailed("the repository URL has no host")
    if p.username or p.password or "@" in p.netloc:
        raise ValidationFailed("credentials must not be part of the repository URL", remediation="create a 'git' connection with a secret and pass connection=")
    if p.query or p.fragment:
        raise ValidationFailed("the repository URL must not have a query or fragment")
    if p.scheme == "http" and host not in _LOOPBACK and not allow_insecure_http:
        raise ValidationFailed("plain http:// is only allowed for loopback hosts", remediation="use https:// or ask an administrator to enable git.allow_insecure_http")
    if allowed_hosts and not any(host == h.lower() or (h.startswith("*.") and host.endswith(h[1:].lower())) for h in allowed_hosts):
        raise ValidationFailed(f"host '{host}' is not in the allowed Git hosts", details={"allowed_hosts": allowed_hosts})
    return url


def _unit_name(key: str) -> str:
    base = key.rsplit("/", 1)[-1]
    return base[:-5] if base.endswith(".json") else base.rsplit(".", 1)[0]


@dataclass
class Decision:
    """Outcome of the three-way comparison for one unit: outgoing, incoming or conflict, with the local and remote change relative to the base.
    """
    key: str
    direction: str  # outgoing | incoming | conflict
    local: str  # change of the workspace relative to base: added | modified | deleted | unchanged
    remote: str


def _change(base: str | None, now: str | None) -> str:
    if base == now:
        return "unchanged"
    return "added" if base is None else "deleted" if now is None else "modified"


def three_way(base: dict[str, str], local: dict[str, str], remote: dict[str, str]) -> list[Decision]:
    """Per-unit three-way comparison by content hash (see the module docstring)."""
    out: list[Decision] = []
    for key in sorted(set(base) | set(local) | set(remote)):
        b, loc, rem = base.get(key), local.get(key), remote.get(key)
        if loc == rem:
            continue
        direction = "outgoing" if rem == b else "incoming" if loc == b else "conflict"
        out.append(Decision(key, direction, _change(b, loc), _change(b, rem)))
    return out


@dataclass
class Compare:
    """Everything a Git status, commit or pull needs from one comparison: repository state, the local, base and remote content hashes, the grouped file contents, the units and the decisions.
    """
    st: dict[str, Any]
    tip: str | None
    synced: str | None
    L: dict[str, str]
    B: dict[str, str]
    R: dict[str, str]
    local_groups: dict[str, dict[str, bytes]]
    remote_groups: dict[str, dict[str, bytes]]
    remote_paths: set[str]
    units: dict[str, dict[str, Any]]
    decisions: list[Decision] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


class GitService:
    """Connect / status / commit / pull / branches for workspaces; see the module docstring."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()
        self._register_settings()

    # ------------------------------------------------------------------ settings
    def _register_settings(self) -> None:
        kv = self.ctx.maybe_service("settings")
        if kv is None:
            return

        def _bool(v: Any) -> bool:
            if not isinstance(v, bool):
                raise BadRequest("must be true or false")
            return v

        def _hosts(v: Any) -> list[str]:
            if not isinstance(v, list) or not all(isinstance(x, str) and len(x) < 253 for x in v):
                raise BadRequest("must be a list of host names ('*.example.com' allowed)")
            return v

        def _secs(v: Any) -> float:
            if not isinstance(v, (int, float)) or isinstance(v, bool) or not 5 <= v <= 3600:
                raise BadRequest("must be between 5 and 3600 seconds")
            return float(v)

        kv.register("git.allow_file_urls", description="Allow file:// repositories (local development / tests only)", default=False, validator=_bool)
        kv.register("git.allow_insecure_http", description="Allow http:// repository URLs on non-loopback hosts (credentials would travel in clear text)", default=False, validator=_bool)
        kv.register("git.allowed_hosts", description="Restrict repositories to these hosts (empty = any https host)", default=[], validator=_hosts)
        kv.register("git.timeout_seconds", description="Timeout of one git command", default=120.0, validator=_secs)

    def _kv(self, key: str, default: Any) -> Any:
        kv = self.ctx.maybe_service("settings")
        v = kv.get(key) if kv is not None else None
        return default if v is None else v

    def _check_url(self, url: str) -> str:
        return check_repo_url(url, allow_file=bool(self._kv("git.allow_file_urls", False)), allowed_hosts=list(self._kv("git.allowed_hosts", []) or []),
                              allow_insecure_http=bool(self._kv("git.allow_insecure_http", False)))

    # ------------------------------------------------------------------ plumbing
    def _ws(self, principal: Principal, ref: str, perm: str, *, needs_git: bool = True) -> dict[str, Any]:
        ws = self.ctx.service("workspaces").resolve(ref)
        principal.require(perm, workspace_id=ws["id"])
        if needs_git:
            self.require_git()
        return ws

    @staticmethod
    def require_git() -> None:
        """Fail early and cleanly (503 ``git_unavailable`` with remediation) when no real git executable exists - before any state is touched."""
        if not git_capabilities()["available"]:
            raise GitUnavailable("Git sync is unavailable: no usable git executable was found on the control plane", remediation=git_unavailable_remediation(),
                                 details={"capability": "git", "see": "GET /v1/workspaces/{ws}/git/capabilities"})

    def capabilities(self, principal: Principal, ws_ref: str) -> dict[str, Any]:
        """``GET /workspaces/{ws}/git/capabilities``: is Git sync usable on this control plane (the UI hides/explains the feature otherwise)?"""
        ws = self._ws(principal, ws_ref, "workspace.read", needs_git=False)
        caps = git_capabilities()
        return {**caps, "workspace_id": ws["id"], "connected": bool((ws.get("git") or {}).get("repo_url")),
                "allow_file_urls": bool(self._kv("git.allow_file_urls", False))}

    def _repo_dir(self, ws_id: str) -> Path:
        return self.ctx.settings.path("git", f"{ws_id}.git")

    def _runner(self, ws_id: str, url: str) -> Repo:
        allow = "https:http" + (":file" if bool(self._kv("git.allow_file_urls", False)) else "")
        return Repo(GitRunner(self._repo_dir(ws_id), self.ctx.settings.path("tmp", "git"), timeout=float(self._kv("git.timeout_seconds", 120.0)), allow_protocols=allow), url)

    @contextlib.contextmanager
    def _lock(self, ws_id: str) -> Iterator[None]:
        with self._guard:
            lock = self._locks.setdefault(ws_id, threading.Lock())
        with lock:
            lock_dir = self.ctx.settings.path("git")
            lock_dir.mkdir(parents=True, exist_ok=True)
            with FileLock(lock_dir / f"{ws_id}.lock"):  # cross-process (several controllers share the git directory); blocks like the old flock
                yield

    def _state(self, ws: dict[str, Any]) -> dict[str, Any]:
        """The connection state of a workspace, re-validated on every use (``workspaces.git`` can be patched through the API, so it is not trusted)."""
        g = dict(ws.get("git") or {})
        if not g.get("repo_url"):
            raise BadRequest("this workspace is not connected to a Git repository", remediation="POST /v1/workspaces/{id}/git/connect")
        g["repo_url"] = self._check_url(str(g["repo_url"]))
        g["branch"] = check_ref(str(g.get("branch") or "main"))
        g["folder"] = check_folder(str(g.get("folder") or ""))
        if g.get("notebook_format", "ipynb") not in workspace_io.NOTEBOOK_FORMATS:
            raise ValidationFailed("invalid notebook_format in the Git connection")
        g.setdefault("notebook_format", "ipynb")
        if g.get("synced_commit") is not None and not _SHA.match(str(g["synced_commit"])):
            g["synced_commit"] = None
        g.setdefault("connection", None)
        return g

    def _save(self, ws_id: str, st: dict[str, Any], *, bump: bool = False) -> None:
        vals: dict[str, Any] = {"git": st}
        if bump:
            vals.update(etag=new_etag(), updated_at=now_iso())
        with self.ctx.db.begin() as conn:
            conn.execute(update(S.workspaces).where(S.workspaces.c.id == ws_id).values(**vals))

    def _creds(self, principal: Principal, ws_id: str, st: dict[str, Any]) -> Credentials | None:
        ref = st.get("connection")
        if not ref:
            return None
        con = self.ctx.service("connections").resolve(ref, ws_id)
        if con["workspace_id"] not in (None, ws_id):
            raise Forbidden("the Git connection belongs to another workspace")
        principal.require("connection.use", workspace_id=ws_id, connection_id=con["id"])
        if not con["secret_id"]:
            return None
        secret = self.ctx.secrets.resolve(con["secret_id"])
        cfg = con.get("config") or {}
        username = cfg.get("username")
        if isinstance(secret, dict):
            username = secret.get("username") or username
            password = secret.get("password") or secret.get("token")
        else:
            password = secret
        if not password or not isinstance(password, str):
            raise BadRequest("the Git connection's secret has no password / token")
        return Credentials(str(username or "olcf"), password)

    # ------------------------------------------------------------------ connect / disconnect
    def connect(self, principal: Principal, ws_ref: str, req: ConnectRequest) -> dict[str, Any]:
        """Connect the workspace to a repository folder + branch (the workspace content is not changed; ``status`` shows what differs)."""
        ws = self._ws(principal, ws_ref, "workspace.update")
        if (ws.get("git") or {}).get("repo_url"):
            raise Conflict("this workspace is already connected to a Git repository", remediation="disconnect it first")
        con = None
        if req.connection:
            con = self.ctx.service("connections").resolve(req.connection, ws["id"])
            if con["type"] != "git":
                raise ValidationFailed(f"connection '{con['name']}' is of type '{con['type']}', not 'git'")
            principal.require("connection.use", workspace_id=ws["id"], connection_id=con["id"])
        url = req.repo_url or ((con or {}).get("config") or {}).get("url")
        if not url:
            raise ValidationFailed("repo_url is required (or a connection whose config has 'url')")
        st = {"repo_url": self._check_url(url), "branch": check_ref(req.branch), "folder": check_folder(req.folder), "connection": con["id"] if con else None,
              "notebook_format": req.notebook_format, "connected_at": now_iso(), "connected_by": principal.id, "synced_commit": None}
        with self._lock(ws["id"]):
            repo = self._runner(ws["id"], st["repo_url"])
            repo.init()
            try:
                creds = self._creds(principal, ws["id"], st)
                heads = repo.ls_remote(creds)
                if st["branch"] not in heads and not req.create_branch:
                    raise NotFound(f"branch '{st['branch']}' does not exist in the repository", details={"branches": sorted(heads)[:50]},
                                   remediation="choose an existing branch or pass create_branch=true")
                if st["branch"] in heads:
                    repo.fetch(st["branch"], creds)
            except OlcfError:
                shutil.rmtree(self._repo_dir(ws["id"]), ignore_errors=True)
                raise
            self._save(ws["id"], st, bump=True)
        self.ctx.audit.record(principal, "git.connected", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"],
                              details={"repo": st["repo_url"], "branch": st["branch"], "folder": st["folder"]})
        return self.status(principal, ws["id"], fetch=False)

    def disconnect(self, principal: Principal, ws_ref: str) -> dict[str, Any]:
        ws = self._ws(principal, ws_ref, "workspace.update", needs_git=False)  # removing the local clone needs no git
        if not (ws.get("git") or {}).get("repo_url"):
            return {"connected": False}
        with self._lock(ws["id"]):
            self._save(ws["id"], {}, bump=True)
            shutil.rmtree(self._repo_dir(ws["id"]), ignore_errors=True)
        self.ctx.audit.record(principal, "git.disconnected", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"])
        return {"connected": False}

    # ------------------------------------------------------------------ comparison
    def _hash_groups(self, groups: dict[str, dict[str, bytes]], fmt: str) -> dict[str, str]:
        return {k: workspace_io.unit_hash(workspace_io.canonical_unit(self.ctx, k, files, fmt)) for k, files in groups.items()}

    def _compare(self, principal: Principal, ws: dict[str, Any], st: dict[str, Any], repo: Repo, *, fetch: bool) -> Compare:
        creds = self._creds(principal, ws["id"], st) if fetch else None
        tip = repo.fetch(st["branch"], creds) if fetch else repo.tip(st["branch"])
        synced = st.get("synced_commit")
        if not tip or not synced or not repo.has_commit(synced):
            synced = None
        fmt, folder = st["notebook_format"], st["folder"]
        files, units = workspace_io.build_tree(self.ctx, ws["id"], mode="git", notebook_format=fmt)
        local_groups = workspace_io.group_units(files)
        base_tree = repo.read_tree(synced, folder) if synced else None
        same = bool(tip and synced and repo.tree_id(tip, folder) == repo.tree_id(synced, folder))
        remote_tree = base_tree if same else (repo.read_tree(tip, folder) if tip else None)
        base_groups = workspace_io.group_units(base_tree.files) if base_tree else {}
        remote_groups = base_groups if same else (workspace_io.group_units(remote_tree.files) if remote_tree else {})
        base_hashes = self._hash_groups(base_groups, fmt)
        cmp = Compare(st=st, tip=tip, synced=synced, L=self._hash_groups(local_groups, fmt), B=base_hashes, R=base_hashes if same else self._hash_groups(remote_groups, fmt),
                      local_groups=local_groups, remote_groups=remote_groups, remote_paths=set(remote_tree.files) if remote_tree else set(), units=units,
                      skipped=remote_tree.skipped if remote_tree else [])
        cmp.decisions = three_way(cmp.B, cmp.L, cmp.R)
        return cmp

    def _describe(self, cmp: Compare, d: Decision) -> dict[str, Any]:
        u = cmp.units.get(d.key)
        return {"unit": d.key, "kind": u["kind"] if u else ("connection" if d.key.startswith("connections/") else "item"),
                "type": u["type"] if u else ("connection" if d.key.startswith("connections/") else d.key.rsplit(".", 1)[-1]), "name": u["name"] if u else _unit_name(d.key),
                "item_id": u["item_id"] if u else None, "direction": d.direction, "local": d.local, "remote": d.remote}

    def status(self, principal: Principal, ws_ref: str, *, fetch: bool = True) -> dict[str, Any]:
        """Changed / added / deleted units of the workspace vs the last synchronised commit (``local``), what the remote changed (``remote``) and the direction."""
        ws = self._ws(principal, ws_ref, "workspace.export")
        g = ws.get("git") or {}
        if not g.get("repo_url"):
            return {"connected": False}
        st = self._state(ws)
        with self._lock(ws["id"]):
            repo = self._runner(ws["id"], st["repo_url"])
            repo.init()
            cmp = self._compare(principal, ws, st, repo, fetch=fetch)
        changes = [self._describe(cmp, d) for d in cmp.decisions]
        return {"connected": True, "repo_url": st["repo_url"], "branch": st["branch"], "folder": st["folder"], "notebook_format": st["notebook_format"], "connection": st["connection"],
                "synced_commit": cmp.synced, "remote_commit": cmp.tip, "remote_branch_exists": cmp.tip is not None, "fetched": fetch,
                "in_sync": not changes,
                "counts": {"outgoing": sum(c["direction"] == "outgoing" for c in changes), "incoming": sum(c["direction"] == "incoming" for c in changes),
                          "conflicts": sum(c["direction"] == "conflict" for c in changes), "units": len(set(cmp.L) | set(cmp.R))},
                "changes": changes, "skipped_paths": cmp.skipped[:50]}

    # ------------------------------------------------------------------ commit + push
    @staticmethod
    def _selected(cmp: Compare, d: Decision, items: list[str]) -> bool:
        u = cmp.units.get(d.key) or {}
        return any(x in (d.key, u.get("item_id"), u.get("name"), _unit_name(d.key)) for x in items)

    def commit(self, principal: Principal, ws_ref: str, req: CommitRequest) -> dict[str, Any]:
        """Commit the outgoing changes of the workspace and push them (no force).  Rejected when the remote has changes you do not have yet, when nothing matches and
        - always - when the secret scan finds credentials in what would be committed."""
        ws = self._ws(principal, ws_ref, "workspace.export")
        st = self._state(ws)
        msg_findings = secretscan.scan_text(req.message, "<commit message>")
        if msg_findings:
            self._reject(principal, ws, msg_findings)
        with self._lock(ws["id"]):
            repo = self._runner(ws["id"], st["repo_url"])
            repo.init()
            creds = self._creds(principal, ws["id"], st)
            cmp = self._compare(principal, ws, st, repo, fetch=True)
            blockers = [self._describe(cmp, d) for d in cmp.decisions if d.direction in ("incoming", "conflict")]
            if blockers:
                raise Conflict(f"the remote branch has changes to {len(blockers)} unit(s) that the workspace does not have; update from Git first",
                               details={"blocking": blockers[:100]}, remediation="POST /v1/workspaces/{id}/git/pull (resolve conflicts with ours|theirs)")
            outgoing = [d for d in cmp.decisions if d.direction == "outgoing"]
            if req.items:
                outgoing = [d for d in outgoing if self._selected(cmp, d, req.items)]
                if not outgoing:
                    raise NotFound("none of the requested items has a pending change", details={"requested": req.items[:50]})
            if not outgoing:
                return {"committed": False, "commit": cmp.tip, "reason": "nothing to commit", "units": []}
            prefix = f"{st['folder']}/" if st["folder"] else ""
            changes: dict[str, bytes | None] = {}
            for d in outgoing:
                old = [p for p in cmp.remote_paths if p == d.key or p.startswith(d.key + "/")]
                new_files = cmp.local_groups.get(d.key)
                if new_files is None:
                    for p in old:
                        changes[prefix + p] = None
                    continue
                written = {(d.key if rel == "" else f"{d.key}/{rel}") for rel in new_files}
                for p in old:
                    if p not in written:
                        changes[prefix + p] = None
                for rel, data in new_files.items():
                    changes[prefix + (d.key if rel == "" else f"{d.key}/{rel}")] = data
            findings = secretscan.scan_files({p: b for p, b in changes.items() if b is not None})
            if findings:
                self._reject(principal, ws, findings)
            sha = repo.commit(cmp.tip, changes, req.message, (principal.name or principal.id, f"{principal.id}@users.olcf.invalid"), f"refs/olcf/pending/{ws['id']}")
            repo.push(sha, st["branch"], creds)
            st["synced_commit"] = sha
            st["last_commit_at"] = now_iso()
            self._save(ws["id"], st)
        described = [self._describe(cmp, d) for d in outgoing]
        self.ctx.audit.record(principal, "git.committed", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"],
                              details={"commit": sha, "branch": st["branch"], "units": len(outgoing)})
        self.ctx.events.publish("git.committed", {"commit": sha, "branch": st["branch"], "units": len(outgoing)}, workspace_id=ws["id"], subject=ws["id"])
        return {"committed": True, "commit": sha, "branch": st["branch"], "pushed": True, "units": described}

    def _reject(self, principal: Principal, ws: dict[str, Any], findings: list[secretscan.Finding]) -> None:
        self.ctx.audit.record(principal, "git.commit_rejected", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"], outcome="denied",
                              details={"reason": "secret scan", "findings": len(findings), "rules": sorted({f.rule for f in findings})})
        raise ValidationFailed(f"the secret scan found {len(findings)} potential secret(s); nothing was committed",
                               details={"findings": [f.to_dict() for f in findings[:50]]},
                               remediation="remove the value, store it with POST /v1/secrets and reference it (sec_...), or mark an intentional dummy with 'olcf: allow-secret'")

    # ------------------------------------------------------------------ pull
    def pull(self, principal: Principal, ws_ref: str, req: PullRequest | None = None) -> dict[str, Any]:
        """Update the workspace from the repository (incoming units); conflicts must be resolved explicitly."""
        req = req or PullRequest()
        ws = self._ws(principal, ws_ref, "workspace.import")
        st = self._state(ws)
        with self._lock(ws["id"]):
            repo = self._runner(ws["id"], st["repo_url"])
            repo.init()
            cmp = self._compare(principal, ws, st, repo, fetch=True)
            if cmp.tip is None:
                raise NotFound(f"branch '{st['branch']}' does not exist in the repository yet", remediation="commit first")
            incoming: dict[str, dict[str, bytes] | None] = {}
            conflicts: list[dict[str, Any]] = []
            kept: list[dict[str, Any]] = []
            for d in cmp.decisions:
                if d.direction == "incoming":
                    incoming[d.key] = cmp.remote_groups.get(d.key)
                elif d.direction == "conflict":
                    info = self._describe(cmp, d)
                    u = cmp.units.get(d.key) or {}
                    choice = next((req.resolutions[k] for k in (d.key, u.get("item_id"), u.get("name"), _unit_name(d.key)) if k and k in req.resolutions), req.resolution)
                    if choice is None:
                        conflicts.append(info)
                    elif choice == "theirs":
                        incoming[d.key] = cmp.remote_groups.get(d.key)
                        info["resolution"] = "theirs"
                        kept.append(info)
                    else:
                        info["resolution"] = "ours"
                        kept.append(info)
            if conflicts:
                raise Conflict(f"{len(conflicts)} unit(s) changed both in the workspace and in the repository; nothing was applied",
                               details={"conflicts": conflicts[:100]}, remediation="repeat with resolution=ours|theirs (or per unit in 'resolutions')")
            result = treeapply.apply_units(self.ctx, principal, ws["id"], incoming, cmp.units, dry_run=req.dry_run, comment="git pull")
            failed = [a for a in result["applied"] if a["action"] == "failed"]
            if failed:
                raise Conflict(f"{len(failed)} unit(s) could not be applied; the synchronised commit was not advanced (repeat the pull after fixing them)",
                               details={"failed": failed[:50], "applied": [a for a in result["applied"] if a["action"] != "failed"][:100]})
            if not req.dry_run:
                st["synced_commit"] = cmp.tip
                st["last_pull_at"] = now_iso()
                self._save(ws["id"], st)
        if not req.dry_run:
            self.ctx.audit.record(principal, "git.pulled", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"],
                                  details={"commit": cmp.tip, "units": len(incoming), "conflicts_resolved": len(kept)})
            self.ctx.events.publish("git.pulled", {"commit": cmp.tip, "units": len(incoming)}, workspace_id=ws["id"], subject=ws["id"])
        return {"commit": cmp.tip, "dry_run": req.dry_run, "applied": result["applied"], "resolved_conflicts": kept, "id_mapping": result["id_mapping"]}

    # ------------------------------------------------------------------ branches
    def branches(self, principal: Principal, ws_ref: str) -> dict[str, Any]:
        ws = self._ws(principal, ws_ref, "workspace.export")
        st = self._state(ws)
        repo = self._runner(ws["id"], st["repo_url"])
        with self._lock(ws["id"]):
            repo.init()
            heads = repo.ls_remote(self._creds(principal, ws["id"], st))
        return {"current": st["branch"], "branches": [{"name": n, "commit": s, "current": n == st["branch"]} for n, s in sorted(heads.items())]}

    def create_branch(self, principal: Principal, ws_ref: str, req: BranchCreate) -> dict[str, Any]:
        """Create a remote branch from ``source`` (default: the workspace's branch); optionally switch the workspace to it."""
        ws = self._ws(principal, ws_ref, "workspace.update")
        st = self._state(ws)
        name = check_ref(req.name)
        source = check_ref(req.source or st["branch"], "source branch")
        with self._lock(ws["id"]):
            repo = self._runner(ws["id"], st["repo_url"])
            repo.init()
            creds = self._creds(principal, ws["id"], st)
            heads = repo.ls_remote(creds)
            if name in heads:
                raise Conflict(f"branch '{name}' already exists")
            if source not in heads:
                raise NotFound(f"source branch '{source}' does not exist", remediation="commit to the workspace's branch first")
            tip = repo.fetch(source, creds)
            if not tip:
                raise NotFound(f"source branch '{source}' does not exist")
            repo.create_remote_branch(tip, name, creds)
            if req.switch:
                st["branch"] = name
                repo.fetch(name, creds)
                self._save(ws["id"], st, bump=True)
        self.ctx.audit.record(principal, "git.branch_created", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"], details={"branch": name, "source": source})
        return {"branch": name, "commit": tip, "source": source, "switched": req.switch}

    def switch_branch(self, principal: Principal, ws_ref: str, req: BranchSwitch) -> dict[str, Any]:
        """Point the workspace at another branch.  The workspace content does not change; ``status`` / ``pull`` then show what differs from that branch
        (the synchronised commit is kept as the common base)."""
        ws = self._ws(principal, ws_ref, "workspace.update")
        st = self._state(ws)
        name = check_ref(req.name)
        with self._lock(ws["id"]):
            repo = self._runner(ws["id"], st["repo_url"])
            repo.init()
            creds = self._creds(principal, ws["id"], st)
            if name not in repo.ls_remote(creds):
                raise NotFound(f"branch '{name}' does not exist in the repository")
            repo.fetch(name, creds)
            st["branch"] = name
            self._save(ws["id"], st, bump=True)
        self.ctx.audit.record(principal, "git.branch_switched", resource_type="workspace", resource_id=ws["id"], workspace_id=ws["id"], details={"branch": name})
        return self.status(principal, ws["id"], fetch=False)

    def health(self) -> dict[str, Any]:
        with self.ctx.db.connect() as conn:
            rows = conn.execute(select(S.workspaces.c.id, S.workspaces.c.git).where(S.workspaces.c.deleted_at.is_(None))).all()
        return {"git_available": bool(git_capabilities()["available"]), "connected_workspaces": sum(1 for r in rows if (r[1] or {}).get("repo_url"))}
