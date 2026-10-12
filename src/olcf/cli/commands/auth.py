# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Identity commands: ``login``, ``logout``, ``whoami``, ``context``, ``token``, ``user``, ``role``, ``password``.

Credentials live only in the per-user contexts file (mode 0600, or the OS keyring with ``OLCF_USE_KEYRING=1``).  Passwords are never accepted
as plain command-line arguments (they would end up in shell history and process lists): they are prompted, read from stdin
(``--password-stdin``) or taken from ``OLCF_PASSWORD``.
"""

from __future__ import annotations

import os
import sys
from typing import Any

import click

from olcf.cli.common import pass_state, secret_prompt
from olcf.cli.state import State
from olcf.sdk import Client
from olcf.sdk.contexts import ContextStore


# --------------------------------------------------------------------------------------------- login / logout / whoami
@click.command("login", help="Sign in and remember the credentials for this server in a named context.")
@click.option("--name", "ctx_name", default=None, help="Context name (default: derived from the URL).")
@click.option("-u", "--username", default=None, help="User name (prompted when omitted).")
@click.option("--password-stdin", is_flag=True, help="Read the password from stdin.")
@click.option("--token-stdin", is_flag=True, help="Store an API token read from stdin instead of signing in with a password.")
@click.option("--workspace", "default_ws", default=None, help="Default workspace for this context.")
@pass_state
def login_cmd(state: State, ctx_name: str | None, username: str | None, password_stdin: bool, token_stdin: bool, default_ws: str | None) -> None:
    conn = state.resolve_connection()
    url = conn["url"]
    name = ctx_name or state.context or conn.get("context") or _context_name(url)
    c = Client(url, ca_bundle=conn.get("ca_bundle"), timeout=state.timeout)
    store = ContextStore()
    try:
        if token_stdin:
            tok = sys.stdin.readline().strip()
            if not tok:
                raise click.UsageError("no token on stdin")
            c.set_token(tok)
            me = c.identity.me()
            store.set(name, url=url, token=tok, ca_bundle=conn.get("ca_bundle"), username=(me.get("principal") or {}).get("name"), workspace=default_ws)
        else:
            user = username or os.environ.get("OLCF_USERNAME") or click.prompt("Username", default="admin")
            pw = secret_prompt("Password", from_env="OLCF_PASSWORD", stdin=password_stdin)
            doc = c.login(user, pw)
            store.set(name, url=url, token=doc.get("access_token"), refresh_token=doc.get("refresh_token"), ca_bundle=conn.get("ca_bundle"), username=user,
                      workspace=default_ws)
            if doc.get("must_change_password"):
                # the session may only change the password: say so instead of failing on the first real call
                state.out.warn("this account must choose a new password before it can do anything else: run 'olcf password'")
                me = {"name": user, "kind": (doc.get("principal") or {}).get("kind", "user")}
            else:
                me = c.identity.me()
    finally:
        c.close()
    who = me.get("principal") or me
    state.out.emit({"context": name, "url": url, "user": who.get("name"), "kind": who.get("kind")},
                   lambda d: state.out.success(f"signed in to {d['url']} as {d['user']} (context '{d['context']}')"))


def _context_name(url: str) -> str:
    from urllib.parse import urlparse

    u = urlparse(url)
    host = u.hostname or "server"
    return "local" if host in ("127.0.0.1", "localhost", "::1") and (u.port or 8470) == 8470 else (f"{host}-{u.port}" if u.port else host)


@click.command("logout", help="Revoke the session on the server and forget the stored credentials.")
@click.option("--all", "all_ctx", is_flag=True, help="Forget the credentials of every context.")
@pass_state
def logout_cmd(state: State, all_ctx: bool) -> None:
    store = ContextStore()
    names = store.names() if all_ctx else [state.context or store.current or ""]
    done = []
    for n in names:
        ctx = store.get(n) if n else None
        if not ctx:
            continue
        if ctx.get("refresh_token"):
            c = Client(ctx["url"], token=ctx.get("token"), refresh_token=ctx.get("refresh_token"), ca_bundle=ctx.get("ca_bundle"), timeout=min(state.timeout, 10))
            try:
                c.logout()
            except Exception:  # noqa: BLE001 - the server may be down; forgetting locally is what matters
                state.out.warn(f"could not reach {ctx['url']} to revoke the session; the local credentials were removed anyway")
            finally:
                c.close()
        store.clear_credentials(n)
        done.append(n)
    state.out.emit({"logged_out": done}, lambda d: state.out.success("signed out of " + (", ".join(d["logged_out"]) or "nothing (no stored credentials)")))


@click.command("whoami", help="Show who you are on the server: principal, roles and token scope.")
@pass_state
def whoami_cmd(state: State) -> None:
    me = state.client.identity.me()

    def human(m: dict[str, Any]) -> None:
        o = state.out
        p = m.get("principal") or m
        o.kv([("user", p.get("name")), ("kind", p.get("kind")), ("id", p.get("id")), ("display name", p.get("display_name")), ("server", state.client.base_url),
              ("token scope", ", ".join(m.get("token_scopes") or []) or "*")])
        rows = m.get("bindings") or []
        if rows:
            o.info("roles:")
            o.table(rows, [("ROLE", "role"), ("SCOPE", "scope_type"), ("SCOPE ID", "scope_id"), ("VIA", "source")])

    state.out.emit(me, human)


@click.command("password", help="Change your password.")
@click.option("--password-stdin", is_flag=True, help="Read current and new password (two lines) from stdin.")
@pass_state
def password_cmd(state: State, password_stdin: bool) -> None:
    if password_stdin:
        cur, new = sys.stdin.readline().rstrip("\n"), sys.stdin.readline().rstrip("\n")
    else:
        cur = click.prompt("Current password", hide_input=True)
        new = click.prompt("New password", hide_input=True, confirmation_prompt=True)
    state.client.identity.change_password(new, cur)
    state.out.emit({"changed": True}, lambda _d: state.out.success("password changed"))


# --------------------------------------------------------------------------------------------- context
@click.group("context", help="Saved server connections (URL + credentials).")
def context_group() -> None:
    pass


@context_group.command("list", help="List saved contexts.")
@pass_state
def context_list(state: State) -> None:
    store = ContextStore()
    rows = [{"name": n, "current": n == store.current, "url": store.contexts[n].get("url"), "user": store.contexts[n].get("username"),
             "workspace": store.contexts[n].get("workspace"), "signed_in": bool(store.contexts[n].get("token"))} for n in store.names()]
    state.out.table(rows, [("CURRENT", "current"), ("NAME", "name"), ("URL", "url"), ("USER", "user"), ("WORKSPACE", "workspace"), ("SIGNED IN", "signed_in")],
                    empty="no contexts yet - run `olcf login` or `olcf init`")


@context_group.command("use", help="Make a context the default.")
@click.argument("name")
@pass_state
def context_use(state: State, name: str) -> None:
    try:
        ContextStore().use(name)
    except KeyError:
        raise click.ClickException(f"unknown context '{name}' (see `olcf context list`)") from None
    state.out.emit({"current": name}, lambda d: state.out.success(f"now using '{d['current']}'"))


@context_group.command("set", help="Create or change a context (URL, default workspace, CA bundle).")
@click.argument("name")
@click.option("--url")
@click.option("--workspace", "default_ws")
@click.option("--ca-bundle", type=click.Path(dir_okay=False))
@click.option("--use/--no-use", default=True)
@pass_state
def context_set(state: State, name: str, url: str | None, default_ws: str | None, ca_bundle: str | None, use: bool) -> None:
    store = ContextStore()
    if name not in store.contexts and not url:
        raise click.UsageError("a new context needs --url")
    store.set(name, make_current=use, url=url, workspace=default_ws, ca_bundle=ca_bundle)
    state.out.emit({"context": name}, lambda d: state.out.success(f"context '{d['context']}' saved"))


@context_group.command("delete", help="Remove a context and its stored credentials.")
@click.argument("name")
@pass_state
def context_delete(state: State, name: str) -> None:
    if not ContextStore().delete(name):
        raise click.ClickException(f"unknown context '{name}'")
    state.out.emit({"deleted": name}, lambda d: state.out.success(f"deleted '{d['deleted']}'"))


@context_group.command("show", help="Show the effective connection (after flags and environment).")
@pass_state
def context_show(state: State) -> None:
    c = state.resolve_connection()
    c = {k: v for k, v in c.items() if k not in ("token", "refresh_token")} | {"authenticated": bool(c.get("token"))}
    state.out.emit(c, lambda d: state.out.kv(list(d.items())))


# --------------------------------------------------------------------------------------------- tokens
@click.group("token", help="Personal access tokens for scripts and agents.")
def token_group() -> None:
    pass


@token_group.command("list", help="List your tokens (never the secrets).")
@click.option("--all", "all_users", is_flag=True, help="Every user's tokens (administrators).")
@pass_state
def token_list(state: State, all_users: bool) -> None:
    rows = state.client.identity.list_tokens(all=all_users)
    state.out.table(rows, [("ID", "id"), ("NAME", "name", 28), ("SCOPES", "scopes"), ("EXPIRES", "expires_at"), ("LAST USED", "last_used_at")])


@token_group.command("create", help="Create a token.  The secret is shown once.")
@click.argument("name")
@click.option("--scope", "scopes", multiple=True, help="Scope (repeatable): read, write, mcp, mcp:write, fabric, * (default *).")
@click.option("--days", "ttl_days", type=int, default=90, show_default=True, help="Lifetime in days (0 = does not expire).")
@click.option("--for", "principal", default=None, help="Create it for another principal id (administrators).")
@pass_state
def token_create(state: State, name: str, scopes: tuple[str, ...], ttl_days: int, principal: str | None) -> None:
    doc = state.client.identity.create_token(name, scopes=list(scopes) or None, ttl_days=ttl_days or None, principal_id=principal)

    def human(d: dict[str, Any]) -> None:
        state.out.success(f"token '{name}' created (scopes: {', '.join(d.get('scopes') or [])}; expires {d.get('expires_at') or 'never'})")
        print(d["token"], file=state.out.out)
        state.out.info("Store it now - it cannot be shown again.  Use it as OLCF_TOKEN.")

    state.out.emit(doc, human, reveal=("token",))


@token_group.command("revoke", help="Revoke a token immediately.")
@click.argument("token_id")
@pass_state
def token_revoke(state: State, token_id: str) -> None:
    state.client.identity.revoke_token(token_id)
    state.out.emit({"revoked": token_id}, lambda d: state.out.success(f"revoked {d['revoked']}"))


# --------------------------------------------------------------------------------------------- users and roles
@click.group("user", help="Users and service principals (administrators).")
def user_group() -> None:
    pass


@user_group.command("list")
@click.option("--kind", type=click.Choice(["user", "service"]))
@click.option("-q", "--query", "q", default=None)
@pass_state
def user_list(state: State, kind: str | None, q: str | None) -> None:
    c = state.client
    rows = list(c.paginate(c.identity.list_users, kind=kind, q=q))
    state.out.table(rows, [("ID", "id"), ("NAME", "name"), ("KIND", "kind"), ("DISPLAY NAME", "display_name", 28), ("SOURCE", "source"), ("ACTIVE", "is_active"),
                           ("LAST LOGIN", "last_login_at")])


@user_group.command("get")
@click.argument("user")
@pass_state
def user_get(state: State, user: str) -> None:
    state.out.emit(state.client.identity.get_user(_user_id(state, user)), lambda d: state.out.kv(list(d.items())))


def _user_id(state: State, ref: str) -> str:
    if ref.startswith("usr_"):
        return ref
    c = state.client
    for u in c.paginate(c.identity.list_users, q=ref):
        if u.get("name") == ref:
            return u["id"]
    raise click.ClickException(f"no user named '{ref}'")


@user_group.command("create", help="Create a user (or --service principal).  A generated password is shown once.")
@click.argument("name")
@click.option("--service", "is_service", is_flag=True, help="A service principal (token-only, no password).")
@click.option("--display-name")
@click.option("--email")
@click.option("--role", "roles", multiple=True, help="ROLE or ROLE@workspace:NAME-OR-ID; repeatable.")
@click.option("--password-stdin", is_flag=True, help="Read the password from stdin instead of generating one.")
@click.option("--must-change-password", is_flag=True, help="Force the user to choose a new password at first sign-in (recommended when you hand out the generated password).")
@pass_state
def user_create(state: State, name: str, is_service: bool, display_name: str | None, email: str | None, roles: tuple[str, ...], password_stdin: bool,
                must_change_password: bool) -> None:
    generated = False
    pw: str | None = None
    if not is_service:
        if password_stdin:
            pw = sys.stdin.readline().rstrip("\n")
        else:
            from olcf.security.passwords import generate_password

            pw, generated = generate_password(), True
    bindings = [_parse_role(state, r) for r in roles]
    doc = state.client.identity.create_user(name, kind="service" if is_service else "user", password=pw, display_name=display_name, email=email, roles=bindings,
                                          must_change_password=must_change_password)

    def human(d: dict[str, Any]) -> None:
        u = d.get("principal") or d
        state.out.success(f"created {u.get('kind', 'user')} '{u.get('name', name)}' ({u.get('id')})")
        if generated:
            state.out.kv([("password", pw)], indent=2)
            state.out.info("  shown once - pass it to the user over a secure channel")
        elif is_service:
            state.out.info(f"  a service principal signs in with tokens: olcf token create NAME --for {u.get('id')}")

    out_doc = dict(doc) | ({"generated_password": pw} if generated else {})
    state.out.emit(out_doc, human, reveal=("password", "generated_password"))


@user_group.command("set-active", help="Disable (--no-active) or re-enable a user.")
@click.argument("user")
@click.option("--active/--no-active", default=True)
@pass_state
def user_set_active(state: State, user: str, active: bool) -> None:
    doc = state.client.identity.update_user(_user_id(state, user), is_active=active)
    state.out.emit(doc, lambda d: state.out.success(f"{d.get('name', user)} is now {'active' if active else 'disabled'}"))


@user_group.command("reset-password", help="Generate a new password for a user (shown once).")
@click.argument("user")
@pass_state
def user_reset_password(state: State, user: str) -> None:
    doc = state.client.identity.reset_password(_user_id(state, user))
    state.out.emit(doc, lambda d: state.out.kv([("new password", d.get("password") or d.get("generated_password"))]), reveal=("password", "generated_password"))


def _parse_role(state: State, spec: str) -> dict[str, Any]:
    role, _, scope = spec.partition("@")
    if not scope:
        return {"role": role, "scope_type": "org", "scope_id": ""}
    kind, _, ref = scope.partition(":")
    if kind not in ("workspace", "item", "connection", "pool"):
        raise click.BadParameter(f"scope must look like workspace:NAME (got '{scope}')")
    if kind == "workspace" and not ref.startswith("wks_"):
        ref = state.client.workspaces.get(ref)["id"]
    return {"role": role, "scope_type": kind, "scope_id": ref}


@click.group("role", help="Roles and role bindings.")
def role_group() -> None:
    pass


@role_group.command("list", help="Available roles and what they allow.")
@pass_state
def role_list(state: State) -> None:
    doc = state.client.identity.roles()
    rows = doc.get("roles", []) if isinstance(doc, dict) else doc
    for r in rows:
        perms = r.get("permissions") or []
        r["summary"] = "everything" if "*" in perms else f"{len(perms)} permissions"
    state.out.table(rows, [("ROLE", "name"), ("ALLOWS", "summary"), ("DESCRIPTION", "description", 70)])


@role_group.command("bindings", help="Who has which role where.")
@click.option("--user", default=None)
@click.option("--workspace", "ws", default=None)
@pass_state
def role_bindings(state: State, user: str | None, ws: str | None) -> None:
    c = state.client
    kw: dict[str, Any] = {}
    if user:
        kw["principal_id"] = _user_id(state, user)
    if ws:
        kw.update(scope_type="workspace", scope_id=c.workspaces.get(ws)["id"])
    rows = c.identity.list_bindings(**kw)
    state.out.table(rows, [("ID", "id"), ("PRINCIPAL", "principal_id"), ("ROLE", "role"), ("SCOPE", "scope_type"), ("SCOPE ID", "scope_id")])


@role_group.command("grant", help="Give a user a role (org-wide, or on a workspace).")
@click.argument("user")
@click.argument("role")
@click.option("--workspace", "ws", default=None, help="Limit the role to this workspace.")
@pass_state
def role_grant(state: State, user: str, role: str, ws: str | None) -> None:
    spec = _parse_role(state, role + (f"@workspace:{ws}" if ws else ""))
    doc = state.client.identity.bind(_user_id(state, user), spec["role"], scope_type=spec["scope_type"], scope_id=spec["scope_id"])
    state.out.emit(doc, lambda d: state.out.success(f"granted {role} to {user}" + (f" on workspace {ws}" if ws else "")))


@role_group.command("revoke", help="Remove a role binding (id from `olcf role bindings`).")
@click.argument("binding_id")
@pass_state
def role_revoke(state: State, binding_id: str) -> None:
    state.client.identity.unbind(binding_id)
    state.out.emit({"removed": binding_id}, lambda d: state.out.success(f"removed {d['removed']}"))


def register(group: click.Group) -> None:
    for cmd in (login_cmd, logout_cmd, whoami_cmd, password_cmd, context_group, token_group, user_group, role_group):
        group.add_command(cmd)
