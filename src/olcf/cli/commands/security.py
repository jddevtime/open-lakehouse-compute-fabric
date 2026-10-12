# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf security``: credential lifecycle - key rings and staged rotation of the master key and the JWT signing key.

``olcf security keys``                                  key ids, stages, ages, usage (never key material)
``olcf security rotate master-key [--action ...]``      start (default) | reencrypt | drop-old | rollback | rewrap
``olcf security rotate jwt-signing-key [--action ...]`` start (default) | drop-old | rollback | rewrap

Needs the SecurityAdmin (or PlatformAdmin) role.  The server does the work; every control-plane node follows a rotation without a restart.
"""

from __future__ import annotations

import time
from typing import Any

import click

from olcf.cli.common import confirm, pass_state, yes_option
from olcf.cli.state import State
from olcf.sdk.errors import OlcfTimeoutError

TERMINAL = {"succeeded", "failed", "cancelled"}


def _wait_operation(state: State, op_id: str, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = ""
    while True:
        op = state.client.admin.operation(op_id)
        line = f"{op.get('progress', 0):>3}% {op.get('message', '')}"
        if line != last and state.fmt == "table":
            state.out.progress("re-encryption", line)
            last = line
        if op.get("state") in TERMINAL:
            return op
        if time.monotonic() >= deadline:
            raise OlcfTimeoutError(f"operation {op_id} still {op.get('state')} after {timeout:g}s (it keeps running on the server)", last=op)
        time.sleep(0.5)


def _short(keys: list[dict[str, Any]], kid: str) -> str:
    return next((f"{k['kid']} ({k['stage']})" for k in keys if k["kid"] == kid), kid)


def _print_section(state: State, title: str, d: dict[str, Any]) -> None:
    state.out.info(state.out.style(title, bold=True))
    src = d.get("root_source", "?")
    managed = "externally managed root" if d.get("externally_managed") else "managed through the key ring"
    state.out.info(f"  primary {d['primary']}   ring version {d.get('ring_version', 0)}   source {src}   ({managed})")
    rows = [{"kid": k["kid"], "stage": k["stage"], "source": k["source"], "age": "-" if k.get("age_days") is None else f"{k['age_days']:.1f} d",
             "secrets": k.get("secrets", "-"), "available": k.get("available", True)} for k in d["keys"]]
    state.out.table(rows, [("KEY", "kid", 10), ("STAGE", "stage", 10), ("SOURCE", "source", 10), ("AGE", "age", 8), ("SECRETS", "secrets", 8), ("USABLE", "available", 7)])
    if d.get("secrets"):
        s = d["secrets"]
        state.out.info(f"  secrets: {s['total']} total, {s['pending_reencryption']} not yet under the primary key, {s['external_provider']} in an external provider")
    if d.get("reencryption"):
        r = d["reencryption"]
        stale = " (stale: the node stopped; re-run it)" if r.get("stale") else ""
        state.out.info(f"  last re-encryption: {r.get('state')}{stale} {r.get('done', 0)} done, {r.get('failed', 0)} failed  [{r.get('operation_id', '-')}]")
    if d.get("drop_old_eligible_at"):
        state.out.info(f"  previous signing key may be dropped after {d['drop_old_eligible_at']} (longest token lifetime {d['max_token_lifetime_seconds']} s)")
    for b in d.get("drop_old_blockers") or []:
        state.out.warn(f"drop-old blocked: {b}")
    for e in d.get("errors") or []:
        state.out.warn(e)


@click.group("security", help="Credential lifecycle: key rings and staged rotation of the master key and the JWT signing key.")
def security_group() -> None:
    pass


@security_group.command("keys", help="Show key ids, stages, ages and usage of the master key and the JWT signing key (never key material).")
@pass_state
def keys_cmd(state: State) -> None:
    doc = state.client.security.keys()

    def human(d: dict[str, Any]) -> None:
        _print_section(state, "Master key (encrypts stored secrets)", d["master_key"])
        state.out.info("")
        _print_section(state, "JWT signing key (signs login tokens)", d["jwt_signing_key"])
        state.out.info(f"\n  every node adopts a rotation within {d['refresh_interval_seconds']:g} s")

    state.out.emit(doc, human)


@security_group.group("rotate", help="Rotate a key in stages: start, (re-encrypt), drop-old; roll back before drop-old.")
def rotate_group() -> None:
    pass


def _run(state: State, kind: str, action: str, *, reencrypt: bool, wait: bool, force: bool, to: str | None, yes: bool, timeout: float) -> None:
    if action in ("drop-old", "rollback") or force:
        confirm(state, f"{action} on the {kind}" + (" (forced: signs users out)" if force else "") + "?", yes)
    c = state.client.security
    res = c.rotate(kind, action, reencrypt=reencrypt if kind == "master-key" and action in ("start", "rollback") else None, force=force or None, to=to)
    op_id = res.get("operation_id")
    if op_id and wait:
        op = _wait_operation(state, op_id, timeout)
        res["operation"] = op
        if op.get("state") != "succeeded":
            err = op.get("error") or {}
            state.out.emit(res, lambda d: None)
            state.out.error(err.get("message") or op.get("message") or "re-encryption failed", err.get("remediation"))
            raise SystemExit(1)

    def human(d: dict[str, Any]) -> None:
        if d.get("new_key"):
            state.out.success(f"{kind}: new primary key {d['new_key']} (previous {d.get('previous_key') or '-'} stays as overlap)")
            if d.get("propagation"):
                state.out.info(f"  {d['propagation']}")
        elif d.get("dropped") is not None:
            state.out.success(f"{kind}: dropped {', '.join(d['dropped']) or 'nothing'}; primary {d['primary']}" + ("  (forced)" if d.get("forced") else ""))
            if d.get("warning"):
                state.out.warn(d["warning"])
        elif d.get("action") == "rollback":
            state.out.success(f"{kind}: key {d['primary']} is primary again (demoted {d.get('demoted') or '-'})")
        elif d.get("rewrapped") is not None:
            state.out.success(f"re-wrapped the key rings under root key {d['root_key']}: {d['rewrapped']}")
        op = d.get("operation")
        if op:
            r = op.get("result") or {}
            state.out.success(f"re-encryption {op.get('state')}: {r.get('reencrypted', 0)} secret(s) re-sealed, {r.get('remaining', 0)} remaining")
        elif op_id:
            state.out.info(f"re-encryption running as operation {op_id} (`olcf security keys` shows progress; re-run the command to resume after a failure)")

    state.out.emit(res, human)


def _common(f: Any) -> Any:
    f = click.option("--timeout", type=float, default=900.0, show_default=True, help="Seconds to wait for the re-encryption operation.")(f)
    f = yes_option(f)
    f = click.option("--wait/--no-wait", default=True, show_default=True, help="Follow the re-encryption operation until it finishes.")(f)
    return f


@rotate_group.command("master-key", help="Rotate the master key: new primary key, previous key kept for decryption, secrets re-encrypted in the background.")
@click.option("--action", type=click.Choice(["start", "reencrypt", "drop-old", "rollback", "rewrap"]), default="start", show_default=True,
              help="start: new key + re-encryption. reencrypt: resume it. drop-old: remove old keys (refused while secrets use them). rollback: previous key primary again. rewrap: after a root-key change.")
@click.option("--no-reencrypt", is_flag=True, help="start/rollback: only switch keys; run --action reencrypt later.")
@click.option("--to", default=None, help="rollback: key id to make primary (default: the previous one).")
@_common
@pass_state
def rotate_master(state: State, action: str, no_reencrypt: bool, to: str | None, wait: bool, yes: bool, timeout: float) -> None:
    _run(state, "master-key", action, reencrypt=not no_reencrypt, wait=wait, force=False, to=to, yes=yes, timeout=timeout)


@rotate_group.command("jwt-signing-key", help="Rotate the JWT signing key: new tokens use the new key, the previous one keeps verifying until dropped.")
@click.option("--action", type=click.Choice(["start", "drop-old", "rollback", "rewrap"]), default="start", show_default=True)
@click.option("--force", is_flag=True, help="drop-old: do not wait for the longest token lifetime (signs users out).")
@click.option("--to", default=None, help="rollback: key id to make primary (default: the previous one).")
@_common
@pass_state
def rotate_jwt(state: State, action: str, force: bool, to: str | None, wait: bool, yes: bool, timeout: float) -> None:
    _run(state, "jwt-signing-key", action, reencrypt=False, wait=wait, force=force, to=to, yes=yes, timeout=timeout)


def register(group: click.Group) -> None:
    group.add_command(security_group)
