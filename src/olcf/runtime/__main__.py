# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``python -m olcf.runtime`` - manage runtimes on this machine without a control plane.

::

    python -m olcf.runtime list                                   # bundled manifests + what is installed here
    python -m olcf.runtime doctor [--runtime olcf-rt-2.0.0]       # host Python / Java / uv discovery for a manifest
    python -m olcf.runtime install olcf-rt-2.0.0 [--with-jdk] [--force] [--bundle-file X.tar.gz]
    python -m olcf.runtime verify olcf-rt-2.0.0 [--deep]          # --deep re-runs the Spark smoke test
    python -m olcf.runtime repair|retire|rollback <id|family> [--purge]
    python -m olcf.runtime export olcf-rt-2.0.0 --out bundle.tar.gz   # offline bundle (needs network or a warm cache)
    python -m olcf.runtime import bundle.tar.gz                   # installs with --no-index; no network used
    python -m olcf.runtime trust                                  # key ids accepted for manifest signatures
    python -m olcf.runtime keygen --out DIR [--name N]            # create an Ed25519 signing key pair
    python -m olcf.runtime sign MANIFEST.json --key N.key         # (re)sign a manifest in place

Common options: ``--state-dir`` (default ``~/.olcf-worker``), ``--index-url``, ``--find-links``, ``--maven-repository`` (repeatable),
``--trust-dir``, ``--json``. Exit code 0 on success, 1 on a runtime-manager error (message + remediation on stderr), 2 on usage errors.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from olcf.core.errors import OlcfError
from olcf.core.logging import configure_logging
from olcf.runtime import hostenv
from olcf.runtime.manager import ManagerConfig, RuntimeManager
from olcf.runtime.manifest import RuntimeManifest
from olcf.runtime.signing import (
    TrustStore,
    generate_keypair,
    key_id_for,
    load_private_key,
    load_public_key,
    sign_manifest_dict,
)


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="python -m olcf.runtime", description="OLCF runtime manager")
    ap.add_argument("--state-dir", default="~/.olcf-worker")
    ap.add_argument("--index-url")
    ap.add_argument("--find-links", action="append")
    ap.add_argument("--maven-repository", action="append")
    ap.add_argument("--trust-dir")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    d = sub.add_parser("doctor")
    d.add_argument("--runtime")
    i = sub.add_parser("install")
    i.add_argument("manifest", help="manifest id or path")
    i.add_argument("--force", action="store_true")
    i.add_argument("--with-jdk", action="store_true", help="download a pinned Temurin JDK (Adoptium, SHA-256 verified) if no suitable Java exists")
    i.add_argument("--uv-python", action="store_true", help="allow `uv python install` for a missing host Python")
    i.add_argument("--bundle-file")
    i.add_argument("--no-activate", action="store_true")
    for name in ("verify", "repair", "retire", "rollback"):
        p = sub.add_parser(name)
        p.add_argument("runtime")
        if name == "verify":
            p.add_argument("--deep", action="store_true")
        if name == "retire":
            p.add_argument("--purge", action="store_true")
    e = sub.add_parser("export")
    e.add_argument("runtime")
    e.add_argument("--out", required=True)
    im = sub.add_parser("import")
    im.add_argument("bundle")
    im.add_argument("--force", action="store_true")
    t = sub.add_parser("trust", help="list / add / remove the public keys trusted to sign runtime manifests (a directory of *.pub files: --trust-dir or $OLCF_RUNTIME_TRUST_DIR)")
    t.add_argument("action", nargs="?", choices=("list", "add", "remove"), default="list")
    t.add_argument("target", nargs="?", help="add: path of a PEM public key (*.pub); remove: key id (or name) to remove")
    t.add_argument("--name", help="add: file name to store the key under (default: the key file's name)")
    k = sub.add_parser("keygen")
    k.add_argument("--out", required=True)
    k.add_argument("--name", default="olcf-signing")
    s = sub.add_parser("sign")
    s.add_argument("manifest")
    s.add_argument("--key", required=True)
    return ap


def _manager(a: argparse.Namespace) -> RuntimeManager:
    cfg = ManagerConfig(state_dir=Path(a.state_dir).expanduser(), trust=TrustStore.from_dir(a.trust_dir) if a.trust_dir else None, maven_repositories=a.maven_repository or None,
                        pip_index_url=a.index_url, pip_find_links=a.find_links or [], with_jdk=getattr(a, "with_jdk", False), allow_uv_python_install=getattr(a, "uv_python", False))
    return RuntimeManager(cfg)


def _trust_dir(a: argparse.Namespace) -> Path | None:
    raw = a.trust_dir or os.environ.get("OLCF_RUNTIME_TRUST_DIR")
    return Path(raw).expanduser() if raw else None


def _trust(a: argparse.Namespace) -> int:
    """``trust list|add|remove``: the operator's trust anchor is a directory of ``*.pub`` files that *replaces* the built-in store (the development key included)."""
    d = _trust_dir(a)
    if a.action == "list":
        store = TrustStore.from_dir(d) if d else TrustStore.default()
        keys = [{"key_id": k, "name": store.get(k).name, "development": store.get(k).dev} for k in store.key_ids()]  # type: ignore[union-attr]
        src = f"directory {d}" if d else "built-in store" + (" (development key trusted: this is not acceptable in production)" if store.trusts_dev_key() else " (no development key)")
        _out(a, {"source": str(d) if d else "built-in", "keys": keys, "key_ids": store.key_ids()},
             f"trusted signing keys ({src}):\n" + ("\n".join(f"  {k['key_id']}  {k['name']}{'  [DEVELOPMENT KEY]' if k['development'] else ''}" for k in keys) or "  (none)"))
        return 0
    if d is None:
        raise OlcfError("no trust directory", remediation="pass --trust-dir DIR (before the sub-command) or set OLCF_RUNTIME_TRUST_DIR; it replaces the built-in store, so the development key stops being trusted")
    if a.action == "add":
        if not a.target:
            raise OlcfError("trust add needs the path of a public key (*.pub)")
        pub_pem = Path(a.target).expanduser().read_bytes()
        kid = key_id_for(load_public_key(pub_pem))  # validates: an Ed25519 public key, not a private one
        name = (a.name or Path(a.target).stem).replace("/", "_")
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{name}.pub").write_bytes(pub_pem)
        _out(a, {"key_id": kid, "file": str(d / f"{name}.pub")}, f"trusting key {kid} ({d / (name + '.pub')})")
        return 0
    if not a.target:
        raise OlcfError("trust remove needs a key id or key name")
    store = TrustStore.from_dir(d)
    victims = [k for k in store.key_ids() if k == a.target or store.get(k).name == a.target]  # type: ignore[union-attr]
    if not victims:
        raise OlcfError(f"no trusted key {a.target!r} in {d}", details={"trusted_key_ids": store.key_ids()})
    for p in sorted(d.glob("*.pub")):
        if key_id_for(load_public_key(p.read_bytes())) in victims:
            p.unlink()
    _out(a, {"removed": victims}, f"removed {', '.join(victims)} from {d}")
    return 0


def _out(a: argparse.Namespace, doc: Any, text: str | None = None) -> None:
    print(json.dumps(doc, indent=2, default=str) if a.json or text is None else text)


def main(argv: list[str] | None = None) -> int:
    a = _parser().parse_args(argv)
    configure_logging("DEBUG" if a.verbose else "WARNING")
    try:
        return _dispatch(a)
    except OlcfError as exc:
        print(f"error: {exc.message}" + (f"\n  -> {exc.remediation}" if getattr(exc, "remediation", None) else ""), file=sys.stderr)
        return 1


def _progress(step: str, detail: str) -> None:
    print(f"  [{step}] {detail}", file=sys.stderr, flush=True)


def _dispatch(a: argparse.Namespace) -> int:
    if a.cmd == "keygen":
        priv, pub = generate_keypair()
        out = Path(a.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{a.name}.key").write_bytes(priv)
        (out / f"{a.name}.key").chmod(0o600)
        (out / f"{a.name}.pub").write_bytes(pub)
        _out(a, {"key_id": key_id_for(load_private_key(priv).public_key()), "private": str(out / f"{a.name}.key"), "public": str(out / f"{a.name}.pub")},
             f"wrote {out / (a.name + '.key')} (keep secret) and {out / (a.name + '.pub')} (distribute to nodes: --trust-dir)")
        return 0
    if a.cmd == "trust":
        return _trust(a)
    if a.cmd == "sign":
        p = Path(a.manifest)
        doc = json.loads(p.read_text())
        doc.pop("signature", None)
        signed = sign_manifest_dict(doc, load_private_key(Path(a.key)))
        RuntimeManifest.from_dict(signed)  # validate before overwriting
        p.write_text(json.dumps(signed, indent=2, sort_keys=True) + "\n")
        _out(a, signed["signature"], f"signed {p} with key {signed['signature']['key_id']}")
        return 0
    mgr = _manager(a)
    if a.cmd == "list":
        avail = [{"id": m.id, "family": m.family, "spark": m.spark_version, "python": m.python_series, "java": m.java_major} for m in mgr.available_manifests()]
        inst = mgr.list()
        text = "available manifests:\n" + "\n".join(f"  {m['id']:<28} {m['family']:<24} spark {m['spark']} python {m['python']} java {m['java']}" for m in avail) + "\ninstalled here:\n"
        text += "\n".join(f"  {r['id']:<28} {r['state']:<10} {'active' if r.get('active') else ''}" for r in inst) if inst else "  (none)"
        _out(a, {"available": avail, "installed": inst}, text)
    elif a.cmd == "doctor":
        ids = [a.runtime] if a.runtime else [m.id for m in mgr.available_manifests()]
        rep: dict[str, Any] = {}
        for rid in ids:
            m = mgr.load_manifest(rid)
            pys = [{"cmd": p.cmd, "version": p.version, "executable": p.executable} if hasattr(p, "cmd") else str(p) for p in hostenv.discover_pythons(m.python_series)]
            jvs = [{"major": j.major, "home": str(j.home)} for j in hostenv.discover_javas()]
            rep[rid] = {"python_series": m.python_series, "pythons_found": pys, "java_supported": m.java_supported, "javas_found": jvs, "uv": __import__("shutil").which("uv")}
        _out(a, rep, "\n".join(f"{rid}: python {v['python_series']} found {len(v['pythons_found'])}, java {v['java_supported']} found {[j['major'] for j in v['javas_found']]}, uv {v['uv']}" for rid, v in rep.items()))
    elif a.cmd == "install":
        res = mgr.import_bundle(a.bundle_file, progress=_progress, force=a.force, activate=not a.no_activate) if a.bundle_file else mgr.install(a.manifest, force=a.force, activate=not a.no_activate, progress=_progress)
        _out(a, res, f"{res['runtime_id']}: {res['state']} (fingerprint {res.get('fingerprint')})")
    elif a.cmd == "verify":
        res = mgr.verify(a.runtime, deep=a.deep, progress=_progress)
        _out(a, res, f"{a.runtime}: verified ({len(res['steps'])} steps)")
    elif a.cmd == "repair":
        res = mgr.repair(a.runtime, progress=_progress)
        _out(a, res, f"{a.runtime}: {res.get('detail') or res['state']}")
    elif a.cmd == "retire":
        res = mgr.retire(a.runtime, purge=a.purge)
        _out(a, res, f"{a.runtime}: retired" + (" and purged" if a.purge else ""))
    elif a.cmd == "rollback":
        res = mgr.rollback(a.runtime)
        _out(a, res, f"active runtime of {res['family']} is now {res['active']} (was {res['previous']})")
    elif a.cmd == "export":
        res = mgr.export_bundle(a.runtime, a.out, progress=_progress)
        _out(a, res, f"wrote {a.out}")
    elif a.cmd == "import":
        res = mgr.import_bundle(a.bundle, progress=_progress, force=a.force)
        _out(a, res, f"{res['runtime_id']}: {res['state']} (from bundle)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
