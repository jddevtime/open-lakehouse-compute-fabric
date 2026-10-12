#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Documentation checker: keeps the Markdown honest against the code.  Nothing is written; exit status 1 when any check fails.

    PYTHONPATH=src python scripts/check_docs.py                  # every check
    PYTHONPATH=src python scripts/check_docs.py --only links,cli
    PYTHONPATH=src python scripts/check_docs.py --skip generated  # faster: do not run the three generators

Checks (``--only`` / ``--skip`` take a comma separated list of these names):

``links``      every relative link and image in every Markdown file points at a file or directory that exists, and every ``#anchor`` is a heading
               (GitHub's slug rules, duplicates numbered) or an explicit ``<a id>`` of the target file.  External URLs are not fetched.
``cli``        every ``olcf ...`` command line in a code block or code span names commands that exist in the click tree (``olcf.cli.main``) and,
               for the long options it uses, options that the command or one of its parent groups defines.
``config``     every ``section.key`` named in code spans (and every key in ``toml`` blocks) that belongs to a configuration section or to the runtime
               settings registry exists; every ``OLCF_*`` environment variable named in the docs occurs somewhere in the source tree.
``states``     the mermaid ``stateDiagram`` in ``docs/concepts/compute-model.md`` has exactly the edges of ``olcf.core.states.TRANSITIONS``.
``generated``  ``gen_reference.py``, ``gen_api_docs.py`` and ``gen_sdk_docs.py`` each report ``--check`` clean (the generated pages are current).

Standard library plus the project itself; run it with ``PYTHONPATH=src``.  Author: Justin Dodrill.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import subprocess
import sys
import tempfile
import unicodedata
from collections.abc import Iterable, Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
SKIP_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__", ".pytest_cache", ".ruff_cache", "dist", "build", "vendor", ".mypy_cache"}
#: Markdown that is test input or third-party text, not documentation of this project.
SKIP_PREFIXES = ("tests/corpus/", "src/olcf/web/static/vendor/", "src/olcf/sample/", "src/olcf/notebooks/")
CHECKS = ("links", "cli", "config", "states", "generated")
STATE_DOC = ROOT / "docs" / "concepts" / "compute-model.md"
GENERATORS = ("gen_reference.py", "gen_api_docs.py", "gen_sdk_docs.py")
#: Pages written by the generators; they are checked by running the generators, not by the heuristics below.
GENERATED_PAGES = {f"docs/reference/{n}.md" for n in ("cli", "api", "sdk", "fabric-sdk", "fabric-rules", "configuration", "permissions", "health-checks", "errors", "events", "mcp-tools")}
#: Dotted names that look like settings in prose but are not, each with the reason.
NOT_SETTINGS = {
    "db.dialect": "Python attribute of the Database wrapper",
    "auth.refresh_reuse_detected": "audit action name",
    "network.onelake_dfs": "field of the Fabric administration document, not a platform setting",
    "runtime.context": "attribute of the Fabric notebookutils.runtime object (the notebook context), not a platform setting",
    "server.tls": "quoted as the WRONG name in the defect list of the traceability page (the setting is tls.enabled)",
}
#: Environment-variable-looking names that belong to the reader's own examples.
NOT_ENV = {"OLCF_BP": "a variable the reader chooses for a passphrase (docs/guides/backup-and-restore.md)"}
FILE_SUFFIXES = {"py", "yml", "yaml", "json", "key", "crt", "pem", "out", "toml", "md", "db", "txt", "log", "sh", "ps1", "js", "css", "html", "cfg", "env", "lock", "pub", "csr", "jsonl", "gz", "tpl", "xml", "plist", "service"}


# ----------------------------------------------------------------------------------------------------------------------------------- markdown
def markdown_files() -> list[Path]:
    """Every ``*.md`` below the repository root that is documentation of this project."""
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS)
        for name in sorted(filenames):
            if name.endswith(".md"):
                p = Path(dirpath) / name
                rel = p.relative_to(ROOT).as_posix()
                if not rel.startswith(SKIP_PREFIXES):
                    out.append(p)
    return out


FENCE = re.compile(r"^(\s*)(`{3,}|~{3,})\s*([A-Za-z0-9_+.-]*)")


def split_blocks(text: str) -> list[tuple[str, int, str, str]]:
    """``[(kind, first_line_number, language, text)]`` with kind ``prose`` or ``code`` (fenced blocks)."""
    blocks: list[tuple[str, int, str, str]] = []
    cur: list[str] = []
    kind, lang, start, fence = "prose", "", 1, ""
    for n, line in enumerate(text.splitlines(), 1):
        m = FENCE.match(line)
        if kind == "prose" and m:
            if cur:
                blocks.append(("prose", start, "", "\n".join(cur)))
            kind, lang, start, fence, cur = "code", m.group(3).lower(), n + 1, m.group(2), []
        elif kind == "code" and line.strip().startswith(fence) and line.strip().strip(fence[0]) == "" and len(line.strip()) >= len(fence):
            blocks.append(("code", start, lang, "\n".join(cur)))
            kind, lang, start, fence, cur = "prose", "", n + 1, "", []
        else:
            cur.append(line)
    if cur:
        blocks.append((kind, start, lang, "\n".join(cur)))
    return blocks


CODE_SPAN = re.compile(r"(?<!`)(`+)(?!`)(.+?)(?<!`)\1(?!`)")


def strip_code_spans(line: str) -> str:
    """The line with inline code replaced by spaces (so links inside code are not links)."""
    return CODE_SPAN.sub(lambda m: " " * len(m.group(0)), line)


def slug(heading: str) -> str:
    """GitHub's heading anchor: lower case, drop punctuation, spaces to hyphens."""
    text = re.sub(r"<[^>]+>", "", heading)
    text = re.sub(r"!?\[([^\]]*)\]\([^)]*\)", r"\1", text)  # links keep their text
    text = text.replace("`", "").strip().lower()
    out = []
    for ch in text:
        if ch.isalnum() or ch in "_-":
            out.append(ch)
        elif ch == " ":
            out.append("-")
        elif unicodedata.category(ch).startswith(("L", "N", "M")):
            out.append(ch)
    return "".join(out)


_anchor_cache: dict[Path, set[str]] = {}


def anchors_of(path: Path) -> set[str]:
    """All anchors a Markdown file offers: heading slugs (with ``-1``, ``-2`` for duplicates) and explicit ``id=``/``name=`` attributes."""
    if path in _anchor_cache:
        return _anchor_cache[path]
    seen: dict[str, int] = {}
    found: set[str] = set()
    text = path.read_text(encoding="utf-8")
    for kind, _n, _lang, body in split_blocks(text):
        if kind != "prose":
            continue
        for line in body.splitlines():
            m = re.match(r"^\s{0,3}#{1,6}\s+(.*?)\s*#*\s*$", line)
            if m:
                s = slug(m.group(1))
                k = seen.get(s, 0)
                seen[s] = k + 1
                found.add(s if k == 0 else f"{s}-{k}")
            for a in re.finditer(r"""<a\s+[^>]*?(?:id|name)=["']([^"']+)["']""", line):
                found.add(a.group(1))
    _anchor_cache[path] = found
    return found


LINK = re.compile(r"!?\[(?:[^\]\[]|\[[^\]]*\])*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"]*\")?\s*\)")
REF_DEF = re.compile(r"^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+.*)?$")


def iter_links(text: str) -> Iterator[tuple[int, str]]:
    """``(line number, target)`` of every inline link, image and reference definition outside code."""
    for kind, start, _lang, body in split_blocks(text):
        if kind != "prose":
            continue
        for off, line in enumerate(body.splitlines()):
            n = start + off
            clean = strip_code_spans(line)
            for m in LINK.finditer(clean):
                yield n, m.group(1)
            m2 = REF_DEF.match(clean)
            if m2:
                yield n, m2.group(1)


def check_links(files: list[Path]) -> list[str]:
    """Missing files and missing anchors."""
    errs: list[str] = []
    for f in files:
        rel = f.relative_to(ROOT).as_posix()
        for n, target in iter_links(f.read_text(encoding="utf-8")):
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target) or target.startswith("//"):
                continue  # http(s), mailto, ...
            path_part, _, frag = target.partition("#")
            if path_part.startswith("/"):
                dest = ROOT / path_part.lstrip("/")
            else:
                dest = (f.parent / path_part).resolve() if path_part else f
            path_part = path_part.split("?")[0]
            if path_part and not dest.exists():
                errs.append(f"{rel}:{n}: broken link '{target}' (no such file)")
                continue
            if frag and dest.is_file() and dest.suffix == ".md":
                if frag.lower() not in {a.lower() for a in anchors_of(dest)}:
                    errs.append(f"{rel}:{n}: broken anchor '#{frag}' in '{target}' (headings of {dest.relative_to(ROOT).as_posix()} do not include it)")
    return errs


# ----------------------------------------------------------------------------------------------------------------------------------- cli
def _load(name: str) -> object:
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _click_root():  # noqa: ANN202
    sys.path.insert(0, str(SRC))
    from olcf.cli.main import cli  # type: ignore[attr-defined]

    return cli


PLACEHOLDER = re.compile(r"^[<\[{$(\"'./~\\@*]|[<>{}$*|=\\\"']|\.\.\.|^[A-Z0-9_]+$|^\d")
CMD_WORD = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
OPTION_VALUE_FLAGS_DEFAULT = {"-h", "--help"}


def _opts(cmd) -> tuple[set[str], set[str]]:  # noqa: ANN001
    """(all option strings, those that take a value) of one click command."""
    import click

    allopts, valued = set(), set()
    for p in cmd.params:
        if isinstance(p, click.Option):
            for o in [*p.opts, *p.secondary_opts]:
                allopts.add(o)
                if not p.is_flag and not p.count:
                    valued.add(o)
    return allopts, valued


def check_command_line(tokens: list[str], root) -> str | None:  # noqa: ANN001
    """Validate the tokens after ``olcf``; returns an error text or ``None``."""
    import click

    cmd = root
    all_opts, valued = _opts(cmd)
    all_opts |= OPTION_VALUE_FLAGS_DEFAULT
    path = ["olcf"]
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.startswith("-") and not PLACEHOLDER.search(tok.split("=")[0]):
            name = tok.split("=")[0]
            if name.startswith("--") and name not in all_opts:
                return f"unknown option {name} for '{' '.join(path)}'"
            if "=" not in tok and name in valued:
                i += 1  # the option's value
            i += 1
            continue
        if isinstance(cmd, click.Group):
            if PLACEHOLDER.search(tok) or not CMD_WORD.match(tok):
                return None  # an argument of the group's default behaviour, or prose: stop
            sub = cmd.get_command(click.Context(cmd), tok)
            if sub is None:
                known = ", ".join(cmd.list_commands(click.Context(cmd)))
                return f"'{' '.join(path)}' has no command '{tok}' (known: {known})"
            cmd = sub
            path.append(tok)
            a, v = _opts(cmd)
            all_opts |= a
            valued |= v
            i += 1
            continue
        i += 1  # arguments of a leaf command
    return None


CLI_SEGMENT = re.compile(r"(?:^|[\s;&|(`$])olcf((?:[ \t]+[^\s;&|)`]+)*)")
#: what may stand just before ``olcf`` for it to be the command (not a container name, a namespace or part of a diagram)
COMMAND_POSITION = {"", "$", ">", "&&", "||", ";", "|", "(", "sudo", "--", "time", "watch", "then", "do", "else", "olcf", "`"}


def command_lines(code: str) -> Iterator[list[str]]:
    """Token lists after each ``olcf`` that stands in command position in one line of shell."""
    for m in CLI_SEGMENT.finditer(code):
        before = code[: m.start(1)].rstrip()
        before = before[: -len("olcf")].rstrip() if before.endswith("olcf") else before
        last = before.split()[-1] if before.split() else ""
        toks = m.group(1).split()
        if last in COMMAND_POSITION or last.endswith(("&&", "||", ";", "|", "(", "`")):
            yield toks
        elif toks and toks[0] == "olcf":
            yield toks[1:]  # `docker compose exec olcf olcf runtime install`: the first one is the container


def iter_cli_lines(text: str) -> Iterator[tuple[int, list[str]]]:
    """``(line, tokens after olcf)`` from shell code blocks and inline code spans."""
    for kind, start, lang, body in split_blocks(text):
        if kind == "code":
            if lang not in ("", "sh", "bash", "shell", "console", "zsh", "text", "powershell", "ps1", "cmd"):
                continue
            joined: list[tuple[int, str]] = []
            buf, first = "", 0
            for off, line in enumerate(body.splitlines()):
                if not buf:
                    first = start + off
                stripped = line.rstrip()
                if stripped.endswith("\\"):
                    buf += stripped[:-1] + " "
                    continue
                joined.append((first, buf + line))
                buf = ""
            for n, line in joined:
                s = line.strip()
                if lang in ("", "text", "console") and not re.match(r"^(\$|>|#)?\s*(sudo\s+)?olcf(\s|$)", s) and "olcf " not in s.split("#")[0]:
                    continue
                code = s.split(" #")[0] if " #" in s else s  # trailing comment
                code = re.sub(r"^[$>]\s*", "", code)
                for toks in command_lines(code):
                    yield n, toks
        else:
            for off, line in enumerate(body.splitlines()):
                for m in CODE_SPAN.finditer(line):
                    span = m.group(2)
                    if span.strip().startswith("olcf ") or span.strip() == "olcf":
                        for toks in command_lines(span):
                            yield start + off, toks


def check_cli(files: list[Path]) -> list[str]:
    """Commands and long options in the docs against the click tree."""
    root = _click_root()
    errs: list[str] = []
    for f in files:
        rel = f.relative_to(ROOT).as_posix()
        if rel.startswith("docs/dev/"):
            continue  # engineering notes (contracts, review reports) quote commands that are planned, removed or deliberately NOT exposed; same rule as the `config` check
        for n, toks in iter_cli_lines(f.read_text(encoding="utf-8")):
            err = check_command_line(toks, root)
            if err:
                errs.append(f"{rel}:{n}: `olcf {' '.join(toks)}`: {err}")
    return errs


# ----------------------------------------------------------------------------------------------------------------------------------- config
def known_config() -> tuple[set[str], set[str], set[str]]:
    """(static ``section.key`` names, runtime registry keys, section names) from the code."""
    sys.path.insert(0, str(SRC))
    import logging

    from pydantic import BaseModel

    from olcf.api.app import create_app
    from olcf.core.config import Settings

    logging.disable(logging.CRITICAL)
    static: set[str] = set()
    sections: set[str] = set()
    for name, field in Settings.model_fields.items():
        ann = field.annotation
        if isinstance(ann, type) and issubclass(ann, BaseModel):
            sections.add(name)
            for fname, sub in ann.model_fields.items():
                static.add(f"{name}.{fname}")
                sub_ann = sub.annotation
                if isinstance(sub_ann, type) and issubclass(sub_ann, BaseModel):  # nested table such as [runtime.trust]
                    sections.add(f"{name}.{fname}")
                    for sname in sub_ann.model_fields:
                        static.add(f"{name}.{fname}.{sname}")
    with tempfile.TemporaryDirectory(prefix="olcf-checkdocs-") as tmp:
        app = create_app(Settings(data_dir=Path(tmp)))
        registry = set(app.state.ctx.service("settings").registry)
        try:
            app.state.ctx.db.dispose()
        except Exception:  # noqa: BLE001, S110 - cleanup only
            pass
    return static, registry, sections


def known_non_config_names() -> set[str]:
    """Dotted names that look like settings but are permissions or events (they share prefixes such as ``compute.`` or ``sql.``)."""
    from olcf.security.rbac import PERMISSIONS

    names = set(PERMISSIONS)
    events = ROOT / "docs" / "reference" / "events.md"
    if events.exists():
        names |= set(re.findall(r"^\|\s*`([a-z_]+(?:\.[a-z_]+)+)`", events.read_text(encoding="utf-8"), flags=re.M))
    return names


def env_names_in_tree() -> set[str]:
    """Every ``OLCF_*`` token that occurs in source, scripts, deployment files, SDK, tests and examples."""
    found: set[str] = set()
    pat = re.compile(r"\bOLCF_[A-Z0-9_]+")
    for base in ("src", "scripts", "deploy", "sdk", "tests", "examples", ".github"):
        for dirpath, dirnames, filenames in os.walk(ROOT / base):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for fn in filenames:
                p = Path(dirpath) / fn
                if p.suffix in (".pyc", ".png", ".woff2", ".gz", ".zip", ".whl"):
                    continue
                try:
                    found |= set(pat.findall(p.read_text(encoding="utf-8", errors="ignore")))
                except OSError:
                    continue
    return found


def check_config(files: list[Path]) -> list[str]:
    """Setting keys, TOML blocks and environment variables named in the docs."""
    static, registry, sections = known_config()
    prefixes = sections | {k.split(".")[0] for k in registry}
    other = known_non_config_names()
    env_known = env_names_in_tree()
    key_re = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
    errs: list[str] = []
    for f in files:
        rel = f.relative_to(ROOT).as_posix()
        if rel in GENERATED_PAGES or rel.startswith("docs/dev/"):
            continue  # generated by the scripts (checked by `generated`), or engineering notes outside the documentation set
        text = f.read_text(encoding="utf-8")
        for kind, start, lang, body in split_blocks(text):
            if kind == "code" and lang == "toml":
                section = ""
                for off, line in enumerate(body.splitlines()):
                    s = line.strip()
                    ms = re.match(r"^\[([A-Za-z0-9_.]+)\]$", s)
                    if ms:
                        section = ms.group(1)
                        continue
                    mk = re.match(r"^([A-Za-z0-9_]+)\s*=", s)
                    if mk and section in sections and f"{section}.{mk.group(1)}" not in static:
                        errs.append(f"{rel}:{start + off}: toml key '{section}.{mk.group(1)}' is not a setting (section [{section}] has no '{mk.group(1)}')")
            if kind == "prose":
                for off, line in enumerate(body.splitlines()):
                    for m in CODE_SPAN.finditer(line):
                        span = re.split(r"\s*=\s*", m.group(2).strip())[0].strip()
                        if key_re.match(span) and span.split(".")[0] in prefixes and span not in other and span not in NOT_SETTINGS and span.rsplit(".", 1)[-1] not in FILE_SUFFIXES:
                            if span not in static and span not in registry and not any(r.startswith(span + ".") for r in registry):
                                errs.append(f"{rel}:{start + off}: `{span}` looks like a setting but is neither a process setting nor a runtime setting")
        for n, line in enumerate(text.splitlines(), 1):
            for name in re.findall(r"\bOLCF_[A-Z0-9_]+", line):
                if len(name) <= 5 or name in env_known or name in NOT_ENV:
                    continue
                if name.endswith("_") and any(e.startswith(name) for e in env_known):
                    continue  # a prefix such as OLCF_LIVE_ONELAKE_*
                m = re.match(r"^OLCF_([A-Z0-9_]+?)__([A-Z0-9_]+)$", name)
                if m and f"{m.group(1).lower()}.{m.group(2).lower()}" in static:
                    continue
                if name in {f"OLCF_{k.upper()}" for k in ("data_dir", "environment", "log_level", "log_json")}:
                    continue
                errs.append(f"{rel}:{n}: environment variable {name} does not occur anywhere in the source tree")
    return errs


# ----------------------------------------------------------------------------------------------------------------------------------- states
def check_states() -> list[str]:
    """The mermaid state diagram against ``TRANSITIONS``."""
    sys.path.insert(0, str(SRC))
    from olcf.core.states import TRANSITIONS

    rel = STATE_DOC.relative_to(ROOT).as_posix()
    text = STATE_DOC.read_text(encoding="utf-8")
    blocks = [b for b in split_blocks(text) if b[0] == "code" and b[2] == "mermaid" and "stateDiagram" in b[3]]
    if not blocks:
        return [f"{rel}: no mermaid stateDiagram found"]
    code = TRANSITIONS
    want = {(a.value, b.value) for a, targets in code.items() for b in targets}
    got: set[tuple[str, str]] = set()
    initial: set[str] = set()
    terminal: set[str] = set()
    for _k, _n, _l, body in blocks[:1]:
        for line in body.splitlines():
            m = re.match(r"^\s*(\[\*\]|[A-Za-z_][A-Za-z0-9_]*)\s*-->\s*(\[\*\]|[A-Za-z_][A-Za-z0-9_]*)", line)
            if not m:
                continue
            a, b = m.groups()
            if a == "[*]":
                initial.add(b)
            elif b == "[*]":
                terminal.add(a)
            else:
                got.add((a, b))
    errs = []
    for a, b in sorted(want - got):
        errs.append(f"{rel}: TRANSITIONS allows {a} -> {b} but the diagram has no such edge")
    for a, b in sorted(got - want):
        errs.append(f"{rel}: the diagram has {a} --> {b} but core.states.TRANSITIONS does not allow it")
    terminal_code = {s.value for s, t in code.items() if not t}
    if terminal != terminal_code:
        errs.append(f"{rel}: terminal states in the diagram {sorted(terminal)} differ from the code {sorted(terminal_code)}")
    if initial != {"accepted"}:
        errs.append(f"{rel}: the diagram's initial state is {sorted(initial)}, expected ['accepted']")
    return errs


# ----------------------------------------------------------------------------------------------------------------------------------- generated
def check_generated() -> list[str]:
    """Run each generator with ``--check``."""
    errs: list[str] = []
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SRC) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env.setdefault("OLCF_DATA_DIR", tempfile.mkdtemp(prefix="olcf-checkdocs-gen-"))
    for g in GENERATORS:
        res = subprocess.run([sys.executable, str(ROOT / "scripts" / g), "--check"], capture_output=True, text=True, env=env, timeout=900, check=False)  # noqa: S603
        if res.returncode != 0:
            tail = (res.stdout + res.stderr).strip().splitlines()[-6:]
            errs.append(f"scripts/{g} --check failed (exit {res.returncode}): regenerate with `PYTHONPATH=src python scripts/{g}`\n    " + "\n    ".join(tail))
    return errs


# ----------------------------------------------------------------------------------------------------------------------------------- main
def _csv(value: str) -> set[str]:
    return {v.strip() for v in value.split(",") if v.strip()}


def main(argv: Iterable[str] | None = None) -> int:
    """Run the selected checks and print one line per problem."""
    ap = argparse.ArgumentParser(description="Check the documentation against the code")
    ap.add_argument("--only", default="", help="comma separated subset of: " + ",".join(CHECKS))
    ap.add_argument("--skip", default="", help="comma separated checks to leave out")
    ns = ap.parse_args(list(argv) if argv is not None else None)
    chosen = [c for c in CHECKS if (not ns.only or c in _csv(ns.only)) and c not in _csv(ns.skip)]
    unknown = (_csv(ns.only) | _csv(ns.skip)) - set(CHECKS)
    if unknown:
        print(f"unknown check(s): {', '.join(sorted(unknown))}", file=sys.stderr)
        return 2
    files = markdown_files()
    failures = 0
    for name in chosen:
        if name == "links":
            errs = check_links(files)
        elif name == "cli":
            errs = check_cli(files)
        elif name == "config":
            errs = check_config(files)
        elif name == "states":
            errs = check_states()
        else:
            errs = check_generated()
        status = "ok" if not errs else f"{len(errs)} problem(s)"
        print(f"[{name}] {status} ({len(files)} Markdown files)" if name != "states" and name != "generated" else f"[{name}] {status}")
        for e in errs:
            print("  " + e)
        failures += len(errs)
    if failures:
        print(f"check_docs: {failures} problem(s)", file=sys.stderr)
        return 1
    print("check_docs: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
