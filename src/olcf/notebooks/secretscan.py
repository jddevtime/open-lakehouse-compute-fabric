# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Secret-leak scanner used on every outgoing Git commit and on variable values.

The scanner never returns the secret itself: a :class:`Finding` carries the rule name, file, line and a *redacted* preview (the first characters only for
long tokens).  Detection is rule based (well-known token formats plus credential-looking assignments); a line that contains ``olcf: allow-secret`` is
skipped (for documentation snippets, test fixtures and intentional dummy values), and obvious references / placeholders (``sec_...``, ``${VAR}``, ``<token>``,
``***REDACTED***``, ``changeme``, ``env:NAME``) are not findings.

Notebooks are decoded first (cell sources, text outputs and metadata strings are scanned as text), because secret strings inside JSON are escaped and a plain
regular expression over the raw file would miss them.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

ALLOW_MARKER = "olcf: allow-secret"
MAX_SCAN_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class Finding:
    """A credential-like string found in a notebook: the rule, line, a masked preview and the path."""
    rule: str
    line: int
    preview: str
    path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"rule": self.rule, "path": self.path, "line": self.line, "preview": self.preview}


_PLACEHOLDER = re.compile(
    r"(?i)^(?:<[^>]*>|\$\{[^}]*\}|\$[A-Za-z_][A-Za-z0-9_]*|\{\{.*\}\}|%[A-Za-z_]+%|\*{3,}.*|x{4,}|\.{3,}|changeme|change-me|password|secret|token|none|null|true|false|"
    r"sec_[A-Za-z0-9]{10,40}|env:[A-Za-z_][A-Za-z0-9_]*|redacted|\*\*\*redacted\*\*\*|your[-_ ].*|example.*|dummy.*|placeholder.*|todo|fixme)$")

# (rule name, compiled pattern, group holding the secret or 0)
_RULES: list[tuple[str, re.Pattern[str], int]] = [
    ("private-key", re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP |ENCRYPTED )?PRIVATE KEY(?: BLOCK)?-----"), 0),
    ("aws-access-key-id", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), 0),
    ("aws-secret-access-key", re.compile(r"(?i)aws_?secret_?access_?key[\"']?\s*[:=]\s*[\"']?([A-Za-z0-9/+=]{40})\b"), 1),
    ("azure-storage-account-key", re.compile(r"(?i)AccountKey=([A-Za-z0-9+/=]{40,})"), 1),
    ("azure-sas-token", re.compile(r"(?i)[?&]sig=([A-Za-z0-9%+/=]{30,})(?:&|$|\s|\"|')"), 1),
    ("github-token", re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{50,})\b"), 0),
    ("slack-token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"), 0),
    ("google-api-key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"), 0),
    ("stripe-key", re.compile(r"\b(?:sk|rk)_live_[0-9A-Za-z]{20,}\b"), 0),
    ("npm-token", re.compile(r"\bnpm_[A-Za-z0-9]{36}\b"), 0),
    ("pypi-token", re.compile(r"\bpypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{20,}"), 0),
    ("olcf-token", re.compile(r"\bolcf_(?:pat|wrk|enr)_[A-Za-z0-9_-]{8,}"), 0),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{10,}"), 0),
    ("url-credentials", re.compile(r"://[^/\s:@\"']{1,64}:([^@\s/\"']{3,})@[A-Za-z0-9.-]+"), 1),
    ("bearer-token", re.compile(r"(?i)\bbearer\s+([A-Za-z0-9._~+/=-]{20,})"), 1),
    ("credential-assignment", re.compile(
        r"(?i)\b[A-Za-z0-9_.-]*(?:password|passwd|pwd|secret|api[_-]?key|access[_-]?key|auth[_-]?token|client[_-]?secret|private[_-]?key|token)[A-Za-z0-9_.-]*[\"']?\s*[:=]\s*"
        r"(?:\"([^\"\n]{8,})\"|'([^'\n]{8,})'|([^\s\"',;(\[{)\]}]{8,}))"), -1),
]


def _preview(secret: str) -> str:
    s = secret.strip()
    if len(s) <= 8:
        return "***"
    return s[:4] + "***"


def _is_placeholder(value: str) -> bool:
    v = value.strip().strip("\"'")
    if not v or _PLACEHOLDER.match(v):
        return True
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\(.*\)", v):  # a function call such as os.environ.get(...) / getpass()
        return True
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*(\[[^\]]*\])?", v) and not re.search(r"\d", v) and len(set(v)) < 20:  # an identifier / attribute access, not a literal
        return True
    return False


def scan_text(text: str, path: str = "") -> list[Finding]:
    """Findings in ``text`` (redacted).  Lines containing ``olcf: allow-secret`` are skipped."""
    out: list[Finding] = []
    if not text:
        return out
    for n, line in enumerate(text.split("\n"), start=1):
        if len(line) > 20000:
            line = line[:20000]
        if ALLOW_MARKER in line:
            continue
        seen_spans: list[tuple[int, int]] = []
        for rule, rx, grp in _RULES:
            for m in rx.finditer(line):
                if grp == -1:
                    secret = next((g for g in m.groups() if g), "")
                    span = m.span()
                elif grp == 0:
                    secret, span = m.group(0), m.span()
                else:
                    secret, span = m.group(grp) or "", m.span(grp)
                if not secret or (grp != 0 and _is_placeholder(secret)):
                    continue
                if grp == -1:
                    # an unquoted value followed by '(' / '[' / '.' is code (a call, an index, an attribute), and pure numbers are not credentials
                    nxt = line[m.end():m.end() + 1]
                    quoted = m.group(1) is not None or m.group(2) is not None
                    if not quoted and (nxt in ("(", "[", ".") or secret.isdigit() or "." in secret or re.fullmatch(r"[a-z_]+|[A-Z_]+", secret)):
                        continue
                if any(s <= span[0] < e or s < span[1] <= e for s, e in seen_spans):
                    continue  # already reported by a more specific rule
                seen_spans.append(span)
                out.append(Finding(rule=rule, line=n, preview=_preview(secret), path=path))
    return out


def _strings(obj: Any, depth: int = 0) -> list[str]:
    if depth > 20:
        return []
    if isinstance(obj, str):
        return [obj]
    if isinstance(obj, list):
        return [s for x in obj for s in _strings(x, depth + 1)]
    if isinstance(obj, dict):
        return [s for k, v in obj.items() for s in ([k] if isinstance(k, str) else []) + _strings(v, depth + 1)]
    return []


def scan_bytes(path: str, data: bytes) -> list[Finding]:
    """Scan a file's content.  Binary files and files above 8 MiB are skipped (reported by the caller as skipped, not as clean)."""
    if len(data) > MAX_SCAN_BYTES or b"\x00" in data[:4096]:
        return []
    text = data.decode("utf-8", errors="replace")
    findings = scan_text(text, path)
    if path.lower().endswith((".ipynb", ".json")):
        try:
            doc = json.loads(text)
        except ValueError:
            return findings
        decoded = "\n".join(_strings(doc))
        have = {(f.rule, f.preview) for f in findings}
        for f in scan_text(decoded, path):
            if (f.rule, f.preview) not in have:
                findings.append(Finding(f.rule, 0, f.preview, path))
    return findings


def scan_files(files: dict[str, bytes]) -> list[Finding]:
    out: list[Finding] = []
    for p in sorted(files):
        out.extend(scan_bytes(p, files[p]))
    return out
