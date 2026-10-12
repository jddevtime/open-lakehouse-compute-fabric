# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Secret / token redaction (spec 23, 28, 31).

Logs, error traces, telemetry and support bundles must pass through :func:`redact_text`
or :func:`redact_obj`. Redaction is conservative: it removes anything that looks like a
bearer token, JWT, platform token, storage key, password assignment or private key.
"""

from __future__ import annotations

import logging
import re
from typing import Any

REDACTED = "***REDACTED***"

_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]{8,}"), r"\1" + REDACTED),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{4,}"), REDACTED),  # JWT
    (re.compile(r"\bolcf_(?:pat|wrk|enr|hook)_[A-Za-z0-9_-]{8,}"), REDACTED),  # platform tokens
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S), REDACTED),
    (re.compile(r"(?i)\b(AccountKey|SharedAccessSignature|sig)=[A-Za-z0-9%+/=_-]{8,}"), r"\1=" + REDACTED),
    (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), REDACTED),  # AWS access key id
    (
        re.compile(
            r"(?i)\b(aws_secret_access_key|client_secret|password|passwd|secret|api[_-]?key|token|authorization)"
            r"(\s*[:=]\s*)(\"[^\"]*\"|'[^']*'|[^\s,;&\"']+)"
        ),
        r"\1\2" + REDACTED,
    ),
    (re.compile(r"(?i)(://[^/\s:@]+:)[^@\s/]+(@)"), r"\1" + REDACTED + r"\2"),  # credentials in URLs
]

_SENSITIVE_KEYS = re.compile(
    r"(?i)(password|passwd|secret|token|api[_-]?key|authorization|credential|private[_-]?key|account[_-]?key|sas|signature|client_secret|cookie)"
)
# keys that merely *reference* a secret are safe to show. ``signature_header`` is the NAME of the HTTP header that carries a webhook signature (trigger config), not the signature.
_SAFE_KEYS = re.compile(r"(?i)(secret_id|secret_ref|secret_name|token_id|token_type|token_ttl|tokens?_count|has_secret|secret_provider|credential_type|credential_id|signature_header)")


def redact_text(text: str) -> str:
    if not text:
        return text
    out = text
    for pat, repl in _RULES:
        out = pat.sub(repl, out)
    return out


def redact_obj(obj: Any, _depth: int = 0) -> Any:
    """Deep-copy ``obj`` with sensitive values masked (dict keys and string contents)."""
    if _depth > 12:
        return obj
    if isinstance(obj, dict):
        out: dict[Any, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if _SENSITIVE_KEYS.search(ks) and not _SAFE_KEYS.search(ks) and v not in (None, "", False):
                out[k] = REDACTED
            else:
                out[k] = redact_obj(v, _depth + 1)
        return out
    if isinstance(obj, (list, tuple)):
        return [redact_obj(v, _depth + 1) for v in obj]
    if isinstance(obj, str):
        return redact_text(obj)
    return obj


class RedactingFilter(logging.Filter):
    """Logging filter that scrubs the formatted message and exception text."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:  # pragma: no cover
            return True
        record.msg = redact_text(msg)
        record.args = ()
        if record.exc_info and record.exc_info[1] is not None and not record.exc_text:
            import traceback

            record.exc_text = redact_text("".join(traceback.format_exception(*record.exc_info)))
            record.exc_info = None
        return True
