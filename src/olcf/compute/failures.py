# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Failure classification and the deterministic retry rule (pure functions, no I/O).

Classification
--------------
The runner (NODE) normally reports ``failure.category`` itself. When it does not (or reports something unknown, or the
process was killed before it could write ``result.json``), :func:`classify_failure` derives a
:class:`~olcf.core.errors.FailureCategory` from the exit code and the text of the error / log tail. Rules are ordered, first match wins,
and each rule maps to a remediation hint shown to the user.

Retry (CONTRACTS 4.2 "replay / duplicate protection")
-----------------------------------------------------
:func:`retry_decision` decides whether a failed attempt is replayed. A job is replayed only when **all** hold:

1. attempts are left (``attempt < max_attempts``);
2. the failure category is *retryable* (infrastructure or engine trouble, never user code, authorisation, compatibility, timeouts
   or cancellation);
3. replaying is *safe*: the job is declared ``policy.idempotent`` **or** the attempt never reached ``running`` (so it cannot have
   produced side effects); and when an attempt was running and the controller lost contact with it (``infrastructure loss``), the
   agent must have **confirmed that the process is gone** (``confirmed_gone``), otherwise two copies could run at once.

The back-off doubles per attempt (``retry_backoff_seconds * 2**(attempt-1)``, capped at one hour) so the schedule is deterministic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from olcf.core.errors import FailureCategory

RETRYABLE_CATEGORIES = frozenset({
    FailureCategory.PROVIDER, FailureCategory.RESOURCE, FailureCategory.SPARK, FailureCategory.STORAGE, FailureCategory.CATALOG, FailureCategory.INTERNAL,
})
MAX_BACKOFF_SECONDS = 3600

#: (pattern, category, remediation) - evaluated in order against ``message + traceback + stderr tail``.
_RULES: tuple[tuple[re.Pattern[str], FailureCategory, str], ...] = tuple(
    (re.compile(p, re.I | re.S), c, r) for p, c, r in (
        (r"compatibilityblocked|unsupported in olcf|not supported by the olcf runtime", FailureCategory.COMPATIBILITY, "see the compatibility report for the blocking constructs"),
        (r"outofmemoryerror|java heap space|gc overhead limit|memoryerror|container killed|exceeded memory limit|oom[- ]?kill|no space left on device|cannot allocate memory",
         FailureCategory.RESOURCE, "request more driver/executor memory or reduce the data per task"),
        (r"\b401\b|unauthori[sz]ed|invalid (credentials|token)|token (has )?expired|authentication failed|invalid_client|aadsts\d+", FailureCategory.AUTHN,
         "check the connection credentials / token lifetime"),
        (r"accessdenied|access denied|\b403\b|forbidden|permission denied|not authori[sz]ed to|authorizationpermissionmismatch|insufficient privileges",
         FailureCategory.AUTHZ, "grant the required role or storage permission"),
        (r"modulenotfounderror|importerror|no module named|classnotfoundexception|noclassdeffounderror|no matching distribution|could not find a version that satisfies",
         FailureCategory.DEPENDENCY, "add the missing package to the environment and rebuild its lock"),
        (r"nosuchbucket|nosuchkey|filenotfoundexception|no such file or directory|unknownhostexception|connection (refused|reset|timed out)|connecttimeout|"
         r"sockettimeoutexception|ioexception|abfss?://.*(error|failed)|s3a?://.*(error|failed)|storage account|bucket does not exist|path does not exist",
         FailureCategory.STORAGE, "check the storage connection and that the path exists"),
        (r"table or view not found|tableorviewnotfound|schema .* not found|database .* not found|catalog .* not found|nosuchtableexception|nosuchdatabase",
         FailureCategory.CATALOG, "check the table/database name and the lakehouse the job is attached to"),
        (r"executorlostfailure|executor lost|master removed our application|stage \d+ cancelled|sparkcontext was shut down|py4jnetworkerror|"
         r"lost executor|fetchfailed|shuffle.*(failed|corrupt)|org\.apache\.spark\.sparkexception: job aborted due to stage failure",
         FailureCategory.SPARK, "inspect the Spark UI for the failed stage; executor loss is often memory or node trouble"),
        (r"timed? ?out after|deadline exceeded|timeouterror", FailureCategory.TIMEOUT, "increase policy.timeout_seconds or optimise the job"),
        (r"keyboardinterrupt|cancelled by|cancellation requested", FailureCategory.CANCELLATION, ""),
    )
)

_KNOWN = {c.value for c in FailureCategory}


@dataclass(frozen=True)
class Classified:
    """The result of classifying a failure: its category, a message, a remediation and where the decision came from (reported, rule, exit code or default).
    """
    category: str
    message: str
    remediation: str | None = None
    source: str = "rule"  # reported | rule | exit-code | default

    def as_failure(self, base: dict[str, Any] | None = None) -> dict[str, Any]:
        out = dict(base or {})
        out.update({"category": self.category, "message": out.get("message") or self.message})
        if self.remediation and not out.get("remediation"):
            out["remediation"] = self.remediation
        out.setdefault("classified_by", self.source)
        return out


def classify_failure(failure: dict[str, Any] | None, *, exit_code: int | None = None, log_tail: str = "", default: FailureCategory = FailureCategory.USER_CODE) -> Classified:
    """Category for a failed attempt. A valid category reported by the runner always wins; ``internal`` from the runner is refined by the rules."""
    failure = failure or {}
    reported = str(failure.get("category") or "")
    msg = str(failure.get("message") or "")
    if reported in _KNOWN and reported != FailureCategory.INTERNAL.value:
        return Classified(reported, msg or reported, failure.get("remediation"), "reported")
    text = "\n".join(x for x in (msg, str(failure.get("traceback") or ""), log_tail[-8000:]) if x)
    for pat, cat, rem in _RULES:
        if pat.search(text):
            return Classified(cat.value, msg or f"{cat.value} failure", rem or None, "rule")
    if exit_code is not None and exit_code in (137, -9):
        return Classified(FailureCategory.RESOURCE.value, msg or "the process was killed (SIGKILL), most likely out of memory", "request more memory", "exit-code")
    if exit_code is not None and exit_code in (143, -15):
        return Classified(FailureCategory.CANCELLATION.value, msg or "the process was terminated (SIGTERM)", None, "exit-code")
    if reported == FailureCategory.INTERNAL.value:
        return Classified(reported, msg or "internal error", failure.get("remediation"), "reported")
    return Classified(default.value, msg or (f"the job exited with code {exit_code}" if exit_code is not None else "the job failed"), failure.get("remediation"), "default")


@dataclass(frozen=True)
class RetryDecision:
    """Whether a failed job is retried automatically, why, and after how long."""
    retry: bool
    reason: str
    backoff_seconds: int = 0


def backoff_for(attempt: int, base_seconds: int) -> int:
    return int(min(MAX_BACKOFF_SECONDS, max(0, base_seconds) * (2 ** max(0, attempt - 1))))


def retry_decision(*, attempt: int, max_attempts: int, category: str, idempotent: bool, reached_running: bool, infrastructure_loss: bool,
                   confirmed_gone: bool, backoff_base_seconds: int = 30) -> RetryDecision:
    """Pure replay rule (see module docstring)."""
    if attempt >= max_attempts:
        return RetryDecision(False, f"no attempts left ({attempt}/{max_attempts})")
    try:
        cat = FailureCategory(category)
    except ValueError:
        cat = FailureCategory.INTERNAL
    if cat not in RETRYABLE_CATEGORIES:
        return RetryDecision(False, f"failures of category '{cat.value}' are never retried")
    if reached_running and not idempotent:
        return RetryDecision(False, "the job is not declared idempotent (policy.idempotent) and had started running; replaying could repeat side effects")
    if infrastructure_loss and reached_running and not confirmed_gone:
        return RetryDecision(False, "the agent has not confirmed that the previous process is gone; replaying could run two copies")
    return RetryDecision(True, f"retrying (attempt {attempt + 1}/{max_attempts})", backoff_for(attempt, backoff_base_seconds))


__all__ = ["Classified", "RETRYABLE_CATEGORIES", "RetryDecision", "backoff_for", "classify_failure", "retry_decision"]
