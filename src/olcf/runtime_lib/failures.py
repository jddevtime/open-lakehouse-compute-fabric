# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Failure classification for jobs (spec section 33; categories from :class:`olcf.core.errors.FailureCategory`).

Operators filter jobs by *why* they failed. The classifier looks at the exception type name, the message and the
traceback text (which for PySpark contains the JVM exception class names) and maps them to one category with a
remediation hint. The order of the rules matters: specific infrastructure causes (authn/authz/storage/catalog/
dependency/resource) are tested before the generic ``spark`` and ``user-code`` fallbacks.
"""

from __future__ import annotations

import re
from typing import Any

from olcf.core.errors import FailureCategory as FC
from olcf.core.hostruntime import NO_SPARK_REMEDIATION
from olcf.core.redaction import redact_text

_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

# (category, regex over "<ename>: <evalue>\n<traceback>", remediation)
_RULES: list[tuple[FC, re.Pattern[str], str]] = [
    (FC.CANCELLATION, re.compile(r"KeyboardInterrupt|JobCancelled|cancelled by (user|controller)|SIGTERM", re.I), "The job was cancelled."),
    (FC.TIMEOUT, re.compile(r"\bTimeoutError\b|CellTimeout|timed out after|deadline exceeded", re.I), "Increase limits.timeout_seconds or optimise the job."),
    (FC.COMPATIBILITY, re.compile(r"runtime 'olcf-host-python'\), which has no Spark|PySpark is not installed in the Python environment of this job", re.I), NO_SPARK_REMEDIATION),
    (FC.COMPATIBILITY, re.compile(r"\A(Unsupported|CompatibilityBlocked)\b|not supported in OLCF|unsupported Fabric", re.I), "Use the compatibility analyzer report to adapt the notebook."),
    (FC.AUTHN, re.compile(r"InvalidAuthenticationToken|AuthenticationFailed|ExpiredToken|Unauthorized\b|HTTP(?:\D{0,10})401|\b401 Unauthorized|InvalidAccessKeyId|invalid_client|AADSTS\d+", re.I),
     "Credentials are missing or expired: refresh the connection secret / token used by the job."),
    (FC.AUTHZ, re.compile(r"AccessDenied|AuthorizationPermissionMismatch|AuthorizationFailure|\b403\b.{0,40}Forbidden|Forbidden\b|PermissionError|not authorized|SignatureDoesNotMatch", re.I),
     "The identity used by the job lacks permission on the storage/resource: grant read/write access."),
    (FC.RESOURCE, re.compile(r"OutOfMemoryError|java\.lang\.OutOfMemory|MemoryError|Container killed|No space left on device|SparkOutOfMemory|Initial job has not accepted any resources|"
                             r"Could not allocate|Java heap space|GC overhead limit|Resource temporarily unavailable", re.I),
     "Request more driver/executor memory or cores (spark.driver.memory / spark.executor.memory) or reduce data volume."),
    (FC.DEPENDENCY, re.compile(r"ModuleNotFoundError|No module named|ImportError|ClassNotFoundException|NoClassDefFoundError|PackageNotInEnvironment|DependencyBlocked|"
                               r"UnsatisfiedLinkError|has no attribute '__version__'", re.I),
     "Add the missing package/JAR to the job's Environment (declared dependency lock); notebook-time installs are blocked by policy."),
    (FC.CATALOG, re.compile(r"TABLE_OR_VIEW_NOT_FOUND|SCHEMA_NOT_FOUND|NoSuchTableException|NoSuchDatabaseException|NoSuchNamespaceException|Table or view not found|"
                            r"Database '.+' not found|TableAlreadyExistsException|SCHEMA_ALREADY_EXISTS", re.I),
     "Check the table/database name and that the default lakehouse contains the table (Tables/<name> with a _delta_log)."),
    (FC.STORAGE, re.compile(r"FileNotFoundException|NoSuchBucket|NoSuchKey|PATH_NOT_FOUND|Path does not exist|AbfsRestOperationException|AzureBlobFileSystem|S3Exception|"
                            r"AmazonS3Exception|UnsupportedFileSystemException|No FileSystem for scheme|UnknownHostException|java\.net\.(ConnectException|SocketTimeoutException)|"
                            r"DeltaIOException|DeltaFileNotFoundException|IOException: .*(storage|bucket)|\[Errno 2\] No such file", re.I),
     "Check the storage connection (endpoint, credentials), the path, and network access from the worker to the storage account."),
    (FC.SPARK, re.compile(r"Py4JJavaError|Py4JNetworkError|SparkException|PySparkException|org\.apache\.spark\.|Job aborted|Stage \d+ cancelled|"
                          r"ExecutorLostFailure|SparkContext was shut down|Cannot call methods on a stopped SparkContext|Py4JError", re.I),
     "Inspect the Spark UI / executor logs for the failed stage."),
]


def clean(text: str | None) -> str:
    return _ANSI.sub("", text or "")


def classify(ename: str | None, evalue: str | None, traceback_text: str | None = None) -> tuple[str, str | None]:
    """Return ``(category, remediation)`` for an exception description."""
    blob = f"{ename or ''}: {evalue or ''}\n{clean(traceback_text)}"
    for cat, rx, remediation in _RULES:
        if rx.search(blob):
            return cat.value, remediation
    if ename in ("SyntaxError", "IndentationError", "NameError", "TypeError", "ValueError", "KeyError", "AttributeError", "ZeroDivisionError", "AssertionError",
                 "AnalysisException", "ParseException", "IllegalArgumentException", "PythonException", "Exception", "RuntimeError"):
        return FC.USER_CODE.value, "Fix the error in the notebook/script code (see traceback)."
    return FC.USER_CODE.value, "Fix the error in the notebook/script code (see traceback)."


_LINE_PATTERNS = [
    re.compile(r"Cell In\[\d+\],\s*line\s+(\d+)"),
    re.compile(r"<ipython-input-\d+-[0-9a-f]+>\", line (\d+)"),
    re.compile(r"File \"<ipython-input[^\"]*>\", line (\d+)"),
    re.compile(r"\bline (\d+)\b"),
]


def extract_line(traceback_lines: list[str] | str | None) -> int | None:
    """Best-effort 1-based line number *within the failing cell* from an ipykernel traceback."""
    text = clean("\n".join(traceback_lines) if isinstance(traceback_lines, list) else (traceback_lines or ""))
    for pat in _LINE_PATTERNS[:3]:
        found = pat.findall(text)
        if found:
            return int(found[-1])
    # SyntaxError tracebacks name the line only in a generic "line N"
    found = _LINE_PATTERNS[3].findall(text)
    return int(found[-1]) if found else None


def failure_dict(category: str, message: str, *, cell: int | None = None, line: int | None = None, traceback: str | None = None,
                 remediation: str | None = None, details: dict[str, Any] | None = None) -> dict[str, Any]:
    """FailureInfo-shaped dict (matches :class:`olcf.core.models.FailureInfo`), always redacted."""
    return {"category": category, "message": redact_text(message)[:4000], "cell": cell, "line": line,
            "traceback": redact_text(clean(traceback))[-20000:] if traceback else None, "remediation": remediation, "details": details or {}}


def failure_from_exception(exc: BaseException, *, cell: int | None = None, line: int | None = None, tb_text: str | None = None) -> dict[str, Any]:
    """Classify a Python exception raised in-process (script / sql / spark_job kinds)."""
    import traceback as tbm

    text = tb_text if tb_text is not None else "".join(tbm.format_exception(type(exc), exc, exc.__traceback__))
    forced = getattr(exc, "olcf_category", None)
    if forced:
        cat, remediation = str(forced), getattr(exc, "olcf_remediation", None)
    else:
        cat, remediation = classify(type(exc).__name__, str(exc), text)
    return failure_dict(cat, f"{type(exc).__name__}: {exc}", cell=cell, line=line, traceback=text, remediation=remediation)
