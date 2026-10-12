# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The connector contract of OLCF Intelligence discovery.

A *connector* knows one kind of source and turns it into the plain records of :mod:`olcf.intelligence.model`:

* ``discover(ctx)`` is a generator of **batches** - :class:`~olcf.intelligence.model.Facts` (assets, edges, gaps) and
  :class:`~olcf.intelligence.model.DefinitionFact` (a stored definition for the lineage analyzers). Batches are produced one small unit at a time (one lakehouse, one Fabric
  workspace, one scanner batch of at most 100 workspaces) so a large tenant never has to fit in memory, and the scan runner commits each batch in its own transaction, which
  is what keeps the graph consistent if the process dies half way.
* ``test(config, secrets)`` is the "Test connectivity" of the source: an **ordered list of steps** ``{name, ok, detail, remediation, duration_ms}``. It stops at the first
  failing step and reports the remaining ones as ``skipped`` (so a person sees which step failed *and* what was never tried).
* Connectors never write to the database or the graph. The scan runner (:mod:`olcf.intelligence.discovery.scans`) is the only writer.

New kinds are added with :func:`register_connector` (other packages may register their own at import / install time).
"""

from __future__ import annotations

import collections
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from olcf.core.redaction import redact_text
from olcf.intelligence.model import DefinitionFact, Facts, Gap

#: A unit produced by ``Connector.discover``.
Batch = Facts | DefinitionFact


# --------------------------------------------------------------------------------------------- steps
@dataclass
class Step:
    """One step of a connectivity test. ``ok`` is false for a failed step and for a step that was ``skipped`` because an earlier one failed;
    an optional step that does not apply (for example no sampling identity configured) is ``ok`` *and* ``skipped``."""

    name: str
    ok: bool
    detail: str = ""
    remediation: str | None = None
    duration_ms: float = 0.0
    skipped: bool = False

    def to_dict(self) -> dict[str, Any]:
        """The JSON form returned by the API (``detail`` is passed through the secret redactor)."""
        return {"name": self.name, "ok": self.ok, "detail": scrub(self.detail), "remediation": self.remediation, "duration_ms": round(self.duration_ms, 1), "skipped": self.skipped}


class StepFailure(Exception):
    """Raised inside a step function to fail the step with a message and a remediation."""

    def __init__(self, detail: str, remediation: str | None = None) -> None:
        super().__init__(detail)
        self.detail, self.remediation = detail, remediation


class StepSkip(Exception):
    """Raised inside a step function when the step does not apply (it is reported ``ok`` and ``skipped`` and does not stop the test)."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


@dataclass
class StepSpec:
    """A planned step: its name and the function that performs it (returning the detail text, raising :class:`StepFailure` / :class:`StepSkip`)."""

    name: str
    run: Callable[[], str]
    skip_reason_if_blocked: str = "skipped because an earlier step failed"


def run_steps(plan: Sequence[StepSpec]) -> list[Step]:
    """Run ``plan`` in order; the first failing step stops the run and every step after it is reported as skipped (``ok=False, skipped=True``).

    A step function may return text (the detail), raise :class:`StepFailure` (fail with remediation) or :class:`StepSkip` (not applicable, ``ok=True``). Any other exception
    fails the step with its redacted message, so a connectivity test reports instead of raising.
    """
    out: list[Step] = []
    failed = False
    for spec in plan:
        if failed:
            out.append(Step(spec.name, False, spec.skip_reason_if_blocked, None, 0.0, True))
            continue
        t0 = time.perf_counter()
        try:
            detail = spec.run()
            out.append(Step(spec.name, True, detail or "", None, (time.perf_counter() - t0) * 1000))
        except StepSkip as skip:
            out.append(Step(spec.name, True, skip.detail, None, (time.perf_counter() - t0) * 1000, True))
        except StepFailure as fail:
            failed = True
            out.append(Step(spec.name, False, fail.detail, fail.remediation, (time.perf_counter() - t0) * 1000))
        except Exception as exc:  # noqa: BLE001 - a connectivity test reports failures, it does not raise them
            failed = True
            out.append(Step(spec.name, False, f"unexpected {type(exc).__name__}: {safe_message(exc)}", None, (time.perf_counter() - t0) * 1000))
    return out


def steps_ok(steps: Sequence[Step]) -> bool:
    """True when no step failed (skipped optional steps are fine)."""
    return all(s.ok for s in steps)


_KNOWN_SECRETS: collections.OrderedDict[str, None] = collections.OrderedDict()
_KNOWN_LOCK = threading.Lock()
_KNOWN_MAX = 512
_MIN_SECRET_LEN = 6


def remember_secret(value: Any) -> None:
    """Remember a credential value that this process has resolved, so :func:`scrub` can remove it from any text that is about to be shown or logged - pattern based redaction
    cannot recognise a bare secret that an upstream error message happens to echo. Only a bounded number of values is kept, in memory, never persisted or logged."""
    if isinstance(value, str) and len(value) >= _MIN_SECRET_LEN:
        with _KNOWN_LOCK:
            _KNOWN_SECRETS[value] = None
            _KNOWN_SECRETS.move_to_end(value)
            while len(_KNOWN_SECRETS) > _KNOWN_MAX:
                _KNOWN_SECRETS.popitem(last=False)


def scrub(text: str) -> str:
    """``text`` without any credential value remembered by :func:`remember_secret` and without the patterns of :func:`~olcf.core.redaction.redact_text`."""
    with _KNOWN_LOCK:
        known = list(_KNOWN_SECRETS)
    for secret in known:
        if secret in text:
            text = text.replace(secret, "[redacted]")
    return redact_text(text)


def safe_message(exc: BaseException, limit: int = 300) -> str:
    """A short, secret-redacted description of an exception for steps, gaps and logs (known credential values are removed, see :func:`scrub`)."""
    msg = getattr(exc, "message", None) or str(exc) or type(exc).__name__
    return scrub(str(msg).splitlines()[0] if str(msg) else type(exc).__name__)[:limit]


# --------------------------------------------------------------------------------------------- cancellation
class ScanCancelled(Exception):
    """Raised by :meth:`CancellationToken.raise_if_cancelled` when somebody asked the scan to stop."""


class ConnectorFailure(Exception):
    """Raised by a connector when the whole scan cannot proceed (no credentials, the identity is rejected everywhere). The runner records the scan as ``failed`` with this
    message (already free of secrets) instead of a stack trace; problems that affect only one workspace or item are gaps, never this exception."""

    def __init__(self, message: str, remediation: str | None = None) -> None:
        super().__init__(message)
        self.remediation = remediation


class CancellationToken:
    """Cooperative cancellation: the runner (or an API call, possibly on another replica) sets it; connectors check it **between batches** (and while waiting out a throttle).

    ``external`` is polled at most once per ``poll_seconds`` (the runner passes a function that reads the cancel flag of the source row), so a cancel issued on another node
    reaches the scan without a database round trip per batch.
    """

    def __init__(self, external: Callable[[], bool] | None = None, *, poll_seconds: float = 1.0) -> None:
        self._event = threading.Event()
        self._external = external
        self._poll = poll_seconds
        self._last_poll = 0.0

    def cancel(self) -> None:
        """Request cancellation."""
        self._event.set()

    def is_cancelled(self) -> bool:
        """True once cancellation was requested here or through the external flag."""
        if self._event.is_set():
            return True
        if self._external is not None and time.monotonic() - self._last_poll >= self._poll:
            self._last_poll = time.monotonic()
            try:
                if self._external():
                    self._event.set()
            except Exception:  # noqa: BLE001 - an unreadable flag must not kill the scan
                return False
        return self._event.is_set()

    def raise_if_cancelled(self) -> None:
        """Raise :class:`ScanCancelled` when cancellation was requested."""
        if self.is_cancelled():
            raise ScanCancelled("the scan was cancelled")

    def wait(self, seconds: float) -> bool:
        """Sleep up to ``seconds`` but wake early on cancellation; returns True if cancelled."""
        end = time.monotonic() + max(0.0, seconds)
        while True:
            left = end - time.monotonic()
            if left <= 0:
                return self.is_cancelled()
            if self._event.wait(min(left, self._poll)):
                return True
            if self.is_cancelled():
                return True


# --------------------------------------------------------------------------------------------- secrets and context
@dataclass
class ResolvedSecrets:
    """Credential material resolved from the secret store for one source: ``values[name]`` is a credential dict (``tenant_id``, ``client_id``, ``client_secret`` or
    ``private_key_pem`` + ``certificate_pem``) for ``name`` in ``metadata_identity`` / ``sampling_identity``; ``errors[name]`` says why a configured identity could not be
    resolved. Instances are passed to connectors in memory only; they have no ``__repr__`` of the values, are never logged and never stored."""

    values: dict[str, dict[str, Any]] = field(default_factory=dict, repr=False)
    errors: dict[str, str] = field(default_factory=dict)
    configured: set[str] = field(default_factory=set)

    def get(self, name: str) -> dict[str, Any] | None:
        """Credentials of an identity, or ``None``."""
        return self.values.get(name)

    def __repr__(self) -> str:
        return f"ResolvedSecrets(identities={sorted(self.values)}, errors={sorted(self.errors)})"


@dataclass
class ScanContext:
    """Everything a connector may use during ``discover``: the source, the mode and watermark, cancellation, the scope filter and the helpers to report gaps and counters.

    ``state`` is the connector's checkpoint document: connectors read the previous scan's values and write their own; the runner stores it with the source after the scan
    (only for scans that were not cancelled or failed). ``app`` is the platform :class:`~olcf.core.context.AppContext` (used by the local connectors; ``None`` in pure tests).
    """

    source: dict[str, Any]
    config: dict[str, Any]
    mode: str                                   # full | incremental
    cancel: CancellationToken = field(default_factory=CancellationToken)
    secrets: ResolvedSecrets = field(default_factory=ResolvedSecrets)
    since: str | None = None                    # ISO-8601 UTC start of the last scan of this source that completed (the incremental watermark)
    workspace_filter: frozenset[str] | None = None   # restrict to these OLCF workspace ids / Fabric workspace ids (None = everything the source is configured for)
    state: dict[str, Any] = field(default_factory=dict)
    app: Any = None
    payload: bytes | None = None                # scanner_import: the saved scanResult document
    http_client_factory: Callable[[], Any] | None = None
    sleep: Callable[[float], None] | None = None
    clock: Callable[[], float] = time.monotonic
    now: Callable[[], str] | None = None
    principal: Any = None                       # the principal that started the scan (the system principal for scheduled scans)
    gaps: list[Gap] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)
    #: runtime limits resolved by the runner from the ``intelligence.*`` settings (scanner batch size, getInfo requests per hour, concurrent scans, poll interval ...)
    limits: dict[str, Any] = field(default_factory=dict)

    @property
    def source_id(self) -> str:
        """The ``src_`` id of the source being scanned."""
        return str(self.source.get("id") or "")

    @property
    def is_full(self) -> bool:
        """True for a full scan."""
        return self.mode == "full"

    def record_gap(self, kind: str, subject: str, detail: str, remediation: str | None = None, *, location: dict[str, Any] | None = None, source: str | None = None) -> Gap:
        """Record a gap (something that could not be discovered) on the scan; it is stored by the runner after the current batch."""
        gap = Gap(kind=kind, source=source or f"discovery:{self.source.get('kind', 'source')}", subject=subject, detail=scrub(detail)[:2000], remediation=remediation,
                  location=dict(location or {}))
        self.gaps.append(gap)
        return gap

    def count(self, name: str, n: int = 1) -> None:
        """Add to a named counter reported in the scan statistics."""
        self.counters[name] = self.counters.get(name, 0) + n

    def check_cancelled(self) -> None:
        """Raise :class:`ScanCancelled` if the scan was cancelled."""
        self.cancel.raise_if_cancelled()

    def pause(self, seconds: float) -> bool:
        """Wait ``seconds`` (the injected ``sleep`` in tests, otherwise interruptible by cancellation); returns True if the scan was cancelled meanwhile."""
        if self.sleep is not None:
            self.sleep(seconds)
            return self.cancel.is_cancelled()
        return self.cancel.wait(seconds)


# --------------------------------------------------------------------------------------------- the contract
@runtime_checkable
class Connector(Protocol):
    """What the scan runner and the API need from a connector."""

    #: stable kind name stored in ``discovery_sources.kind``
    kind: str
    #: one-line description for the UI
    label: str

    def validate_config(self, config: Mapping[str, Any]) -> dict[str, Any]:
        """Check and normalise the non-secret ``config`` of a source (raise :class:`~olcf.core.errors.ValidationFailed` with field-level messages)."""
        ...

    def test(self, config: Mapping[str, Any], secrets: ResolvedSecrets) -> list[Step]:
        """The ordered connectivity steps; stops at the first failure and reports the rest as skipped."""
        ...

    def discover(self, ctx: ScanContext) -> Iterator[Batch]:
        """Stream the source as batches of facts and definitions; check ``ctx.cancel`` between batches; report what could not be read through ``ctx.record_gap``."""
        ...


ConnectorFactory = Callable[[], Connector]
_REGISTRY: dict[str, ConnectorFactory] = {}
_LOCK = threading.Lock()


def register_connector(kind: str, factory: ConnectorFactory, *, replace: bool = False) -> None:
    """Make a connector kind available. ``factory`` returns a (stateless) connector. Registering an existing kind needs ``replace=True``."""
    with _LOCK:
        if kind in _REGISTRY and not replace:
            raise ValueError(f"a connector for kind '{kind}' is already registered")
        _REGISTRY[kind] = factory


def connector_for(kind: str) -> Connector:
    """A connector instance for ``kind``; ``KeyError`` for an unknown kind."""
    with _LOCK:
        factory = _REGISTRY[kind]
    return factory()


def registered_kinds() -> list[str]:
    """All registered connector kinds, sorted."""
    with _LOCK:
        return sorted(_REGISTRY)


def facts_counts(batch: Batch) -> dict[str, int]:
    """Counts of what a batch holds (``assets``, ``edges``, ``gaps``, ``definitions``) for scan statistics."""
    if isinstance(batch, DefinitionFact):
        return {"assets": 0, "edges": 0, "gaps": 0, "definitions": 1}
    return {"assets": len(batch.assets), "edges": len(batch.edges), "gaps": len(batch.gaps), "definitions": 0}
