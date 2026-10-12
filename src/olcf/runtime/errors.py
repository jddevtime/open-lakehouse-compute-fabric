# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Error types raised by the runtime manager.

Every error carries an actionable ``remediation`` string (CONTRACTS section 0, rule 2: fail closed
with a message the operator can act on). They derive from :class:`olcf.core.errors.OlcfError` so the
worker agent can serialise them verbatim into a ``result`` frame (``error`` object).
"""

from __future__ import annotations

from olcf.core.errors import OlcfError, Unavailable, Unsupported, ValidationFailed


class RuntimeManagerError(OlcfError):
    """Base class for all runtime-manager failures."""

    status = 500
    code = "runtime_error"


class ManifestInvalid(ValidationFailed):
    """The manifest does not validate against the JSON schema or violates a semantic rule."""

    code = "manifest_invalid"


class SignatureInvalid(RuntimeManagerError):
    """Signature missing, malformed, made by an untrusted key, or not matching the manifest bytes."""

    status = 403
    code = "manifest_signature_invalid"


class PrerequisiteMissing(Unavailable):
    """A host prerequisite (Python, JDK, uv, ...) is missing; ``remediation`` says how to fix it."""

    code = "prerequisite_missing"


class PlatformUnsupported(Unsupported):
    """The manifest does not support this OS / CPU architecture."""

    code = "platform_unsupported"


class InstallFailed(RuntimeManagerError):
    """An install / repair step failed; the staging directory has been discarded."""

    code = "runtime_install_failed"


class VerifyFailed(RuntimeManagerError):
    """A runtime failed verification (hash drift, import failure, smoke-test failure)."""

    code = "runtime_verify_failed"


class DependencyResolutionError(RuntimeManagerError):
    """Maven dependency resolution failed (missing artifact, unsupported POM construct)."""

    code = "dependency_resolution_failed"


class ChecksumMismatch(RuntimeManagerError):
    """A downloaded artifact does not match its published or locked checksum (fail closed)."""

    status = 502
    code = "checksum_mismatch"


class RuntimeNotFound(RuntimeManagerError):
    """The requested runtime is not installed or not known (HTTP 404, code ``runtime_not_found``)."""
    status = 404
    code = "runtime_not_found"


class BundleInvalid(RuntimeManagerError):
    """An offline bundle is malformed, tampered with, or built for another platform."""

    status = 422
    code = "bundle_invalid"
