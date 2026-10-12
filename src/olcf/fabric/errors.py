# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Errors of the Fabric adapter. Every rejection carries a stable ``code`` (what the SDK and the dashboards switch on) and a remediation."""

from __future__ import annotations

from olcf.core.errors import BadRequest, Conflict, Forbidden, OlcfError, Unauthenticated, ValidationFailed


class CapsuleTooLarge(OlcfError):
    """The capsule exceeds the maximum size (HTTP 413, code ``capsule_too_large``)."""
    status = 413
    code = "capsule_too_large"


class CapsuleSignatureInvalid(BadRequest):
    """The capsule signature does not verify (code ``capsule_signature_invalid``)."""
    code = "capsule_signature_invalid"


class CapsuleReplay(Conflict):
    """The challenge nonce, idempotency key or submission was already used (code ``capsule_replay``)."""
    code = "capsule_replay"


class NonceInvalid(BadRequest):
    """The challenge nonce is missing, unknown or expired (code ``capsule_nonce_invalid``)."""
    code = "capsule_nonce_invalid"


class CapsuleExpired(BadRequest):
    """The capsule is older than the accepted maximum age (code ``capsule_expired``)."""
    code = "capsule_expired"


class CapsuleContainsSecret(ValidationFailed):
    """The capsule contains something that looks like a secret and is refused (code ``capsule_contains_secret``)."""
    code = "capsule_contains_secret"


class TenantNotAllowed(Forbidden):
    """The Entra tenant is not on the allowed list (code ``fabric_tenant_not_allowed``)."""
    code = "fabric_tenant_not_allowed"


class WorkspaceNotAllowed(Forbidden):
    """The Fabric workspace is not mapped to an OLCF workspace (code ``fabric_workspace_not_allowed``)."""
    code = "fabric_workspace_not_allowed"


class FabricNotConfigured(OlcfError):
    """The Fabric integration is disabled or lacks configuration that the call needs (HTTP 409, code ``fabric_not_configured``)."""
    status = 409
    code = "fabric_not_configured"


class TokenInvalid(Unauthenticated):
    """The Entra token was rejected (code ``entra_token_invalid``)."""
    code = "entra_token_invalid"


__all__ = ["CapsuleContainsSecret", "CapsuleExpired", "CapsuleReplay", "CapsuleSignatureInvalid", "CapsuleTooLarge", "FabricNotConfigured", "NonceInvalid",
           "TenantNotAllowed", "TokenInvalid", "WorkspaceNotAllowed"]
