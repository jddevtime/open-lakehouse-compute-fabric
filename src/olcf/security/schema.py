# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Identity-federation metadata schema (migration stream ``security``).

Tables
------
``identity_providers``  one row per configured OIDC / Entra / LDAP provider. The client secret is *never* stored here: ``client_secret_id``
                        is an opaque reference into the secret store (spec 23). ``config`` holds claim/role mappings and policy knobs.
``oidc_auth_requests``  in-flight authorization-code logins (state, nonce, PKCE verifier, browser binding). Single use, short lived.
                        Only the SHA-256 of ``state`` and of the browser-binding value are stored, so a database read cannot be replayed
                        against the callback endpoint. The PKCE verifier is sealed with the master key.
``oidc_login_codes``    one-time codes handed to the web UI after a successful callback (exchanged for OLCF tokens). Only a hash is stored.

The two ephemeral tables are deliberately excluded from backups (``olcf.ops.backup.EPHEMERAL_TABLES``): restoring a stale login handshake
would be pointless and storing PKCE material in backups is an unnecessary exposure.
"""

from __future__ import annotations

from sqlalchemy import JSON, Boolean, Column, Index, LargeBinary, String, Table

from olcf.db.migrate import Migration, create_tables, drop_tables, register
from olcf.db.schema import ID, TS, metadata

identity_providers = Table(
    "identity_providers", metadata,
    Column("id", ID, primary_key=True),
    Column("name", String(64), nullable=False, unique=True),  # URL-safe slug, used in /v1/identity/oidc/{name}/...
    Column("type", String(16), nullable=False),  # oidc | entra | ldap
    Column("display_name", String(255)),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("issuer", String(512)),  # exact issuer (OIDC) / authority issuer template (Entra); NULL for LDAP
    Column("client_id", String(255)),
    Column("client_secret_id", ID),  # reference into ``secrets``
    Column("config", JSON, nullable=False, default=dict),
    Column("last_test", JSON),  # {ok, at, steps[]}
    Column("etag", String(64), nullable=False),
    Column("created_by", ID),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

oidc_auth_requests = Table(
    "oidc_auth_requests", metadata,
    Column("state_hash", String(64), primary_key=True),
    Column("provider_id", ID, nullable=False),
    Column("nonce", String(128), nullable=False),
    Column("verifier_enc", LargeBinary, nullable=False),
    Column("verifier_kid", String(64), nullable=False),
    Column("binding_hash", String(64), nullable=False),
    Column("redirect_to", String(1024), nullable=False),
    Column("ui_challenge", String(128)),
    Column("created_at", TS, nullable=False),
    Column("expires_at", TS, nullable=False, index=True),
    Column("consumed_at", TS),
)

oidc_login_codes = Table(
    "oidc_login_codes", metadata,
    Column("code_hash", String(64), primary_key=True),
    Column("principal_id", ID, nullable=False),
    Column("provider_id", ID, nullable=False),
    Column("ui_challenge", String(128)),
    Column("created_at", TS, nullable=False),
    Column("expires_at", TS, nullable=False, index=True),
    Column("consumed_at", TS),
    Index("ix_oidc_login_codes_principal", "principal_id"),
)

SECURITY_TABLES = [identity_providers, oidc_auth_requests, oidc_login_codes]

register(Migration("security", 1, "identity federation", up=lambda c: create_tables(c, SECURITY_TABLES),
                   down=lambda c: drop_tables(c, reversed(SECURITY_TABLES))))
