# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Deterministic names shared by the control plane and the runtime shim (stdlib only).

The control plane places a granted secret into the job under an environment variable whose name is derived from the Key Vault coordinates the notebook
uses, so ``notebookutils.credentials.getSecret(vault_url, name)`` finds it without any lookup service and without the notebook source changing.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit

_VAULT_NAME = re.compile(r"^[A-Za-z0-9-]{3,24}$")


def normalise_vault(vault: str) -> str:
    """``https://MyVault.vault.azure.net/`` | ``myvault`` | ``myvault.vault.azure.net`` -> ``myvault.vault.azure.net`` (lower case host)."""
    v = (vault or "").strip()
    if "://" in v:
        host = (urlsplit(v).hostname or "").lower()
    else:
        host = v.split("/")[0].lower()
    if _VAULT_NAME.match(host):
        host += ".vault.azure.net"
    return host


def secret_env_name(vault: str, name: str) -> str:
    """Environment variable carrying the value of Key Vault secret ``name`` of ``vault``: ``OLCF_KV_<16 hex>``."""
    digest = hashlib.sha256(f"{normalise_vault(vault)}|{(name or '').strip()}".encode()).hexdigest()[:16].upper()
    return f"OLCF_KV_{digest}"
