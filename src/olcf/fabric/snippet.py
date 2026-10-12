# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The ready-to-paste Fabric notebook cell (``GET /v1/integrations/fabric/snippet``).

The cell installs nothing by itself (a ``%pip`` of the wheel is shown as a comment because where the wheel lives - Lakehouse Files, a Fabric Environment, an internal
index - is the customer's choice). It configures the SDK for this control plane and submits the notebook it is pasted into.
"""

from __future__ import annotations

from typing import Any

from olcf.version import __version__

SDK_DISTRIBUTION = "olcf-fabric"
SDK_VERSION = __version__  # kept equal to sdk/fabric/olcf_fabric/__init__.py (tests/fabric/test_sdk_packaging.py)


def render(*, endpoint: str, fabric_workspace: str | None = None, olcf_workspace: str | None = None, auth: str = "keyvault", vault: str = "https://<your-vault>.vault.azure.net/",
           secret_name: str = "olcf-api-token", audience: str | None = None) -> dict[str, Any]:  # noqa: S107 - a Key Vault secret *name*
    """Build the snippet. ``auth`` selects which authentication block is shown (the other modes are listed as alternatives in the notes)."""
    lines = [
        "# --- Run this notebook on the on-prem compute fabric (Fabric Runtime 2.0 Compatible) ---",
        f"# Wheel: {SDK_DISTRIBUTION}-{SDK_VERSION}-py3-none-any.whl  (attach it to a Fabric Environment, or install it for this session:)",
        f"# %pip install /lakehouse/default/Files/{SDK_DISTRIBUTION.replace('-', '_')}-{SDK_VERSION}-py3-none-any.whl",
        "import olcf_fabric as olcf",
        "",
    ]
    if fabric_workspace:
        lines.append(f"# Fabric workspace {fabric_workspace}" + (f" runs in OLCF workspace '{olcf_workspace}'" if olcf_workspace else " (check that it is mapped by your OLCF administrator)"))
    if auth == "token":
        lines += ["olcf.configure(", f'    endpoint="{endpoint}",', '    token="<paste a short-lived token; prefer keyvault>",', ")"]
    elif auth == "client_credentials":
        lines += ["olcf.configure(", f'    endpoint="{endpoint}",', '    auth="client_credentials",', '    tenant_id="<entra-tenant-id>",', '    client_id="<app-registration-client-id>",',
                  f'    keyvault="{vault}", client_secret_name="olcf-client-secret",', f'    audience="{audience or "api://<olcf-api-app-id>"}",', ")"]
    elif auth == "notebookutils":
        lines += ["olcf.configure(", f'    endpoint="{endpoint}",', '    auth="notebookutils",', f'    audience="{audience or "api://<olcf-api-app-id>"}",', ")"]
    else:
        lines += ["olcf.configure(", f'    endpoint="{endpoint}",', '    auth="keyvault",', f'    keyvault="{vault}", secret_name="{secret_name}",', ")"]
    lines += [
        "",
        "olcf.test_connection()   # optional: prints a checklist (DNS, TLS, token, handshake, authorisation, ...)",
        "olcf.run_remote()        # runs the whole notebook remotely; this cell is not part of the remote run",
    ]
    code = "\n".join(lines) + "\n"
    return {
        "snippet": code, "language": "python", "endpoint": endpoint, "sdk": {"distribution": SDK_DISTRIBUTION, "version": SDK_VERSION},
        "fabric_workspace": fabric_workspace, "olcf_workspace": olcf_workspace, "auth": auth,
        "notes": [
            "Paste the cell at the top of the notebook (or anywhere: the cell that calls olcf.run_remote() is removed from the remote run).",
            "Do not put a token or client secret in the cell. Keep it in Azure Key Vault and read it with notebookutils.credentials.getSecret (auth='keyvault').",
            "Other authentication modes: token, client_credentials, notebookutils (needs a tenant that supports the audience), auto.",
            "Return value: on success the notebook's exit value is passed to notebookutils.notebook.exit(); on failure olcf.RemoteExecutionError is raised so pipelines see the failure.",
        ],
    }


__all__ = ["SDK_DISTRIBUTION", "SDK_VERSION", "render"]
