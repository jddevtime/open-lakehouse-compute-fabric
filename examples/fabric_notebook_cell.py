# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""The cell to paste into a Microsoft Fabric notebook so that the notebook runs on YOUR OLCF cluster instead of Fabric's Spark pools.

The server generates this cell for you, already filled in for your endpoint and workspace mapping:

    curl -fsS -H "Authorization: Bearer $OLCF_TOKEN" https://olcf.example.com/v1/integrations/fabric/snippet

The package imported below is `olcf-fabric` (sdk/fabric in this repository; `scripts/build_fabric_sdk.sh` builds the wheel).  Attach the wheel to
a Fabric Environment, or upload it to the lakehouse Files area and `%pip install` it in the first cell.  It has no dependencies beyond the Python
standard library.  The Microsoft behaviours it relies on are listed in docs/guides/fabric-assumptions.md.

The credential never lives in the notebook: the platform token (scope `fabric`) is kept in Azure Key Vault and read at run time.
Create it with   olcf token create fabric-notebooks --scope fabric   and store it as the Key Vault secret named below.
"""

# %% ---- paste from here into a Fabric notebook cell ------------------------------------------------------------------------------------
# %pip install /lakehouse/default/Files/olcf_fabric-0.1.0-py3-none-any.whl     # or attach the wheel to the Fabric Environment
import olcf_fabric as olcf

olcf.configure(
    endpoint="https://olcf.example.com",                      # your control plane (https; plain http only for loopback)
    auth="keyvault",
    keyvault="https://<your-vault>.vault.azure.net/",         # Azure Key Vault holding the OLCF platform token
    secret_name="olcf-api-token",  # noqa: S106 - the NAME of a Key Vault secret, not a secret
)

olcf.test_connection()   # optional: a checklist (DNS, TLS, token, handshake, authorisation); each failing item says how to fix it
olcf.run_remote()        # runs the WHOLE notebook on the OLCF cluster; this cell itself is not part of the remote run
