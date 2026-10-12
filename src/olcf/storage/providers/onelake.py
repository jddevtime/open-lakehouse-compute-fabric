# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Microsoft OneLake: the ADLS Gen2 API at ``onelake.dfs.fabric.microsoft.com``.

OneLake addresses data as ``<workspace>/<item>.Lakehouse/<Files|Tables>/...`` where workspace and item may be given
by GUID or by (URL-safe) name.  Supported authentication: Entra service principal (client secret), a pre-acquired
user/OBO bearer token (stored as a secret ``{access_token}``), managed identity, or ``DefaultAzureCredential``.  Account
keys and account SAS do not exist for OneLake and are rejected.  The canonical root is
``abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<item>``.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from olcf.core.errors import BadRequest, Unsupported
from olcf.storage.paths import normalize_rel
from olcf.storage.providers.adls import (
    AdlsFileSystem,
    AdlsProvider,
    StaticTokenCredential,
    _secret_value,
    parse_abfss,
)
from olcf.storage.providers.base import FieldSpec

ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
ONELAKE_AUTH = ("service_principal", "user_token", "managed_identity", "default")


def build_onelake_root(workspace: str, item: str, *, host: str = ONELAKE_HOST) -> str:
    """Compose the canonical root URI from workspace and lakehouse (GUIDs or names)."""
    if not workspace or not item:
        raise BadRequest("OneLake needs both a workspace and a lakehouse (GUID or name)")
    return f"abfss://{workspace}@{host}/{item.strip('/')}"


class OneLakeProvider(AdlsProvider):
    """Storage provider for Microsoft Fabric OneLake; it uses the ADLS Gen2 implementation with OneLake addressing."""
    id = "onelake"
    label = "Microsoft Fabric OneLake"
    description = "OneLake lakehouses addressed by workspace/lakehouse GUID or name; Entra service principal or user token."
    connection_types = ("onelake",)
    default_suffix = ONELAKE_HOST
    connection_fields = (
        FieldSpec("workspace", "Fabric workspace (GUID or name)", required=True),
        FieldSpec("lakehouse", "Lakehouse (GUID or name, e.g. Sales.Lakehouse)", required=True),
        FieldSpec("auth", "Authentication", type="enum", required=True, options=ONELAKE_AUTH, default="service_principal"),
        FieldSpec("tenant_id", "Tenant id", visible_when={"auth": ["service_principal", "managed_identity"]}),
        FieldSpec("client_id", "Client id", visible_when={"auth": ["service_principal", "managed_identity"]}),
        FieldSpec("endpoint", "Endpoint override", default=f"https://{ONELAKE_HOST}", help="Use the regional endpoint (e.g. https://westus-onelake.dfs.fabric.microsoft.com) when required."),
        FieldSpec("secret", "Client secret / access token", type="secret", secret=True, visible_when={"auth": ["service_principal", "user_token"]},
                  help="service_principal -> {client_secret}; user_token -> {access_token} (audience https://storage.azure.com, short-lived)."),
    )

    def _auth(self, config: dict[str, Any]) -> str:
        auth = config.get("auth") or "service_principal"
        if auth in ("account_key", "sas"):
            raise Unsupported("OneLake does not support account keys or SAS tokens", remediation="use a service principal or a user token")
        if auth not in ONELAKE_AUTH:
            raise BadRequest(f"unknown auth mode '{auth}'", details={"supported": list(ONELAKE_AUTH)})
        return auth

    def _account_host(self, root_uri: str, config: dict[str, Any]) -> str:
        return parse_abfss(root_uri)[1]

    def _account_name(self, root_uri: str, config: dict[str, Any]) -> str:
        return "onelake"

    def _account_url(self, root_uri: str, config: dict[str, Any]) -> str:
        if config.get("endpoint"):
            return config["endpoint"].rstrip("/")
        return f"https://{parse_abfss(root_uri)[1]}"

    def claims(self, root_uri: str) -> bool:
        u = urlparse(root_uri)
        return u.scheme in ("abfss", "abfs", "https") and "fabric.microsoft.com" in (u.netloc.split("@")[-1])

    def validate_root(self, root_uri: str) -> str:
        if root_uri.startswith("https://"):  # https://onelake.dfs.fabric.microsoft.com/<ws>/<item>[/...]
            u = urlparse(root_uri)
            segs = [s for s in u.path.split("/") if s]
            if len(segs) < 2:
                raise BadRequest("OneLake https URL must be https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>")
            return build_onelake_root(segs[0], "/".join(segs[1:]), host=u.hostname or ONELAKE_HOST)
        container, host, path = parse_abfss(root_uri)
        if "fabric.microsoft.com" not in host:
            raise BadRequest(f"'{host}' is not a OneLake endpoint (expected {ONELAKE_HOST})")
        if not path:
            raise BadRequest("a OneLake lakehouse root must include the lakehouse (GUID or '<name>.Lakehouse')")
        return f"abfss://{container}@{host}/{path}"

    def validate_config(self, config: dict[str, Any]) -> dict[str, Any]:
        auth = self._auth(config)
        if auth == "service_principal" and not (config.get("tenant_id") and config.get("client_id")):
            raise BadRequest("service_principal authentication needs tenant_id and client_id")
        return config

    def root_from_config(self, config: dict[str, Any]) -> str:
        return build_onelake_root(config["workspace"], config["lakehouse"])

    def credential(self, root_uri: str, config: dict[str, Any], secret: Any) -> Any:
        auth = self._auth(config)
        if auth == "user_token":
            tok = _secret_value(secret, "access_token", "token")
            if not tok:
                raise BadRequest("user_token authentication needs a secret {access_token}")
            return StaticTokenCredential(tok)
        return super().credential(root_uri, config, secret)

    def filesystem(self, root_uri: str, config: dict[str, Any], secret: Any) -> AdlsFileSystem:
        fs = super().filesystem(root_uri, config, secret)
        fs.provider = "onelake"
        return fs

    # --- runtime settings
    def spark_conf(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        """ABFS OAuth client-credentials keyed by the OneLake host, as documented for external Spark engines."""
        auth = self._auth(config)
        host = parse_abfss(root_uri)[1]
        if auth == "user_token":
            raise Unsupported("Spark cannot use a pasted user access token with the ABFS driver",
                              remediation="use auth=service_principal (client credentials) for Spark workloads")
        c = super().spark_conf(root_uri, config, secret)
        # OneLake additionally requires the generic endpoint keys for the default OneLake host
        p = "spark.hadoop."
        if host != ONELAKE_HOST:
            for k in list(c):
                if k.startswith(p + "fs.azure.") and k.endswith("." + host):
                    c[k[: -len(host)] + ONELAKE_HOST] = c[k]
        return c

    def delta_storage_options(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        auth = self._auth(config)
        o: dict[str, str] = {"azure_storage_account_name": "onelake", "azure_use_fabric_endpoint": "true"}
        if config.get("endpoint") and not config["endpoint"].startswith("https://" + ONELAKE_HOST):
            o["azure_storage_endpoint"] = config["endpoint"]
            if config["endpoint"].startswith("http://"):
                o["allow_http"] = "true"
        if auth == "service_principal":
            o.update(azure_storage_client_id=config["client_id"], azure_storage_client_secret=_secret_value(secret, "client_secret", "secret") or "",
                     azure_storage_tenant_id=config["tenant_id"])
        elif auth == "user_token":
            o["azure_storage_token"] = _secret_value(secret, "access_token", "token") or ""
        else:
            o["azure_storage_token"] = self._acquire_token(root_uri, config, secret)
        return o

    def env(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        e: dict[str, str] = {"AZURE_STORAGE_ACCOUNT": "onelake"}
        if self._auth(config) == "service_principal":
            e.update(AZURE_TENANT_ID=config["tenant_id"], AZURE_CLIENT_ID=config["client_id"],
                     AZURE_CLIENT_SECRET=_secret_value(secret, "client_secret", "secret") or "")
        return e

    def iceberg_properties(self, root_uri: str, config: dict[str, Any], secret: Any) -> dict[str, str]:
        raise Unsupported("PyIceberg has no OneLake FileIO; Iceberg tables in OneLake are exposed through Delta (Iceberg metadata virtualisation)")


def normalize_item(item: str) -> str:
    """Strip a trailing slash / duplicate separators in an item path (``Sales.Lakehouse/``)."""
    return normalize_rel(item)
