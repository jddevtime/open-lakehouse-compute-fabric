# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Layered configuration.

Precedence (highest first): environment variables (``OLCF_`` prefix, ``__`` for nesting),
``<data_dir>/config.toml``, built-in defaults. Example::

    OLCF_SERVER__PORT=9000 OLCF_DB__URL=postgresql+psycopg://olcf:pw@db/olcf olcf serve

The data directory defaults to ``$OLCF_DATA_DIR`` or ``~/.olcf``.
"""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def default_data_dir() -> Path:
    return Path(os.environ.get("OLCF_DATA_DIR") or Path.home() / ".olcf").expanduser().resolve()


class ServerSettings(BaseModel):
    """HTTP listener settings: bind address (loopback by default), port, public URL, CORS origins, trusted proxy count and whether to serve the web console.
    """
    host: str = "127.0.0.1"  # localhost by default (spec 31: remote access must be explicit)
    port: int = 8470
    #: Externally visible base URL (used in enrolment commands, OIDC redirects, Fabric snippets).
    public_url: str | None = None
    #: Origins allowed by CORS (empty = same-origin only).
    cors_origins: list[str] = Field(default_factory=list)
    #: Trusted proxy hop count for X-Forwarded-* headers (0 = do not trust).
    trusted_proxies: int = 0
    serve_ui: bool = True
    request_body_limit_mb: int = 256


class TLSSettings(BaseModel):
    """TLS settings for the API and worker channel: certificate files (or an auto-issued certificate from the built-in CA), extra names, worker client-certificate requirement and a custom outbound CA bundle.
    """
    enabled: bool = False
    #: When enabled and no cert paths are given, OLCF issues a server cert from its built-in CA.
    cert_file: str | None = None
    key_file: str | None = None
    #: Extra DNS names / IPs for the auto-issued server certificate.
    extra_sans: list[str] = Field(default_factory=list)
    #: Require client certificates on the worker channel (mTLS).
    require_client_cert_for_workers: bool = False
    ca_bundle: str | None = None  # custom CA trust for outbound HTTPS (enterprise proxies)


class DatabaseSettings(BaseModel):
    """Control-plane database settings: the SQLAlchemy URL (SQLite under the data directory when unset), pool size and SQL echo."""
    #: SQLAlchemy URL. ``None`` => ``sqlite:///<data_dir>/olcf.db``.
    url: str | None = None
    pool_size: int = 10
    echo: bool = False


class AuthSettings(BaseModel):
    """Authentication settings: token lifetimes, local login switch, password policy, login rate limit and the bootstrap administrator name.
    """
    access_token_ttl_seconds: int = 1800
    refresh_token_ttl_seconds: int = 60 * 60 * 24 * 14
    #: Allow username/password login. Production should enable OIDC and may disable this
    #: after bootstrap ("local bootstrap admin only for initial setup").
    local_login_enabled: bool = True
    password_min_length: int = 12
    login_rate_limit_per_minute: int = 20
    #: Name of the bootstrap administrator created by ``olcf init``.
    bootstrap_admin: str = "admin"


class StorageSettings(BaseModel):
    """Settings for the default local storage provider and for previews and uploads: storage root, maximum preview rows and maximum upload size.
    """
    #: Root for the default local-filesystem lakehouse storage provider.
    local_root: str | None = None  # default <data_dir>/storage
    max_preview_rows: int = 1000
    max_upload_mb: int = 512


class ComputeSettings(BaseModel):
    """Compute settings: the host that Spark executors use to reach drivers, Spark port bases and ranges, and the cores and memory held back from admission on each node.
    """
    #: Host name/IP that Spark executors use to reach drivers started on the control-plane host.
    advertise_host: str | None = None
    #: Spark standalone master RPC: one master per runtime family, each on the first free port of ``base .. base + span - 1``.
    spark_master_port_base: int = 7077
    spark_master_port_span: int = 16
    #: Spark standalone master web UI (same allocation).
    spark_master_ui_port_base: int = 18080
    spark_master_ui_port_span: int = 16
    #: Spark standalone worker daemon RPC (the master connects to it to launch executors) and web UI: one worker per runtime family and node, allocated from these ranges.
    worker_port_range: tuple[int, int] = (41200, 41299)
    worker_ui_port_range: tuple[int, int] = (41300, 41399)
    #: Driver web UI (``spark.ui.port`` is set to the first port; Spark's own retry stays within Spark's default 16 neighbours).
    driver_ui_port_range: tuple[int, int] = (4040, 4056)
    driver_port_range: tuple[int, int] = (41000, 41099)
    block_manager_port_range: tuple[int, int] = (41100, 41199)
    #: Fraction of each node's cores/RAM reserved for OS + broker (admission headroom).
    reserve_cores: int = 1
    reserve_memory_fraction: float = 0.15
    scheduler_interval_seconds: float = 1.0
    worker_heartbeat_seconds: float = 5.0
    worker_offline_after_seconds: float = 20.0
    #: Auto-start an in-process worker agent so a single machine works out of the box.
    embedded_worker: bool = True
    default_job_timeout_seconds: int = 6 * 3600

    def port_ranges(self) -> dict[str, tuple[int, int]]:
        """Every port range the Spark deployment uses, by role (inclusive). The Helm NetworkPolicy, ``/v1/admin/ports`` and the daemon allocation all read this."""
        return {
            "master_rpc": (self.spark_master_port_base, self.spark_master_port_base + max(1, self.spark_master_port_span) - 1),
            "master_ui": (self.spark_master_ui_port_base, self.spark_master_ui_port_base + max(1, self.spark_master_ui_port_span) - 1),
            "worker_rpc": tuple(self.worker_port_range),  # type: ignore[dict-item]
            "worker_ui": tuple(self.worker_ui_port_range),  # type: ignore[dict-item]
            "driver_ui": tuple(self.driver_ui_port_range),  # type: ignore[dict-item]
            "driver": tuple(self.driver_port_range),  # type: ignore[dict-item]
            "block_manager": tuple(self.block_manager_port_range),  # type: ignore[dict-item]
        }

    @model_validator(mode="after")
    def _check_port_ranges(self) -> ComputeSettings:
        spans = [("spark_master_port_span", self.spark_master_port_span), ("spark_master_ui_port_span", self.spark_master_ui_port_span)]
        for name, span in spans:
            if span < 1:
                raise ValueError(f"compute.{name} must be at least 1")
        items = sorted(self.port_ranges().items(), key=lambda kv: kv[1])
        for role, (lo, hi) in items:
            if not (1 <= lo <= hi <= 65535):
                raise ValueError(f"the {role} port range {lo}-{hi} is not a valid range of TCP ports")
        for (ra, (_, ahi)), (rb, (blo, _)) in zip(items, items[1:], strict=False):
            if blo <= ahi:
                raise ValueError(f"the {ra} and {rb} port ranges overlap; give every Spark role its own range (compute.* port settings)")
        return self


class FabricSettings(BaseModel):
    """Settings for accepting work from Microsoft Fabric: whether the adapter is enabled, trusted Entra tenants and audiences, and capsule age and size limits.
    """
    enabled: bool = True
    #: Entra tenant(s) from which Fabric-originated tokens are accepted.
    allowed_tenants: list[str] = Field(default_factory=list)
    #: Audience(s) (App ID URI / client id) the OLCF API registration uses in Entra.
    audiences: list[str] = Field(default_factory=list)
    capsule_max_age_seconds: int = 600
    capsule_max_bytes: int = 64 * 1024 * 1024


class SecuritySettings(BaseModel):
    """Security settings: secrets provider and master key location, audit hash chain, and package index policy."""
    secrets_provider: str = "file"  # file | vault
    #: Master key location for the ``file`` secrets provider (default <data_dir>/secrets/master.key).
    master_key_file: str | None = None
    use_keyring: bool = False
    audit_hash_chain: bool = True
    allow_public_package_install: bool = True
    allowed_package_indexes: list[str] = Field(default_factory=list)


class RuntimeTrustSettings(BaseModel):
    """Which public keys may sign Spark runtime manifests (``runtime.trust``)."""
    #: Trust the **development** signing key shipped with the package (its private half is in the source tree, so anyone can forge a manifest it accepts).
    #: ``None`` = automatic: trusted unless ``environment`` is ``production``. Set ``true`` to opt in explicitly in production (not recommended; the health
    #: check ``runtime-trust`` reports it), ``false`` to refuse it everywhere.
    allow_development_key: bool | None = None
    #: Directory of ``*.pub`` PEM files that *replaces* the built-in store (the development key is then not trusted). ``OLCF_RUNTIME_TRUST_DIR`` wins over it.
    dir: str | None = None


class RuntimeSettings(BaseModel):
    """Runtime (Spark distribution) supply-chain settings."""
    trust: RuntimeTrustSettings = Field(default_factory=RuntimeTrustSettings)


class RetentionSettings(BaseModel):
    """Retention periods in days for job history, audit records, events, metrics and logs."""
    job_history_days: int = 365
    audit_days: int = 730
    event_days: int = 14
    metric_days: int = 14
    log_days: int = 90


class IntelligenceSettings(BaseModel):
    """Static settings of the Intelligence module (asset graph, lineage, discovery); the intervals and limits you tune while running are runtime settings (``olcf settings``)."""
    #: Install the Intelligence module's event subscriber, inbox processor and discovery loops. The REST routes stay available; ``false`` stops collecting.
    enabled: bool = True
    #: Run an incremental scan of every enabled discovery source shortly after start-up.
    scan_on_start: bool = True
    #: Inbox rows (platform events) the processor translates and applies per pass.
    inbox_batch_size: int = 200
    #: Seconds between two passes of the inbox processor (leader only).
    inbox_poll_seconds: float = 2.0
    #: Pending inbox rows above which the health check reports `degraded`.
    inbox_lag_warn: int = 5000


class Settings(BaseSettings):
    """Root settings object. Values are layered from defaults, the configuration file and ``OLCF_`` environment variables (nested keys use ``__``); unknown keys are ignored.
    """
    model_config = SettingsConfigDict(env_prefix="OLCF_", env_nested_delimiter="__", extra="ignore")

    data_dir: Path = Field(default_factory=default_data_dir)
    environment: str = "production"  # production | development | test
    log_level: str = "INFO"
    log_json: bool = False
    server: ServerSettings = Field(default_factory=ServerSettings)
    tls: TLSSettings = Field(default_factory=TLSSettings)
    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    auth: AuthSettings = Field(default_factory=AuthSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    compute: ComputeSettings = Field(default_factory=ComputeSettings)
    fabric: FabricSettings = Field(default_factory=FabricSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)
    retention: RetentionSettings = Field(default_factory=RetentionSettings)
    intelligence: IntelligenceSettings = Field(default_factory=IntelligenceSettings)

    @property
    def development_key_allowed(self) -> bool:
        """Whether the development runtime-signing key is trusted: the explicit ``runtime.trust.allow_development_key``, else true unless ``environment`` is production."""
        explicit = self.runtime.trust.allow_development_key
        return explicit if explicit is not None else self.environment.strip().lower() != "production"

    # ----- derived paths -------------------------------------------------------------
    @property
    def database_url(self) -> str:
        return self.db.url or f"sqlite:///{self.data_dir / 'olcf.db'}"

    @property
    def local_storage_root(self) -> Path:
        return Path(self.storage.local_root) if self.storage.local_root else self.data_dir / "storage"

    @property
    def base_url(self) -> str:
        if self.server.public_url:
            return self.server.public_url.rstrip("/")
        scheme = "https" if self.tls.enabled else "http"
        host = "localhost" if self.server.host in ("0.0.0.0", "::") else self.server.host  # noqa: S104
        return f"{scheme}://{host}:{self.server.port}"

    def path(self, *parts: str) -> Path:
        p = self.data_dir.joinpath(*parts)
        return p

    def ensure_dirs(self) -> None:
        for sub in ("", "secrets", "pki", "logs", "jobs", "runtimes", "storage", "backups", "tmp", "workers", "artifacts"):
            d = self.data_dir / sub
            d.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.data_dir / "secrets", 0o700)
            os.chmod(self.data_dir / "pki", 0o700)
        except OSError:  # pragma: no cover - Windows
            pass


def _deep_merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_settings(data_dir: str | os.PathLike[str] | None = None, **overrides: Any) -> Settings:
    """Load settings from ``config.toml`` in the data dir, then environment, then ``overrides``."""
    dd = Path(data_dir).expanduser().resolve() if data_dir else default_data_dir()
    file_values: dict[str, Any] = {}
    cfg = dd / "config.toml"
    if cfg.is_file():
        with cfg.open("rb") as fh:
            file_values = tomllib.load(fh)
    # Precedence, lowest to highest: defaults < config.toml < environment < explicit overrides. Every layer is merged KEY BY KEY: setting OLCF_SERVER__PORT replaces
    # only ``server.port``; the rest of the ``[server]`` table in config.toml still applies (it used to be dropped as a whole).
    env_values = Settings().model_dump(exclude_unset=True)  # only what the environment (or a .env) really set, nested models included
    merged = _deep_merge(_deep_merge(file_values, env_values), overrides)
    if "data_dir" not in overrides:
        merged["data_dir"] = str(dd)
    return Settings(**merged)


def write_default_config(settings: Settings) -> Path:
    """Write a commented starter ``config.toml`` if none exists."""
    path = settings.data_dir / "config.toml"
    if path.exists():
        return path
    path.write_text(
        "# Open Lakehouse Compute Fabric configuration (author: Justin Dodrill)\n"
        "# Every key may also be set via environment: OLCF_SERVER__PORT=9000\n\n"
        "[server]\n"
        'host = "127.0.0.1"   # use "0.0.0.0" only behind TLS + a firewall\n'
        "port = 8470\n"
        '# public_url = "https://olcf.example.com"\n\n'
        "[tls]\nenabled = false\n\n"
        "[db]\n# url = \"postgresql+psycopg://olcf:password@localhost/olcf\"\n\n"
        "[compute]\nembedded_worker = true\n\n"
        "[fabric]\nenabled = true\n# allowed_tenants = [\"<entra-tenant-guid>\"]\n"
        "# audiences = [\"api://<olcf-app-client-id>\"]\n",
        encoding="utf-8",
    )
    return path
