# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Cross-module data contracts (pydantic v2).

These are the types that travel between services, the REST API, the SDK, the worker
protocol and the Fabric execution capsule. Keep them backwards compatible: add optional
fields, never repurpose existing ones, bump ``schemaVersion`` for breaking changes.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, protected_namespaces=())


class OpenBody(BaseModel):
    """Base of REST request bodies whose *known* fields are typed and documented in OpenAPI while unknown keys still reach the service layer.

    The services validate the semantics themselves (and word the errors); the model gives generated clients and the documentation the field names and types.
    Handlers hand the service ``dump(body)``: only the keys the caller sent, so "absent" and "explicit null" keep their meaning.
    """

    model_config = ConfigDict(extra="allow", protected_namespaces=())


def dump(body: BaseModel | None) -> dict[str, Any]:
    """The keys the caller actually sent (extras included) as a plain dict; ``{}`` for an absent optional body."""
    return body.model_dump(exclude_unset=True) if body is not None else {}


class ItemType(StrEnum):
    """Kinds of workspace item: notebook, lakehouse, SQL query, pipeline, environment, Spark job definition, variable set, schedule, report and others.
    """
    NOTEBOOK = "notebook"
    LAKEHOUSE = "lakehouse"
    SQL_QUERY = "sql_query"
    PIPELINE = "pipeline"
    ENVIRONMENT = "environment"
    SPARK_JOB_DEFINITION = "spark_job_definition"
    VARIABLE_SET = "variable_set"
    SCHEDULE = "schedule"
    REPORT = "report"  # dashboards / reports driven by SQL against lakehouses
    EXTENSION = "extension"


class Role(StrEnum):
    """Built-in roles that can be bound to a principal: PlatformAdmin, SecurityAdmin, RuntimeAdmin, WorkspaceAdmin, Contributor, Operator, Viewer and ServicePrincipal.
    """
    PLATFORM_ADMIN = "PlatformAdmin"
    SECURITY_ADMIN = "SecurityAdmin"
    RUNTIME_ADMIN = "RuntimeAdmin"
    WORKSPACE_ADMIN = "WorkspaceAdmin"
    CONTRIBUTOR = "Contributor"  # "Developer/Contributor"
    OPERATOR = "Operator"
    VIEWER = "Viewer"
    SERVICE_PRINCIPAL = "ServicePrincipal"


class ScopeType(StrEnum):
    """Scope at which a role binding applies: organisation, workspace, item, connection, runtime or pool."""
    ORG = "org"
    WORKSPACE = "workspace"
    ITEM = "item"
    CONNECTION = "connection"
    RUNTIME = "runtime"
    POOL = "pool"


class JobKind(StrEnum):
    """Kinds of job: notebook, Spark job, SQL, Fabric capsule and script."""
    NOTEBOOK = "notebook"
    SPARK_JOB = "spark_job"
    SQL = "sql"
    CAPSULE = "capsule"  # Fabric execution capsule (whole notebook)
    SCRIPT = "script"


class OriginKind(StrEnum):
    """Where a job came from: standalone, Fabric, pipeline, schedule, API or MCP."""
    STANDALONE = "standalone"
    FABRIC = "fabric"
    PIPELINE = "pipeline"
    SCHEDULE = "schedule"
    API = "api"
    MCP = "mcp"


# ----------------------------------------------------------------------------- job contracts
class ResourceRequest(_Model):
    """Resources a job asks for: driver and executor cores and memory, executor counts, a cap on total cores and dynamic allocation."""
    driver_cores: int = Field(1, ge=1, le=256)
    driver_memory_mb: int = Field(1024, ge=256)
    executor_cores: int = Field(1, ge=1, le=256)
    executor_memory_mb: int = Field(1024, ge=256)
    min_executors: int = Field(1, ge=0)
    max_executors: int | None = Field(None, ge=1)
    #: Cap on total executor cores for the application (Spark ``spark.cores.max``).
    max_cores: int | None = Field(None, ge=1)
    dynamic_allocation: bool = False


class RuntimeRequest(_Model):
    """Which runtime a job needs: a runtime family or exact runtime id, version specifiers for Spark, Python, Java and Scala, and an environment or explicit dependency lock.
    """
    #: ``fabric-2.0-compatible`` | ``fabric-1.3-compatible`` | ``olcf-native`` | None (platform default)
    family: str | None = None
    runtime_id: str | None = None  # exact runtime (e.g. ``olcf-rt-2.0.0``)
    spark: str | None = None  # PEP 440-style specifier, e.g. ``>=4.1,<4.2``
    python: str | None = None
    java: str | None = None
    scala: str | None = None
    environment_id: str | None = None  # Environment item providing dependency lock + spark conf
    dependency_lock: dict[str, Any] | None = None  # explicit lock (hash-pinned) when no environment item
    spark_conf: dict[str, str] = Field(default_factory=dict)  # only keys on the approved list are honoured


class PolicySpec(_Model):
    """Scheduling policy of a job: queue, priority, timeout, retries and backoff, retention, placement constraints and whether the job is safe to replay.
    """
    queue: str = "default"
    priority: int = Field(50, ge=0, le=100)
    timeout_seconds: int | None = Field(None, ge=1)
    retries: int = Field(0, ge=0, le=10)
    retry_backoff_seconds: int = Field(30, ge=0)
    retention_days: int | None = None
    #: Placement constraints: ``pool``, ``labels`` (all must match), ``node`` (worker id/name), ``provider``
    placement: dict[str, Any] = Field(default_factory=dict)
    #: Declares the job safe to replay after infrastructure failure (idempotent writes).
    idempotent: bool = False
    #: Lets ``%pip install`` of packages that are not part of the declared environment install into a per-job layer. Off by default; the controller refuses
    #: it when the administrator capped ``environments.pip_policy_max`` below ``allow`` (and notebooks derive it from their effective pip policy).
    allow_pip_install: bool = False


class JobSource(_Model):
    """The code a job runs: its format (notebook, Python, SQL, Scala, jar, script), inline content or a staged artifact, file name and checksum.
    """
    format: Literal["ipynb", "py", "fabric_git_source", "sql", "scala", "jar", "script"] = "ipynb"
    content: str | None = None  # text (ipynb JSON text, .py, SQL, shell)
    filename: str | None = None
    sha256: str | None = None
    artifact_uri: str | None = None  # large sources / jars staged in storage
    #: Executable metadata of a Spark application (not notebook parameters): the jar's entry point and its command-line arguments. A Spark job definition item carries
    #: the same two fields; both end up in the normalised job spec as ``source.main_class`` / ``source.args``.
    main_class: str | None = None
    args: list[str] = Field(default_factory=list)


class LakehouseRef(_Model):
    """Reference to a lakehouse. ``id`` is accepted everywhere as an alias of ``lakehouse_id`` (item definitions and older clients spell it ``id``)."""

    lakehouse_id: str | None = None
    name: str | None = None
    workspace_id: str | None = None  # OLCF workspace id
    #: Foreign (Fabric) identifiers when the reference originated in Fabric.
    fabric_workspace_id: str | None = None
    fabric_lakehouse_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _accept_id_alias(cls, data: Any) -> Any:
        if isinstance(data, dict) and "id" in data:
            data = dict(data)
            ident = data.pop("id")
            if data.get("lakehouse_id") not in (None, ident) and ident is not None:
                raise ValueError("lakehouse reference has both 'id' and a different 'lakehouse_id'")
            if ident is not None:
                data["lakehouse_id"] = ident
        return data


class DataRef(_Model):
    """A data reference a job declares: kind (lakehouse, warehouse, catalog or path), the reference, its role (default, read or write) and the resolved location.
    """
    kind: Literal["lakehouse", "warehouse", "catalog", "path"]
    ref: str
    role: Literal["default", "read", "write"] = "read"
    resolved: str | None = None


class Origin(_Model):
    """Provenance of a job: the kind of origin plus free-form details."""
    kind: OriginKind = OriginKind.API
    details: dict[str, Any] = Field(default_factory=dict)


class JobSubmission(_Model):
    """Request body for submitting a job: kind, workspace, source, parameters, default lakehouse, data references, runtime, resources and policy.
    """
    kind: JobKind
    workspace_id: str
    name: str | None = None
    item_id: str | None = None  # notebook / spark job definition item
    source: JobSource | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    default_lakehouse: LakehouseRef | None = None
    data_refs: list[DataRef] = Field(default_factory=list)
    runtime: RuntimeRequest = Field(default_factory=RuntimeRequest)
    resources: ResourceRequest = Field(default_factory=ResourceRequest)
    policy: PolicySpec = Field(default_factory=PolicySpec)
    origin: Origin = Field(default_factory=Origin)
    idempotency_key: str | None = None
    #: Free-form compatibility report captured at validation (analyzer output).
    compatibility: dict[str, Any] | None = None
    parent_run_id: str | None = None  # pipeline activity run
    engine: Literal["spark", "python"] = "spark"
    env: dict[str, str] = Field(default_factory=dict)  # non-secret environment variables
    secret_refs: dict[str, str] = Field(default_factory=dict)  # ENV_NAME -> secret id, resolved just-in-time
    tags: dict[str, str] = Field(default_factory=dict)


class FailureInfo(_Model):
    """Structured description of a job failure: category, message, cell and line where known, traceback, remediation and details."""
    category: str
    message: str
    cell: int | None = None
    line: int | None = None
    traceback: str | None = None
    remediation: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class JobRecord(_Model):
    """A job as the API returns it: identity, kind, state, workspace, origin, queue, timing, resources, result and failure information."""
    id: str
    kind: JobKind
    state: str
    workspace_id: str
    name: str | None = None
    item_id: str | None = None
    submitted_by: str | None = None
    origin: Origin = Field(default_factory=Origin)
    queue: str = "default"
    priority: int = 50
    attempt: int = 1
    runtime_id: str | None = None
    runtime_fingerprint: str | None = None
    source_hash: str | None = None
    provider: str | None = None
    application_id: str | None = None  # Spark application id
    driver_worker_id: str | None = None
    resources_granted: dict[str, Any] = Field(default_factory=dict)
    parameters: dict[str, Any] = Field(default_factory=dict)
    exit_value: Any = None
    result: dict[str, Any] = Field(default_factory=dict)
    failure: FailureInfo | None = None
    parent_run_id: str | None = None
    correlation_id: str | None = None
    created_at: str
    updated_at: str
    queued_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    timeout_at: str | None = None
    cancel_requested_at: str | None = None


# ----------------------------------------------------------------------------- pagination
class Page(BaseModel):
    """One page of a list response: the items, a cursor for the next page and an optional total."""
    items: list[Any]
    next_cursor: str | None = None
    total: int | None = None


# ----------------------------------------------------------------------------- workspaces/items
class WorkspaceCreate(_Model):
    """Request body for creating a workspace: name, display name, description, tags, settings and quota."""
    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9._ -]*$")
    display_name: str | None = None
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    settings: dict[str, Any] = Field(default_factory=dict)
    quota: dict[str, Any] = Field(default_factory=dict)


class ItemCreate(_Model):
    """Request body for creating an item: type, name, description, folder, tags, sensitivity label and definition."""
    type: ItemType
    name: str = Field(min_length=1, max_length=128)
    description: str = ""
    folder: str = ""
    tags: list[str] = Field(default_factory=list)
    sensitivity: str | None = None
    definition: dict[str, Any] = Field(default_factory=dict)


class ItemUpdate(_Model):
    """Request body for updating an item. Only the fields that are set change; ``comment`` is stored as the version history note."""
    name: str | None = None
    description: str | None = None
    folder: str | None = None
    tags: list[str] | None = None
    sensitivity: str | None = None
    definition: dict[str, Any] | None = None
    comment: str | None = None  # version history note
