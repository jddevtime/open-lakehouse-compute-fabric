# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Wire protocol between the controller hub and worker agents (``WORKER_PROTOCOL_VERSION = "1.0"``).

Normative description: ``docs/dev/CONTRACTS.md`` section 4.2 plus the "Controller addenda (CTRL)". This module is the
single place on the controller side that knows frame shapes. Parsing is *strict about structure* (every frame must be a JSON
object with ``v``, ``type`` and an object ``payload``; payloads of agent -> controller frames are validated with pydantic
models) and *tolerant about additions* (unknown payload keys are preserved so newer agents keep working).

Frame::

    {"v": 1, "type": "heartbeat", "id": "<uuid, requests only>", "ref": "<id being answered>", "ts": "...", "payload": {...}}
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from olcf.core.timeutil import now_iso

WORKER_PROTOCOL_VERSION = "1.0"
FRAME_VERSION = 1
#: Largest frame the controller accepts from an agent (job_log chunks are <= 64 KiB; hello/heartbeat are small).
MAX_AGENT_FRAME_BYTES = 8 * 1024 * 1024
#: Largest frame the controller will *send* (run_job carries the job source). Agents must accept at least this much.
MAX_CONTROLLER_FRAME_BYTES = 24 * 1024 * 1024
MAX_LOG_CHUNK_BYTES = 64 * 1024

# --------------------------------------------------------------------------- frame types
#: agent -> controller
AGENT_TYPES = frozenset({"hello", "heartbeat", "result", "job_state", "job_log", "job_event", "kernel_event", "daemon_state", "pong",
                         "renew_cert"})  # renew_cert: additive, answered with data (CONTRACTS 4.2 "Credential renewal")
#: controller -> agent commands (each carries an ``id`` and is answered by ``result{ref=id}``)
COMMAND_TYPES = frozenset({
    "ping", "run_job", "cancel_job", "start_spark_master", "stop_spark_master", "start_spark_worker", "stop_spark_worker",
    "start_kernel", "kernel_execute", "kernel_interrupt", "kernel_restart", "kernel_shutdown", "kernel_complete", "probe",
    "install_runtime", "drain", "collect_diagnostics", "shutdown",
    "fetch_artifact",  # additive: paged read of a file in a job's work dir (artifacts too large to travel inline in job_state)
    "renew_credentials",  # additive: the operator asked this node to renew its certificate / token now (the agent answers with a renew_cert exchange)
})
#: controller -> agent non-command frames
CONTROL_TYPES = frozenset({"error", "result"})

JOB_TERMINAL_AGENT_STATES = frozenset({"succeeded", "failed", "cancelled", "timed_out"})
JOB_AGENT_STATES = JOB_TERMINAL_AGENT_STATES | {"running"}


class ProtocolError(Exception):
    """A frame violated the protocol. ``fatal`` frames close the connection (policy violation)."""

    def __init__(self, message: str, *, code: str = "protocol_error", fatal: bool = False, ref: str | None = None) -> None:
        super().__init__(message)
        self.message, self.code, self.fatal, self.ref = message, code, fatal, ref


@dataclass
class Frame:
    """One message of the worker protocol: type, payload, optional id and reference, timestamp and frame version; serialised as a JSON text frame.
    """
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    id: str | None = None
    ref: str | None = None
    ts: str = field(default_factory=now_iso)
    v: int = FRAME_VERSION

    def encode(self) -> str:
        out: dict[str, Any] = {"v": self.v, "type": self.type, "ts": self.ts, "payload": self.payload}
        if self.id:
            out["id"] = self.id
        if self.ref:
            out["ref"] = self.ref
        return json.dumps(out, separators=(",", ":"), default=str)


def new_request_id() -> str:
    return uuid.uuid4().hex


def command(type_: str, payload: dict[str, Any] | None = None) -> Frame:
    """Build a controller command frame (with a fresh request id)."""
    if type_ not in COMMAND_TYPES:
        raise ValueError(f"unknown command type {type_!r}")
    return Frame(type=type_, payload=payload or {}, id=new_request_id())


def result_frame(ref: str, ok: bool, data: Any = None, error: dict[str, Any] | None = None) -> Frame:
    """Controller's acknowledgement of an agent frame that carried an ``id`` (hello, job_state ...)."""
    p: dict[str, Any] = {"ok": ok}
    if data is not None:
        p["data"] = data
    if error is not None:
        p["error"] = error
    return Frame(type="result", payload=p, ref=ref)


def error_frame(code: str, message: str, ref: str | None = None) -> Frame:
    return Frame(type="error", payload={"code": code, "message": message}, ref=ref)


def decode(text: str | bytes) -> Frame:
    """Parse and structurally validate one inbound frame."""
    size = len(text)
    if size > MAX_AGENT_FRAME_BYTES:
        raise ProtocolError(f"frame too large ({size} bytes)", code="frame_too_large", fatal=True)
    try:
        obj = json.loads(text)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ProtocolError(f"frame is not valid JSON: {exc}", code="bad_json") from exc
    if not isinstance(obj, dict):
        raise ProtocolError("frame must be a JSON object", code="bad_frame")
    v = obj.get("v", FRAME_VERSION)
    if v != FRAME_VERSION:
        raise ProtocolError(f"unsupported frame version {v!r}", code="unsupported_version", fatal=True)
    type_ = obj.get("type")
    if not isinstance(type_, str) or not type_:
        raise ProtocolError("frame has no 'type'", code="bad_frame")
    payload = obj.get("payload", {})
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise ProtocolError("frame payload must be an object", code="bad_frame", ref=obj.get("id"))
    fid, ref = obj.get("id"), obj.get("ref")
    for name, val in (("id", fid), ("ref", ref)):
        if val is not None and (not isinstance(val, str) or len(val) > 128):
            raise ProtocolError(f"frame '{name}' must be a string <= 128 chars", code="bad_frame")
    ts = obj.get("ts")
    return Frame(type=type_, payload=payload, id=fid, ref=ref, ts=ts if isinstance(ts, str) else now_iso(), v=v)


# --------------------------------------------------------------------------- agent -> controller payloads
class _Payload(BaseModel):
    model_config = ConfigDict(extra="allow")


class ActiveJob(_Payload):
    """A job process an agent reports as running, in its ``hello`` and heartbeats."""
    job_id: str
    pid: int | None = None
    state: str = "running"
    attempt: int | None = None


class KernelInfo(_Payload):
    """A notebook kernel an agent reports as running."""
    session_id: str
    pid: int | None = None
    state: str | None = None


class DaemonInfo(_Payload):
    """A Spark master or worker daemon an agent reports, with its runtime, process id, URLs and resources."""
    role: Literal["master", "worker"]
    runtime_id: str
    pid: int | None = None
    url: str | None = None
    state: str | None = None
    master_url: str | None = None
    cores: int | None = None
    memory_mb: int | None = None
    port: int | None = None
    ui_port: int | None = None


class RuntimeStatus(_Payload):
    """The state of one installed runtime on a node: ready or not, verified or not, and its fingerprint."""
    id: str
    state: str = "ready"
    verified: bool = False
    fingerprint: str | None = None
    detail: str | None = None


class Component(_Payload):
    """The health of one named component of an agent: a status and a detail."""
    name: str
    status: str = "ok"
    detail: str = ""


class Hello(_Payload):
    """The first frame an agent sends after connecting: identity, versions, inventory and everything it is running, which the controller reconciles against.
    """
    worker_id: str | None = None
    agent_version: str = "unknown"
    protocol: str = WORKER_PROTOCOL_VERSION
    inventory: dict[str, Any] = Field(default_factory=dict)
    active_jobs: list[ActiveJob] = Field(default_factory=list)
    kernels: list[KernelInfo] = Field(default_factory=list)
    spark_daemons: list[DaemonInfo] = Field(default_factory=list)
    runtimes: list[RuntimeStatus] = Field(default_factory=list)
    embedded: bool = False


class Metrics(_Payload):
    """Machine metrics an agent reports in each heartbeat: CPU, load, memory, disk and network rates."""
    cpu_percent: float | None = None
    load1: float | None = None
    mem_total_mb: float | None = None
    mem_available_mb: float | None = None
    disk_total_gb: float | None = None
    disk_free_gb: float | None = None
    net_rx_bps: float | None = None
    net_tx_bps: float | None = None


class Heartbeat(_Payload):
    """The periodic agent frame: metrics plus, when they changed, runtimes, component health, running jobs and Spark daemons."""
    ts: str | None = None
    metrics: Metrics = Field(default_factory=Metrics)
    runtimes: list[RuntimeStatus] | None = None
    components: list[Component] | None = None
    active_jobs: list[ActiveJob] | None = None
    spark_daemons: list[DaemonInfo] | None = None


class JobStateMsg(_Payload):
    """An agent's report that a job it runs changed state, with exit code, result or classified failure."""
    job_id: str
    state: Literal["running", "succeeded", "failed", "cancelled", "timed_out"]
    attempt: int | None = None
    exit_code: int | None = None
    result: dict[str, Any] | None = None
    failure: dict[str, Any] | None = None


class JobLogMsg(_Payload):
    """A numbered chunk of a job's standard output or standard error."""
    job_id: str
    stream: Literal["stdout", "stderr"] = "stdout"
    seq: int = Field(ge=0)
    data: str
    attempt: int | None = None


class JobEventMsg(_Payload):
    """A named lifecycle event of a job on the agent, with its data."""
    job_id: str
    event: str
    data: dict[str, Any] = Field(default_factory=dict)


class KernelEventMsg(_Payload):
    """A message from a notebook kernel (output, status or reply) forwarded to the controller."""
    session_id: str
    channel: Literal["iopub", "shell", "status"] = "iopub"
    content: dict[str, Any] = Field(default_factory=dict)
    msg_type: str | None = None
    parent_msg_id: str | None = None


class DaemonStateMsg(_Payload):
    """An agent's report that a Spark master or worker daemon changed state."""
    role: Literal["master", "worker"]
    runtime_id: str
    state: str
    detail: str | None = None


class RenewCert(_Payload):
    """``renew_cert``: the agent asks for a new machine certificate (``csr_pem``, key generated on the node) and/or reports that its worker token may need rotation.

    ``reason`` is ``scheduled`` (the agent's own timer), ``requested`` (the operator ran ``olcf worker renew``) or ``startup``; ``cert`` is what the agent holds
    (``serial``, ``not_after``). The controller decides what to issue; the answer is a ``result`` with data (CONTRACTS 4.2).
    """
    csr_pem: str | None = Field(default=None, max_length=16384)
    reason: str = Field(default="scheduled", max_length=32)
    cert: dict[str, Any] | None = None


class ResultMsg(_Payload):
    """The answer to a controller command: success with data, or an error."""
    ok: bool
    data: Any = None
    error: Any = None


_MODELS: dict[str, type[BaseModel]] = {
    "hello": Hello, "heartbeat": Heartbeat, "job_state": JobStateMsg, "job_log": JobLogMsg, "job_event": JobEventMsg,
    "kernel_event": KernelEventMsg, "daemon_state": DaemonStateMsg, "result": ResultMsg, "renew_cert": RenewCert,
}


def parse_payload(frame: Frame) -> BaseModel | dict[str, Any]:
    """Validate ``frame.payload`` for its type. Raises :class:`ProtocolError` (never a pydantic error)."""
    model = _MODELS.get(frame.type)
    if model is None:
        return frame.payload
    try:
        return model.model_validate(frame.payload)
    except ValidationError as exc:
        errs = "; ".join(f"{'.'.join(str(x) for x in e['loc'])}: {e['msg']}" for e in exc.errors()[:5])
        raise ProtocolError(f"invalid '{frame.type}' payload: {errs}", code="bad_payload", ref=frame.id, fatal=frame.type == "hello") from exc


# --------------------------------------------------------------------------- inventory normalisation
def _first(d: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if d.get(k) not in (None, ""):
            return d[k]
    return None


def normalize_inventory(inv: dict[str, Any] | None) -> dict[str, Any]:
    """Map the (slightly varying) inventory spellings agents may use to the canonical keys.

    Canonical: ``cores, memory_mb, disk_gb, os, arch, hostname, address, addresses, gpus, python, accelerators, labels``.
    """
    inv = dict(inv or {})
    out = dict(inv)
    cores = _first(inv, "cores", "cpu_cores", "cpu_count", "logical_cores", "cpus")
    mem = _first(inv, "memory_mb", "mem_total_mb", "memory_total_mb", "ram_mb")
    if mem is None and _first(inv, "memory_gb", "ram_gb") is not None:
        mem = float(_first(inv, "memory_gb", "ram_gb")) * 1024
    disk = _first(inv, "disk_gb", "disk_total_gb")
    out["cores"] = int(cores) if cores is not None else 0
    out["memory_mb"] = int(mem) if mem is not None else 0
    out["disk_gb"] = float(disk) if disk is not None else 0.0
    addrs = inv.get("addresses") if isinstance(inv.get("addresses"), list) else []
    addr = _first(inv, "address", "ip", "ip_address") or (addrs[0] if addrs else None)
    if addr:
        out["address"] = str(addr)
    out["addresses"] = [str(a) for a in addrs] if addrs else ([str(addr)] if addr else [])
    out["os"] = _first(inv, "os", "platform", "system")
    out["arch"] = _first(inv, "arch", "machine", "architecture")
    out["gpus"] = inv.get("gpus") if isinstance(inv.get("gpus"), list) else []
    return out
