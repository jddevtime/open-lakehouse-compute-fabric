# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Kubernetes provider: a driver pod per job; Spark creates executor pods itself (``k8s://`` master).

Execution (``submit_mode``)
---------------------------
``driver_pod`` (default)
    The controller creates a *Secret* holding ``job.json`` and the job source, a headless *Service* for the driver and a *Pod*
    that runs the OLCF runner inside the runtime image. The runner starts Spark in in-cluster **client mode**
    (``spark.master=k8s://https://kubernetes.default.svc``, ``spark.kubernetes.driver.pod.name`` = the pod, so executor pods are owned
    by it and garbage collected with it).

    Volumes: the Secret is mounted **read-only at** ``/opt/olcf/input`` (Secret volumes always are); the runner stages it into the writable ``emptyDir`` at
    ``/opt/olcf/work`` (``--workdir``), deletes the staged ``job.json`` when it is done, and runs as a non-root user (``runAsNonRoot``, ``fsGroup``, no privilege
    escalation, no capabilities, ``RuntimeDefault`` seccomp). ``/tmp`` is an ``emptyDir`` too, so ``read_only_root_filesystem: true`` is possible.
``spark_submit``
    The controller runs ``spark-submit --deploy-mode cluster --master k8s://...`` (binary from ``spark_submit_path``) with the same Secret
    mounted into the driver (``spark.kubernetes.driver.secrets.<name>``); the main resource is ``entrypoint`` (default
    ``local:///opt/olcf/entrypoint.py`` provided by the runtime image).

Cluster access uses the ``kubernetes`` Python client when installed, otherwise ``kubectl`` (``kube_backend`` = auto|python|kubectl,
kubeconfig from the OLCF secret store). **Status: contract-tested against a test KubeClient and a kubectl stub.**

Image contract: the image contains the runtime venv with ``olcf`` installed (``--no-deps``) so ``runner_command`` (default
``python -m olcf.runtime_lib.runner --workdir /opt/olcf/work /opt/olcf/input/job.json``; ``{input_dir}`` and ``{work_dir}`` are substituted) works.

Completion contract (``result_contract = "runner"``; CONTRACTS, PROV addenda): after persisting ``result.json`` the runner uploads the declared artifacts, prints **one**
``@@OLCF@@ {"event":"result","job_id","attempt","result":{...}}`` line and leaves a compact record in the termination log (``state.terminated.message``). The
controller reads the log incrementally (a timestamp cursor, see :meth:`KubernetesProvider.logs`), then, if a terminal pod's result was not seen, looks again at the
log tail (:meth:`KubernetesProvider.find_result`) and the termination record before it reports the job as failed with ``result_missing`` - never as a success.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod
from calendar import timegm
from dataclasses import dataclass
from typing import Any

from olcf.compute.errors import ProviderError
from olcf.compute.providers.base import (
    AppHandle,
    AppStatus,
    ComputeProvider,
    Discovery,
    LogsPage,
    PrepareRequest,
    SubmitRequest,
    UiInfo,
    base_spark_conf,
    dynamic_allocation_conf,
    spark_memory,
)
from olcf.core.errors import BadRequest, Unsupported

log = logging.getLogger("olcf.compute.kubernetes")

SECRET_LIMIT_BYTES = 900 * 1024  # k8s caps a Secret at 1 MiB (including base64 overhead)
INPUT_DIR = "/opt/olcf/input"  # the Secret volume (read-only)
WORK_DIR = "/opt/olcf/work"  # emptyDir: the runner's working directory
DEFAULT_RUNNER_COMMAND = ["python", "-m", "olcf.runtime_lib.runner", "--workdir", "{work_dir}", "{input_dir}/job.json"]
DEFAULT_UID = 10001
#: Log reading: a page is fetched with ``limit + LOG_SLACK_BYTES`` so that a page of whole lines fits; the window grows (x4, at most ``LOG_GROW_STEPS`` times) when a
#: burst within one second hides the cursor's anchor line.
LOG_SLACK_BYTES = 64 * 1024
LOG_GROW_STEPS = 4
LOG_MAX_WINDOW_BYTES = 64 * 1024 * 1024
LABEL_APP = "olcf-spark-driver"
_NAME_BAD = re.compile(r"[^a-z0-9-]")


def k8s_name(job_id: str, attempt: int) -> str:
    """DNS-1123 compatible, deterministic pod/service/secret name for a job attempt."""
    base = _NAME_BAD.sub("-", job_id.lower().replace("_", "-"))
    return f"olcf-{base}-a{attempt}"[:63].rstrip("-")


# =========================================================================== log cursor
_TS_LINE = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z(?: (.*))?$", re.S)


@dataclass(frozen=True)
class LogEntry:
    sec: int  # epoch seconds
    nsec: int
    text: str

    @property
    def key(self) -> tuple[int, int]:
        return (self.sec, self.nsec)


def parse_timestamped_log(raw: str) -> list[LogEntry]:
    """Split ``kubectl logs --timestamps`` output into entries. A line without a timestamp prefix continues the previous entry (a long line the runtime split)."""
    out: list[LogEntry] = []
    for line in raw.split("\n"):
        m = _TS_LINE.match(line)
        if m:
            sec = timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S"))
            out.append(LogEntry(sec, int((m.group(2) or "0").ljust(9, "0")), m.group(3) or ""))
        elif out and line:
            last = out.pop()
            out.append(LogEntry(last.sec, last.nsec, last.text + "\n" + line))
    return out


def _iso_seconds(sec: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(sec))


def _anchor(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", "replace"), usedforsecurity=False).hexdigest()[:16]


def advance_log_cursor(entries: list[LogEntry], cursor: dict[str, Any] | None) -> tuple[list[LogEntry], bool, str | None]:
    """The entries of ``entries`` (a window read from ``cursor.s``) that come *after* the cursor, whether the cursor's anchor line was found (always true for no cursor),
    and a gap note when the retained log no longer contains it.

    The cursor is ``{"v":1,"s":sec,"ns":nsec,"n":k,"h":hash}``: the timestamp of the last line consumed, how many lines with exactly that timestamp were consumed, and a
    hash of the last one. Several lines can share a timestamp, so the count says where to continue and the hash proves the line there is the one we consumed (it is
    searched for when the count disagrees, e.g. after the oldest lines of that instant were rotated away)."""
    if cursor is None:
        return entries, True, None
    key = (int(cursor["s"]), int(cursor["ns"]))
    same = [e for e in entries if e.key == key]
    after = [e for e in entries if e.key > key]
    n = int(cursor.get("n") or 0)
    if n and len(same) >= n and _anchor(same[n - 1].text) == cursor.get("h"):
        return same[n:] + after, True, None
    for i in range(len(same) - 1, -1, -1):
        if _anchor(same[i].text) == cursor.get("h"):
            return same[i + 1:] + after, True, None
    if not entries:
        return [], False, None
    # the line we stopped at is not in the window. Either the window is too small (the caller grows it) or the log was truncated/rotated beneath us.
    first = entries[0]
    note = (f"the retained log no longer contains the line at {_iso_seconds(key[0])} where reading stopped; output between it and {_iso_seconds(first.sec)} may be missing")
    return after if after else [], False, note


class AlreadyExists(ProviderError):
    """``create`` hit an object of the same name (HTTP 409 ``AlreadyExists``). Creation is made idempotent by adopting an object that belongs to the same attempt."""

    code = "already_exists"


# =========================================================================== cluster clients
class KubeClient(ABC):
    """The slice of the Kubernetes API this provider needs (implemented by the python client and by kubectl)."""

    @abstractmethod
    def create(self, manifest: dict[str, Any]) -> dict[str, Any]: ...

    @abstractmethod
    def get(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None: ...

    @abstractmethod
    def list(self, kind: str, namespace: str, selector: str) -> list[dict[str, Any]]: ...

    @abstractmethod
    def delete(self, kind: str, namespace: str, name: str, grace_seconds: int | None = None) -> None: ...

    @abstractmethod
    def pod_log(self, namespace: str, pod: str, *, limit_bytes: int = 8388608, since_time: str | None = None, timestamps: bool = False, tail_lines: int | None = None) -> str:
        """The container log: at most ``limit_bytes`` of it (a cut may fall inside a line), from ``since_time`` (RFC 3339; the API resolves whole seconds), each line prefixed
        with its RFC 3339 timestamp when ``timestamps``, or only the last ``tail_lines`` lines."""

    @abstractmethod
    def version(self) -> str: ...


class KubectlClient(KubeClient):
    """``kubectl`` backend (no Python dependency). All arguments are passed as an argv list (never through a shell)."""

    def __init__(self, kubectl: str = "kubectl", kubeconfig_text: str | None = None, context: str | None = None, timeout: float = 30.0) -> None:
        self.kubectl, self.context, self.timeout = kubectl, context, timeout
        self._kubeconfig: str | None = None
        if kubeconfig_text:
            fd, path = tempfile.mkstemp(prefix="olcf-kubeconfig-")
            with os.fdopen(fd, "w") as fh:
                fh.write(kubeconfig_text)
            os.chmod(path, 0o600)
            self._kubeconfig = path

    def close(self) -> None:
        if self._kubeconfig and os.path.exists(self._kubeconfig):
            os.unlink(self._kubeconfig)

    def _run(self, args: list[str], *, stdin: str | None = None, ok_codes: tuple[int, ...] = (0,)) -> subprocess.CompletedProcess[str]:
        cmd = [self.kubectl]
        if self._kubeconfig:
            cmd += ["--kubeconfig", self._kubeconfig]
        if self.context:
            cmd += ["--context", self.context]
        try:
            p = subprocess.run(cmd + args, input=stdin, capture_output=True, text=True, timeout=self.timeout, check=False)  # noqa: S603
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ProviderError(f"kubectl failed to run: {exc}") from exc
        if p.returncode not in ok_codes:
            raise ProviderError(f"kubectl {' '.join(args[:3])} failed: {p.stderr.strip()[:400]}")
        return p

    def create(self, manifest: dict[str, Any]) -> dict[str, Any]:
        try:
            p = self._run(["create", "-f", "-", "-o", "json"], stdin=json.dumps(manifest))
        except ProviderError as exc:
            if "AlreadyExists" in str(exc) or "already exists" in str(exc):
                raise AlreadyExists(str(exc)) from exc
            raise
        return json.loads(p.stdout or "{}")

    def get(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None:
        p = self._run(["get", kind, name, "-n", namespace, "-o", "json", "--ignore-not-found"])
        return json.loads(p.stdout) if p.stdout.strip() else None

    def list(self, kind: str, namespace: str, selector: str) -> list[dict[str, Any]]:
        p = self._run(["get", kind, "-n", namespace, "-l", selector, "-o", "json"])
        return list((json.loads(p.stdout or "{}")).get("items") or [])

    def delete(self, kind: str, namespace: str, name: str, grace_seconds: int | None = None) -> None:
        args = ["delete", kind, name, "-n", namespace, "--ignore-not-found", "--wait=false"]
        if grace_seconds is not None:
            args.append(f"--grace-period={int(grace_seconds)}")
        self._run(args)

    def pod_log(self, namespace: str, pod: str, *, limit_bytes: int = 8388608, since_time: str | None = None, timestamps: bool = False, tail_lines: int | None = None) -> str:
        args = ["logs", pod, "-n", namespace, f"--limit-bytes={int(limit_bytes)}"]
        if since_time:
            args.append(f"--since-time={since_time}")
        if timestamps:
            args.append("--timestamps")
        if tail_lines is not None:
            args.append(f"--tail={int(tail_lines)}")
        # exit code 1 = the container has not started yet (no log to read), which is not an error for a poller
        return self._run(args, ok_codes=(0, 1)).stdout

    def version(self) -> str:
        p = self._run(["version", "-o", "json"])
        return (json.loads(p.stdout or "{}").get("serverVersion") or {}).get("gitVersion", "unknown")


class PythonKubeClient(KubeClient):
    """Backend using the official ``kubernetes`` package (installed with ``pip install olcf[k8s]``)."""

    def __init__(self, kubeconfig_text: str | None = None, context: str | None = None) -> None:
        try:
            from kubernetes import client, config
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ProviderError("the 'kubernetes' package is not installed (pip install olcf[k8s]) and kube_backend=python was requested") from exc
        if kubeconfig_text:
            import yaml

            config.load_kube_config_from_dict(yaml.safe_load(kubeconfig_text), context=context)
        else:
            try:
                config.load_incluster_config()
            except Exception:  # noqa: BLE001
                config.load_kube_config(context=context)
        self._core, self._api_client = client.CoreV1Api(), client.ApiClient()  # pragma: no cover

    def _kind_api(self, kind: str):  # noqa: ANN202  # pragma: no cover
        return {"pod": ("pod", "pods"), "service": ("service", "services"), "secret": ("secret", "secrets")}[kind.lower()]

    def create(self, manifest: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover
        from kubernetes.client.rest import ApiException

        kind, ns = manifest["kind"].lower(), manifest["metadata"].get("namespace", "default")
        fn = getattr(self._core, f"create_namespaced_{self._kind_api(kind)[0]}")
        try:
            return self._api_client.sanitize_for_serialization(fn(ns, manifest))
        except ApiException as exc:
            if exc.status == 409:
                raise AlreadyExists(f"{kind} {manifest['metadata']['name']} already exists") from exc
            raise ProviderError(f"kubernetes API error creating {kind}: {exc.reason}") from exc

    def get(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None:  # pragma: no cover
        from kubernetes.client.rest import ApiException

        try:
            return self._api_client.sanitize_for_serialization(getattr(self._core, f"read_namespaced_{self._kind_api(kind)[0]}")(name, namespace))
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise ProviderError(f"kubernetes API error: {exc.reason}") from exc

    def list(self, kind: str, namespace: str, selector: str) -> list[dict[str, Any]]:  # pragma: no cover
        res = getattr(self._core, f"list_namespaced_{self._kind_api(kind)[0]}")(namespace, label_selector=selector)
        return [self._api_client.sanitize_for_serialization(i) for i in res.items]

    def delete(self, kind: str, namespace: str, name: str, grace_seconds: int | None = None) -> None:  # pragma: no cover
        from kubernetes.client.rest import ApiException

        try:
            kwargs = {"grace_period_seconds": grace_seconds} if grace_seconds is not None and kind.lower() == "pod" else {}
            getattr(self._core, f"delete_namespaced_{self._kind_api(kind)[0]}")(name, namespace, **kwargs)
        except ApiException as exc:
            if exc.status != 404:
                raise ProviderError(f"kubernetes API error: {exc.reason}") from exc

    def pod_log(self, namespace: str, pod: str, *, limit_bytes: int = 8388608, since_time: str | None = None, timestamps: bool = False,
                tail_lines: int | None = None) -> str:  # pragma: no cover
        from kubernetes.client.rest import ApiException

        kwargs: dict[str, Any] = {"limit_bytes": limit_bytes, "timestamps": timestamps}
        if tail_lines is not None:
            kwargs["tail_lines"] = int(tail_lines)
        if since_time:
            # the python client only offers ``since_seconds``: ask for slightly more (the caller de-duplicates by timestamp) and never less
            import calendar

            head = since_time.rstrip("Z").split(".")[0]
            kwargs["since_seconds"] = max(1, int(time.time() - calendar.timegm(time.strptime(head, "%Y-%m-%dT%H:%M:%S"))) + 2)
        try:
            return self._core.read_namespaced_pod_log(pod, namespace, **kwargs)
        except ApiException as exc:
            if exc.status in (400, 404):
                return ""
            raise ProviderError(f"kubernetes API error: {exc.reason}") from exc

    def version(self) -> str:  # pragma: no cover
        from kubernetes import client

        return client.VersionApi().get_code().git_version


# =========================================================================== provider
def build_spark_submit_args(cfg: dict[str, Any], *, pod_name: str, secret_name: str, conf: dict[str, str], entrypoint: str) -> list[str]:
    """argv for ``spark-submit`` in cluster mode against a Kubernetes master (used by ``submit_mode: spark_submit``)."""
    master = cfg.get("api_server") or "https://kubernetes.default.svc"
    args = [cfg.get("spark_submit_path") or "spark-submit", "--master", f"k8s://{master}", "--deploy-mode", "cluster", "--name", conf.get("spark.app.name", pod_name)]
    merged = dict(conf)
    merged["spark.kubernetes.driver.pod.name"] = pod_name
    merged[f"spark.kubernetes.driver.secrets.{secret_name}"] = INPUT_DIR  # read-only
    merged["spark.kubernetes.driver.volumes.emptyDir.olcf-work.mount.path"] = WORK_DIR  # the entrypoint stages the input here (runner --workdir)
    merged["spark.kubernetes.driver.volumes.emptyDir.olcf-work.mount.readOnly"] = "false"
    for k, v in sorted(merged.items()):
        args += ["--conf", f"{k}={v}"]
    args.append(entrypoint)
    return args


class KubernetesProvider(ComputeProvider):
    """Runs Spark applications as driver and executor pods on a Kubernetes cluster."""
    type = "kubernetes"
    live_verified = False
    execution_model = "remote"
    capabilities = ("kubernetes", "remote", "dynamic-allocation", "reconcile", "executor-pods")
    #: the runner in the pod reports the result (``@@OLCF@@`` ``result`` event + termination record); the controller never synthesises one
    result_contract = "runner"

    def __init__(self, *args: Any, kube: KubeClient | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._kube = kube

    @property
    def execution(self) -> str:
        return "remote"

    # ---- configuration --------------------------------------------------------------------------
    @property
    def namespace(self) -> str:
        return str(self.config.get("namespace") or "olcf")

    def validate_config(self) -> None:
        if not self.config.get("image") and not self.config.get("images"):
            raise BadRequest("kubernetes provider needs 'image' (or 'images': {runtime_id: image}) containing the OLCF runtime")
        if self.config.get("submit_mode", "driver_pod") not in ("driver_pod", "spark_submit"):
            raise BadRequest("submit_mode must be 'driver_pod' or 'spark_submit'")
        if self.config.get("kube_backend", "auto") not in ("auto", "python", "kubectl"):
            raise BadRequest("kube_backend must be auto, python or kubectl")

    def close(self) -> None:
        fn = getattr(self._kube, "close", None)
        if callable(fn):
            fn()

    def image_for(self, runtime_id: str | None) -> str:
        imgs = self.config.get("images") or {}
        img = imgs.get(runtime_id or "") or self.config.get("image")
        if not img:
            raise ProviderError(f"no container image configured for runtime '{runtime_id}'", remediation="add it to the provider's 'images' map")
        return str(img)

    @property
    def kube(self) -> KubeClient:
        if self._kube is None:
            secret = self.env.secret(self.config.get("secret_id")) if self.config.get("secret_id") else None
            kubeconfig = secret.get("kubeconfig") if isinstance(secret, dict) else (secret if isinstance(secret, str) else None)
            backend = self.config.get("kube_backend", "auto")
            if backend == "python" or (backend == "auto" and _has_module("kubernetes")):
                self._kube = PythonKubeClient(kubeconfig, self.config.get("context"))
            else:
                if not shutil.which(self.config.get("kubectl_path") or "kubectl"):
                    raise ProviderError("neither the 'kubernetes' package nor 'kubectl' is available on the control plane host")
                self._kube = KubectlClient(self.config.get("kubectl_path") or "kubectl", kubeconfig, self.config.get("context"))
        return self._kube

    # ---- spark section -------------------------------------------------------------------------------
    def prepare(self, req: PrepareRequest) -> dict[str, Any]:
        g, job = req.placement, req.job
        name = k8s_name(job["id"], int(job["attempt"]))
        api = self.config.get("api_server") or "https://kubernetes.default.svc"
        svc_host = f"{name}.{self.namespace}.svc"
        driver_port, bm_port = int(self.config.get("driver_port", 7078)), int(self.config.get("block_manager_port", 7079))
        conf = base_spark_conf(req)
        executors = max(1, int(g.get("executors") or 1))
        conf.update({
            "spark.kubernetes.namespace": self.namespace, "spark.kubernetes.container.image": self.image_for(job.get("runtime_id")),
            "spark.kubernetes.driver.pod.name": name, "spark.executor.instances": str(executors),
            "spark.executor.cores": str(g.get("executor_cores") or 1), "spark.executor.memory": spark_memory(g.get("executor_memory_mb") or 1024),
            "spark.kubernetes.executor.deleteOnTermination": "true", "spark.driver.host": svc_host, "spark.driver.bindAddress": "0.0.0.0",  # noqa: S104 - the driver must accept executor connections
            "spark.driver.port": str(driver_port), "spark.blockManager.port": str(bm_port),
            "spark.kubernetes.executor.label.olcf.io/job-id": job["id"], "spark.kubernetes.driver.label.olcf.io/job-id": job["id"],
        })
        if self.config.get("service_account"):
            conf["spark.kubernetes.authenticate.driver.serviceAccountName"] = str(self.config["service_account"])
        if self.config.get("image_pull_secret"):
            conf["spark.kubernetes.container.image.pullSecrets"] = str(self.config["image_pull_secret"])
        for k, v in (self.config.get("executor_node_selector") or {}).items():
            conf[f"spark.kubernetes.executor.node.selector.{k}"] = str(v)
        conf.update(dynamic_allocation_conf(job.get("resources_request") or {}, g))
        return {"master": f"k8s://{api}", "app_name": req.app_name, "driver_host": svc_host, "driver_port": driver_port, "block_manager_port": bm_port,
                "conf": conf, "jars": [], "packages": [], "kubernetes": {"namespace": self.namespace, "pod": name}}

    # ---- manifests ------------------------------------------------------------------------------------
    def _labels(self, job: dict[str, Any]) -> dict[str, str]:
        return {"app": LABEL_APP, "olcf.io/job-id": job["id"], "olcf.io/attempt": str(job["attempt"]), "olcf.io/workspace": str(job["workspace_id"])[:63]}

    def build_secret(self, job: dict[str, Any], spec: dict[str, Any], source: bytes | None, name: str) -> tuple[dict[str, Any], str | None]:
        """The Secret manifest and the key under which the source file is stored (``None`` without a source)."""
        data = {"job.json": json.dumps(spec)}
        source_key: str | None = None
        if source is not None:
            path = (spec.get("source") or {}).get("path", "source/source.txt")
            source_key = "source__" + path.replace("/", "__")
            data[source_key] = source.decode("utf-8", "replace")
        total = sum(len(v) for v in data.values())
        if total > SECRET_LIMIT_BYTES:
            raise Unsupported(f"the job payload ({total // 1024} KiB) exceeds the Kubernetes Secret size limit",
                              remediation="stage large sources in storage and submit them with source.artifact_uri")
        manifest = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": self.namespace, "labels": self._labels(job)},
                    "type": "Opaque", "stringData": data}
        return manifest, source_key

    def build_pod(self, job: dict[str, Any], spec: dict[str, Any], name: str, secret_name: str, source_key: str | None = None) -> dict[str, Any]:
        res = job.get("resources_granted") or {}
        cores = max(1, int(res.get("driver_cores") or 1))
        mem = int(res.get("driver_memory_mb") or 1024)
        overhead = max(384, int(mem * 0.1))
        sp = spec.get("spark") or {}
        items = [{"key": "job.json", "path": "job.json"}]
        if source_key:
            items.append({"key": source_key, "path": source_key.removeprefix("source__").replace("__", "/")})
        command = [str(c).replace("{input_dir}", INPUT_DIR).replace("{work_dir}", WORK_DIR) for c in (self.config.get("runner_command") or DEFAULT_RUNNER_COMMAND)]
        uid = int(self.config.get("run_as_user") or DEFAULT_UID)
        gid = int(self.config.get("run_as_group") or uid)
        work_vol: dict[str, Any] = {"name": "work", "emptyDir": {}}
        if self.config.get("work_dir_size_limit"):
            work_vol["emptyDir"]["sizeLimit"] = str(self.config["work_dir_size_limit"])
        pod: dict[str, Any] = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": self.namespace, "labels": self._labels(job), "annotations": {"olcf.io/runtime": str(job.get("runtime_id"))}},
            "spec": {
                "restartPolicy": "Never", "terminationGracePeriodSeconds": 30,
                # non-root, and the Secret files (mode 0440, group = fsGroup) stay readable to exactly that user; the work volume is group-writable through fsGroup
                "securityContext": {"runAsNonRoot": True, "runAsUser": uid, "runAsGroup": gid, "fsGroup": gid, "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{
                    "name": "driver", "image": self.image_for(job.get("runtime_id")), "imagePullPolicy": self.config.get("image_pull_policy", "IfNotPresent"), "command": list(command),
                    "env": [{"name": "OLCF_JOB_ID", "value": job["id"]}, {"name": "OLCF_ATTEMPT", "value": str(job["attempt"])}, {"name": "HOME", "value": "/tmp"}],  # noqa: S108 - an emptyDir
                    "ports": [{"name": "driver", "containerPort": int(sp.get("driver_port") or 7078)}, {"name": "blockmgr", "containerPort": int(sp.get("block_manager_port") or 7079)},
                              {"name": "ui", "containerPort": 4040}],
                    "resources": {"requests": {"cpu": str(cores), "memory": f"{mem + overhead}Mi"}, "limits": {"memory": f"{mem + overhead}Mi"}},
                    "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}, "readOnlyRootFilesystem": bool(self.config.get("read_only_root_filesystem", False))},
                    "terminationMessagePath": "/dev/termination-log", "terminationMessagePolicy": "File",
                    "volumeMounts": [{"name": "input", "mountPath": INPUT_DIR, "readOnly": True}, {"name": "work", "mountPath": WORK_DIR}, {"name": "tmp", "mountPath": "/tmp"}],  # noqa: S108
                }],
                "volumes": [{"name": "input", "secret": {"secretName": secret_name, "defaultMode": 0o440, "items": items}}, work_vol, {"name": "tmp", "emptyDir": {}}],
            },
        }
        if self.config.get("service_account"):
            pod["spec"]["serviceAccountName"] = self.config["service_account"]
        if self.config.get("image_pull_secret"):
            pod["spec"]["imagePullSecrets"] = [{"name": self.config["image_pull_secret"]}]
        if self.config.get("driver_node_selector"):
            pod["spec"]["nodeSelector"] = dict(self.config["driver_node_selector"])
        return pod

    def build_service(self, job: dict[str, Any], spec: dict[str, Any], name: str) -> dict[str, Any]:
        sp = spec.get("spark") or {}
        return {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": self.namespace, "labels": self._labels(job)},
                "spec": {"clusterIP": "None", "selector": {"olcf.io/job-id": job["id"], "olcf.io/attempt": str(job["attempt"]), "app": LABEL_APP},
                         "ports": [{"name": "driver", "port": int(sp.get("driver_port") or 7078)}, {"name": "blockmgr", "port": int(sp.get("block_manager_port") or 7079)},
                                   {"name": "ui", "port": 4040}]}}

    # ---- interface ------------------------------------------------------------------------------------
    def _create(self, manifest: dict[str, Any], job: dict[str, Any], created: list[tuple[str, str]]) -> None:
        """Create one object. Creation is idempotent per attempt: an object of that name that carries this job's labels was made by an earlier try of the same
        attempt (a lost response, a restart of the control plane) and is adopted; anything else with that name is a conflict."""
        kind, name = manifest["kind"].lower(), manifest["metadata"]["name"]
        try:
            self.kube.create(manifest)
        except AlreadyExists:
            existing = self.kube.get(kind, self.namespace, name)
            labels = ((existing or {}).get("metadata") or {}).get("labels") or {}
            if existing is None or labels.get("olcf.io/job-id") != job["id"] or labels.get("olcf.io/attempt") != str(job["attempt"]):
                raise ProviderError(f"{kind} '{name}' already exists in namespace '{self.namespace}' and does not belong to this job attempt",
                                    remediation="remove the stale object or choose another namespace") from None
            log.info("adopting existing %s/%s of the same attempt", kind, name)
            return
        created.append((kind, name))

    def submit(self, req: SubmitRequest) -> AppHandle:
        job, spec = req.job, req.spec
        name = k8s_name(job["id"], int(job["attempt"]))
        mode = self.config.get("submit_mode", "driver_pod")
        secret, source_key = self.build_secret(job, spec, req.source_bytes, name)
        kube, ns = self.kube, self.namespace
        created: list[tuple[str, str]] = []
        try:
            self._create(secret, job, created)
            if mode == "spark_submit":
                conf = dict((spec.get("spark") or {}).get("conf") or {})
                args = build_spark_submit_args(self.config, pod_name=name, secret_name=name, conf=conf,
                                               entrypoint=self.config.get("entrypoint") or "local:///opt/olcf/entrypoint.py")
                try:
                    subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)  # noqa: S603
                except OSError as exc:
                    raise ProviderError(f"could not run spark-submit: {exc}") from exc
            else:
                self._create(self.build_service(job, spec, name), job, created)
                self._create(self.build_pod(job, spec, name, name, source_key), job, created)
        except Exception:
            for kind, n in reversed(created):  # do not leak half-created objects (adopted ones belong to an earlier try and stay)
                try:
                    kube.delete(kind, ns, n)
                except Exception:  # noqa: BLE001
                    log.warning("cleanup of %s/%s failed", kind, n)
            raise
        return AppHandle(self.name, job["id"], int(job["attempt"]), native={"namespace": ns, "pod": name, "service": name if mode != "spark_submit" else None, "secret": name,
                                                                          "submit_mode": mode, "submitted_at": time.time()})

    def _pod(self, handle: AppHandle) -> dict[str, Any] | None:
        n = handle.native
        return self.kube.get("pod", n.get("namespace", self.namespace), n["pod"])

    def status(self, handle: AppHandle) -> AppStatus:
        pod = self._pod(handle)
        if pod is None:
            n = handle.native
            # spark-submit creates the driver pod asynchronously: give it a grace period before declaring it lost
            waiting = n.get("submit_mode") == "spark_submit" and not n.get("seen") and time.time() - float(n.get("submitted_at") or 0) < float(self.config.get("submit_grace_seconds", 180))
            if waiting:
                return AppStatus("pending", None, "waiting for spark-submit to create the driver pod", native=n)
            return AppStatus("failed", None, "driver pod does not exist (deleted or never created)", native=n, code="pod_missing", confirmed_gone=True)
        st = pod.get("status") or {}
        phase = st.get("phase", "Unknown")
        exit_code, reason, message, started = None, st.get("reason") or "", "", False
        for cs in st.get("containerStatuses") or []:
            term = (cs.get("state") or {}).get("terminated")
            if (cs.get("state") or {}).get("running") or term:
                started = True
            if term:
                exit_code = term.get("exitCode")
                reason = reason or term.get("reason", "")
                message = term.get("message") or message
        state = {"Pending": "pending", "Running": "running", "Succeeded": "succeeded", "Failed": "failed"}.get(phase, "unknown")
        native = {**handle.native, "seen": True, "pod_ip": st.get("podIP"), "node": (pod.get("spec") or {}).get("nodeName")}
        termination = self._termination(handle, reason, exit_code, message) if state in ("succeeded", "failed") else None
        return AppStatus(state, handle.application_id, f"pod {phase}{': ' + reason if reason else ''}", native=native, exit_code=exit_code, termination=termination,
                         confirmed_gone=True if termination is not None else None, reached_running=(started or None) if termination is not None else None)

    @staticmethod
    def _termination(handle: AppHandle, reason: str, exit_code: int | None, message: str) -> dict[str, Any]:
        """What the kubelet recorded about the end of the driver container: reason, exit code, and the runner's compact record (``result``) when it left one."""
        t: dict[str, Any] = {"reason": reason, "exit_code": exit_code}
        if message:
            try:
                rec = json.loads(message)
            except ValueError:
                rec = None
            if isinstance(rec, dict) and rec.get("v") == 1 and isinstance(rec.get("result"), dict) and rec.get("job_id") in (None, handle.job_id):
                t["result"] = rec["result"]
                if isinstance(rec["result"].get("failure"), dict):
                    t["failure"] = rec["result"]["failure"]
            else:
                t["message"] = message[:500]
        if "failure" not in t:
            if reason == "OOMKilled":
                t["failure"] = {"category": "resource", "message": "the driver container was killed for exceeding its memory limit (OOMKilled)",
                                "remediation": "raise the job's driver memory (resources.driver_memory_mb) or reduce what the driver collects"}
            elif reason == "Evicted":
                t["failure"] = {"category": "provider", "message": "the driver pod was evicted by the node", "remediation": "run the job again; check node pressure if it repeats"}
            elif exit_code == 137:
                t["failure"] = {"category": "provider", "message": "the driver container was killed (exit 137, SIGKILL)"}
        return t

    def logs(self, handle: AppHandle, offset: int = 0, limit: int = 65536, *, cursor: Any = None) -> LogsPage:
        """The driver log, paged from a **timestamp cursor** (``LogsPage.cursor``; see :func:`advance_log_cursor`), so a changing, truncated or rotated log cannot make
        the reader skip or repeat lines: each page is read with ``sinceTime`` from the second of the last consumed line, the lines up to that line are dropped by
        timestamp + count + hash, and a missing anchor is reported in ``LogsPage.gap`` instead of silently resuming somewhere else. Without a cursor the whole retained
        log is read from the start (``offset`` is then a plain line count, used only to continue a job that was started before cursors existed). Stateless, so any
        replica can poll."""
        n = handle.native
        ns, pod = n.get("namespace", self.namespace), n["pod"]
        cur = cursor if isinstance(cursor, dict) and cursor.get("v") == 1 else None
        window = max(int(limit), 4096) + LOG_SLACK_BYTES
        since = _iso_seconds(int(cur["s"])) if cur else None
        gap: str | None = None
        for step in range(LOG_GROW_STEPS + 1):
            raw = self.kube.pod_log(ns, pod, limit_bytes=window, since_time=since, timestamps=True)
            entries = parse_timestamped_log(raw)
            cut = len(raw.encode("utf-8")) >= window
            if cut and raw and not raw.endswith("\n") and len(entries) > 1:
                entries.pop()  # the byte limit fell inside this line: read it whole next time
            new, found, note = advance_log_cursor(entries, cur)
            if not cut or (found and new):
                gap = note if not found else None
                break
            if window >= LOG_MAX_WINDOW_BYTES or step == LOG_GROW_STEPS:
                if not found:
                    # more than the largest window was logged within the cursor's second: step over that second, loudly
                    skip = int(cur["s"]) + 1 if cur else 0
                    gap = f"more than {window // (1024 * 1024)} MiB of output was written within one second ({_iso_seconds(skip - 1)}); lines after the last one read could not be retrieved"
                    return LogsPage("", max(0, offset), False, cursor={"v": 1, "s": skip, "ns": 0, "n": 0, "h": ""}, gap=gap)
                break
            window = min(window * 4, LOG_MAX_WINDOW_BYTES)
        if cur is None:
            new = new[max(0, offset):]
        out, size = [], 0
        for e in new:
            if out and size + len(e.text) + 1 > limit:
                break
            out.append(e)
            size += len(e.text) + 1
        more = cut or len(out) < len(new)
        nxt = max(0, offset) + len(out)
        if not out:
            return LogsPage("", nxt, not more, cursor=cursor if cur else None, gap=gap)
        last = out[-1]
        pos = len(entries) - len(new) + len(out) - 1  # ``new`` is a suffix of ``entries``
        consumed = sum(1 for e in entries[: pos + 1] if e.key == last.key)  # lines of the retained log up to and including ``last`` that share its timestamp
        return LogsPage("".join(e.text + "\n" for e in out), nxt, not more, cursor={"v": 1, "s": last.sec, "ns": last.nsec, "n": consumed, "h": _anchor(last.text)}, gap=gap)

    def find_result(self, handle: AppHandle) -> dict[str, Any] | None:
        """The runner's ``result`` event in the *tail* of the retained log, independent of any cursor (a rotated or truncated middle of the log cannot hide the
        last lines). ``None`` when the log has none for this job and attempt."""
        n = handle.native
        raw = self.kube.pod_log(n.get("namespace", self.namespace), n["pod"], limit_bytes=8 * 1024 * 1024, tail_lines=400)
        found: dict[str, Any] | None = None
        for line in raw.splitlines():
            if not line.startswith("@@OLCF@@"):
                continue
            try:
                ev = json.loads(line[8:].strip())
            except ValueError:
                continue
            if isinstance(ev, dict) and ev.get("event") == "result" and isinstance(ev.get("result"), dict) and ev.get("job_id", handle.job_id) == handle.job_id \
                    and ev.get("attempt", handle.attempt) == handle.attempt:
                found = ev["result"]
        return found

    def cancel(self, handle: AppHandle, *, force: bool = False, grace_seconds: int = 15) -> None:
        n = handle.native
        ns = n.get("namespace", self.namespace)
        self.kube.delete("pod", ns, n["pod"], grace_seconds=0 if force else grace_seconds)
        for kind, key in (("service", "service"), ("secret", "secret")):
            if n.get(key):
                self.kube.delete(kind, ns, n[key])

    def cleanup(self, handle: AppHandle) -> None:
        """Remove the Secret/Service of a finished attempt (the pod is kept for log retrieval until retention)."""
        n = handle.native
        ns = n.get("namespace", self.namespace)
        for kind, key in (("service", "service"), ("secret", "secret")):
            if n.get(key):
                self.kube.delete(kind, ns, n[key])

    def scale(self, handle: AppHandle, policy: dict[str, Any]) -> dict[str, Any]:
        raise Unsupported("Spark on Kubernetes scales through dynamic allocation; request it in the job's ResourceRequest",
                          remediation="resubmit with resources.dynamic_allocation=true and min/max executors")

    def ui(self, handle: AppHandle) -> UiInfo:
        n = handle.native
        tmpl = self.config.get("ui_url_template") or "http://{service}.{namespace}.svc:4040"
        try:
            url = tmpl.format(pod=n.get("pod", ""), service=n.get("service", ""), namespace=n.get("namespace", self.namespace), pod_ip=n.get("pod_ip") or "")
        except (KeyError, IndexError):
            return UiInfo(None, False, "invalid ui_url_template")
        return UiInfo(url, True, "reachable only when the control plane can resolve cluster DNS (run it in-cluster or set ui_url_template)")

    def reconcile(self) -> list[AppStatus]:
        out = []
        for pod in self.kube.list("pod", self.namespace, f"app={LABEL_APP}"):
            labels = (pod.get("metadata") or {}).get("labels") or {}
            phase = (pod.get("status") or {}).get("phase", "Unknown")
            out.append(AppStatus({"Pending": "pending", "Running": "running", "Succeeded": "succeeded", "Failed": "failed"}.get(phase, "unknown"), None, f"pod {(pod.get('metadata') or {}).get('name')}",
                                 job_id=labels.get("olcf.io/job-id"), native={"namespace": self.namespace, "pod": (pod.get("metadata") or {}).get("name"),
                                                                              "attempt": labels.get("olcf.io/attempt")}))
        return out

    def discover(self) -> Discovery:
        try:
            ver = self.kube.version()
            pods = self.kube.list("pod", self.namespace, f"app={LABEL_APP}")
            return Discovery(self.name, self.type, True, f"Kubernetes {ver}; {len(pods)} driver pods in '{self.namespace}'", list(self.capabilities), "remote", live_verified=False,
                             extra={"namespace": self.namespace, "submit_mode": self.config.get("submit_mode", "driver_pod")})
        except Exception as exc:  # noqa: BLE001
            return Discovery(self.name, self.type, False, str(exc), list(self.capabilities), "remote", live_verified=False)


def _has_module(name: str) -> bool:
    import importlib.util

    return importlib.util.find_spec(name) is not None
