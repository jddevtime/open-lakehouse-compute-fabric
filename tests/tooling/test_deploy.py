# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: S101, S603, S607, S104, S105, S106, S314
"""deploy/: Helm chart (via the bundled renderer, plus kubeconform when present), compose, entrypoints, service files and the offline-bundle script.

Neither Docker nor Helm is required: tests that need them skip, and say so, rather than pass vacuously.
"""

from __future__ import annotations

import importlib.util
import os
import plistlib
import re
import shutil
import socket
import subprocess
import sys
import textwrap
import threading
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
CHART = DEPLOY / "helm" / "olcf"

_spec = importlib.util.spec_from_file_location("render_chart", DEPLOY / "scripts" / "render_chart.py")
rc = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
sys.modules["render_chart"] = rc
_spec.loader.exec_module(rc)  # type: ignore[union-attr]


# ------------------------------------------------------------------------------------------------------------------ the template interpreter itself
@pytest.mark.parametrize(
    ("template", "values", "expected"),
    [
        ("a{{ .Values.x }}b", {"x": 1}, "a1b"),
        ("a  {{- .Values.x -}}  b", {"x": "X"}, "aXb"),
        ("{{ if .Values.on }}yes{{ else }}no{{ end }}", {"on": False}, "no"),
        ("{{ if eq .Values.k \"a\" }}A{{ else if eq .Values.k \"b\" }}B{{ else }}C{{ end }}", {"k": "b"}, "B"),
        ("{{ if .Values.o }}x{{ if .Values.i }}1{{ else }}2{{ end }}{{ else }}3{{ end }}", {"o": 1, "i": 0}, "x2"),
        ("{{ range .Values.l }}[{{ . }}]{{ end }}", {"l": [1, 2, 3]}, "[1][2][3]"),
        ("{{ range $i, $e := .Values.l }}{{ $i }}={{ $e }};{{ end }}", {"l": ["a", "b"]}, "0=a;1=b;"),
        ("{{ range $k, $v := .Values.m }}{{ $k }}:{{ $v }},{{ end }}", {"m": {"b": 2, "a": 1}}, "a:1,b:2,"),
        ("{{ range .Values.empty }}x{{ else }}none{{ end }}", {"empty": []}, "none"),
        ("{{ with .Values.w }}{{ .name }}{{ end }}", {"w": {"name": "n"}}, "n"),
        ("{{ with .Values.w }}x{{ else }}y{{ end }}", {"w": {}}, "y"),
        ("{{ $x := 1 }}{{ if true }}{{ $x = 2 }}{{ end }}{{ $x }}", {}, "2"),
        ("{{ $.Values.a }}{{ range .Values.l }}{{ $.Values.a }}{{ end }}", {"a": "z", "l": [1]}, "zz"),
        ("{{ .Values.v | default \"d\" | quote }}", {}, '"d"'),
        ("{{ default \"d\" .Values.v }}", {"v": "set"}, "set"),
        ("{{ printf \"%s-%s\" .Values.a .Values.b | trunc 5 }}", {"a": "abc", "b": "defg"}, "abc-d"),
        ("{{ .Values.s | trimSuffix \"-\" }}", {"s": "name-"}, "name"),
        ("{{ .Values.m | toYaml | nindent 2 }}", {"m": {"b": 1, "a": [1, 2]}}, "\n  a:\n  - 1\n  - 2\n  b: 1"),
        ("{{ include \"h\" (dict \"n\" .Values.n) }}", {"n": "N"}, "<N>"),
        ("{{ include \"h\" (dict \"n\" \"M\") }}", {}, "<M>"),
        ("{{ and .Values.a .Values.b }}|{{ or .Values.a .Values.b }}|{{ not .Values.a }}", {"a": 0, "b": "x"}, "0|x|true"),
        ("{{ if and .Values.a (gt (int .Values.n) 1) }}big{{ end }}", {"a": True, "n": "3"}, "big"),
        ("{{ range $i := until 3 }}{{ if $i }},{{ end }}{{ $i }}{{ end }}", {}, "0,1,2"),
        ("{{/* a comment */}}text{{- /* trimmed */ -}} more", {}, "textmore"),
        ("{{ .Values.s | b64enc | b64dec }}", {"s": "round trip"}, "round trip"),
        ("{{ hasKey .Values.m \"a\" }}{{ hasKey .Values.m \"z\" }}{{ index .Values.m \"a\" }}", {"m": {"a": 7}}, "truefalse7"),
        ("{{ if lookup \"v1\" \"Secret\" \"ns\" \"n\" }}found{{ else }}absent{{ end }}", {}, "absent"),
    ],
)
def test_template_subset(template, values, expected):
    assert rc.render_string(template, values, helpers='{{ define "h" }}<{{ .n }}>{{ end }}') == expected


@pytest.mark.parametrize(
    ("template", "message"),
    [
        ("{{ fail \"boom\" }}", "boom"),
        ("{{ required \"need v\" .Values.v }}", "need v"),
        ("{{ if .Values.a }}unterminated", "missing {{ end }}"),
        ("{{ end }}", "unbalanced"),
        ("{{ .Values.nope.deeper }}", "nil pointer"),
        ("{{ template \"x\" . }}", "unsupported"),
        ("{{ include \"missing\" . }}", "not defined"),
        ("{{ nosuchfunc 1 }}", "not a function"),
    ],
)
def test_template_errors_are_loud(template, message):
    with pytest.raises(rc.TemplateError, match=message.replace("{", r"\{").replace("}", r"\}")):
        rc.render_string(template, {})


# ------------------------------------------------------------------------------------------------------------------ the chart
def _render(*files: str, sets: list[str] | None = None, release: str = "olcf") -> dict[str, str]:
    return rc.render_chart(CHART, [CHART / "ci" / f for f in files], sets or [], release=release)


def _objects(rendered: dict[str, str]) -> list[dict]:
    return [d for _f, d in rc.manifests(rendered)]


def _kinds(objs: list[dict]) -> list[str]:
    return sorted(o["kind"] for o in objs)


@pytest.fixture(scope="module")
def lab() -> list[dict]:
    return _objects(_render("lab-values.yaml"))


@pytest.fixture(scope="module")
def prod() -> list[dict]:
    return _objects(_render("production-values.yaml"))


def _by(objs: list[dict], kind: str, name: str | None = None) -> list[dict]:
    return [o for o in objs if o["kind"] == kind and (name is None or o["metadata"]["name"] == name)]


def test_chart_files_have_license_headers_and_metadata():
    meta = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert meta["apiVersion"] == "v2" and meta["name"] == "olcf" and meta["type"] == "application"
    for path in [CHART / "Chart.yaml", CHART / "values.yaml", *(CHART / "templates").iterdir()]:
        head = path.read_text()[:400]
        assert "SPDX-License-Identifier: Apache-2.0" in head and "Justin Dodrill" in head, path.name


def test_chart_version_matches_package_version():
    meta = yaml.safe_load((CHART / "Chart.yaml").read_text())
    version = (ROOT / "src" / "olcf" / "version.py").read_text()
    assert f'"{meta["appVersion"]}"' in version, "Chart appVersion must equal olcf.version.__version__"


def test_lab_install_objects(lab):
    kinds = set(_kinds(lab))
    assert {"Deployment", "StatefulSet", "Service", "Secret", "ConfigMap", "PersistentVolumeClaim", "NetworkPolicy", "Job", "Pod", "ServiceAccount"} <= kinds
    assert "Ingress" not in kinds and "PodDisruptionBudget" not in kinds
    # the lab bundles PostgreSQL: a StatefulSet for it and one for the workers
    assert {o["metadata"]["name"] for o in _by(lab, "StatefulSet")} == {"olcf-postgres", "olcf-worker"}


def test_production_objects(prod):
    kinds = set(_kinds(prod))
    assert {"Ingress", "PodDisruptionBudget"} <= kinds
    assert _by(prod, "StatefulSet", "olcf-postgres") == []
    dep = _by(prod, "Deployment")[0]
    assert dep["spec"]["replicas"] == 3 and dep["spec"]["strategy"]["type"] == "RollingUpdate"
    ing = _by(prod, "Ingress")[0]
    assert ing["spec"]["tls"][0]["secretName"] == "olcf-tls"
    assert ing["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"] == {"name": "olcf", "port": {"number": 8470}}
    assert "olcf-compute" == _by(prod, "Role", "olcf-compute")[0]["metadata"]["namespace"]


def test_default_values_refuse_to_guess_how_workers_reach_the_controller():
    with pytest.raises(rc.TemplateError, match="workers need a controller URL"):
        rc.render_chart(CHART, [], [])


def test_multiple_replicas_without_postgres_is_refused():
    with pytest.raises(rc.TemplateError, match="needs PostgreSQL"):
        _render("lab-values.yaml", sets=["database.bundled.enabled=false", "controlPlane.replicaCount=2"])


def test_unknown_and_invalid_values_are_rejected_by_the_schema():
    for bad in (["bogus=1"], ["controlPlane.replicaCount=0"], ["persistence.size=lots"], ["image.pullPolicy=Sometimes"], ["podSecurityContext.runAsNonRoot=false"],
                ["containerSecurityContext.allowPrivilegeEscalation=true"], ["ingress.hosts=[]"], ["controlPlane.publicUrl=ftp://x"]):
        with pytest.raises(rc.TemplateError, match="values.schema.json"):
            _render("lab-values.yaml", sets=bad)


def test_control_plane_is_hardened_and_wired(lab):
    dep = _by(lab, "Deployment")[0]
    pod = dep["spec"]["template"]["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True and pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    ctr = pod["containers"][0]
    assert ctr["securityContext"]["readOnlyRootFilesystem"] is True and ctr["securityContext"]["allowPrivilegeEscalation"] is False
    assert ctr["securityContext"]["capabilities"]["drop"] == ["ALL"]
    assert "limits" in ctr["resources"] and "requests" in ctr["resources"]
    env = {e["name"]: e for e in ctr["env"]}
    for key in ("OLCF_MASTER_KEY", "OLCF_JWT_SIGNING_KEY", "OLCF_ADMIN_PASSWORD", "OLCF_DB__URL"):
        assert "secretKeyRef" in env[key]["valueFrom"], f"{key} must come from a Secret, never a literal"
    assert not any("value" in e and e["name"] in ("OLCF_MASTER_KEY", "OLCF_JWT_SIGNING_KEY", "OLCF_ADMIN_PASSWORD") for e in ctr["env"])
    # a ReadWriteOnce volume cannot be shared by an old and a new pod during a rolling update
    assert dep["spec"]["strategy"]["type"] == "Recreate"
    mounts = {m["mountPath"] for m in ctr["volumeMounts"]}
    assert {"/data", "/tmp"} <= mounts  # noqa: S108 - container path, not a host temp file


def test_every_workload_is_restricted_pss_compliant(lab, prod):
    for objs in (lab, prod):
        for o in objs:
            if o["kind"] not in ("Deployment", "StatefulSet", "Job", "Pod"):
                continue
            spec = o["spec"] if o["kind"] == "Pod" else o["spec"]["template"]["spec"]
            name = o["metadata"]["name"]
            if name.endswith("-postgres"):
                assert spec["securityContext"]["runAsNonRoot"] is True
            else:
                assert spec["securityContext"]["runAsNonRoot"] is True, name
            assert not spec.get("hostNetwork") and not spec.get("hostPID") and not spec.get("hostIPC"), name
            for c in spec["containers"] + spec.get("initContainers", []):
                sc = c["securityContext"]
                assert sc["allowPrivilegeEscalation"] is False and sc["readOnlyRootFilesystem"] is True, f"{name}/{c['name']}"
                assert sc["capabilities"]["drop"] == ["ALL"] and not sc.get("privileged"), f"{name}/{c['name']}"
                assert ":" in c["image"].strip('"') and not c["image"].endswith(":latest"), f"{name}/{c['name']}: pin the image tag"


def test_selectors_and_service_targets_line_up(lab):
    pods = {}
    for o in lab:
        if o["kind"] in ("Deployment", "StatefulSet"):
            pods[o["metadata"]["name"]] = o["spec"]["template"]["metadata"]["labels"]
            match = o["spec"]["selector"]["matchLabels"]
            assert match.items() <= pods[o["metadata"]["name"]].items(), o["metadata"]["name"]
    for svc in _by(lab, "Service"):
        sel = svc["spec"]["selector"]
        assert any(sel.items() <= labels.items() for labels in pods.values()), f"service {svc['metadata']['name']} selects nothing"
    port_names = {p["name"] for o in lab if o["kind"] == "Deployment" for c in o["spec"]["template"]["spec"]["containers"] for p in c["ports"]}
    for svc in _by(lab, "Service", "olcf"):
        assert svc["spec"]["ports"][0]["targetPort"] in port_names


def test_worker_enrolment_wiring(lab):
    sts = _by(lab, "StatefulSet", "olcf-worker")[0]
    ctr = sts["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e for e in ctr["env"]}
    assert env["OLCF_WORKER_NAME"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.name"
    assert env["OLCF_WORKER_INSECURE_WS"]["value"] == "true" and env["OLCF_WORKER_URL"]["value"] == "http://olcf:8470"
    vol = {v["name"]: v for v in sts["spec"]["template"]["spec"]["volumes"]}
    assert vol["enroll"]["secret"]["secretName"] == "olcf-enroll" and vol["enroll"]["secret"]["optional"] is True
    job = _by(lab, "Job", "olcf-enroll")[0]
    jenv = {e["name"]: e.get("value") for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert jenv["WORKER_PODS"] == "olcf-worker-0,olcf-worker-1" and jenv["ENROLL_OBJECT_NAME"] == "olcf-enroll"
    assert "post-install" in job["metadata"]["annotations"]["helm.sh/hook"]
    role = [r for r in _by(lab, "Role", "olcf-enroll")][0]
    create = [r for r in role["rules"] if "create" in r["verbs"]][0]
    assert "resourceNames" not in create  # Kubernetes cannot scope "create" by name
    scoped = [r for r in role["rules"] if "patch" in r["verbs"]][0]
    assert scoped["resourceNames"] == ["olcf-enroll"]


def test_hook_script_is_valid_python_without_secrets_in_output(lab):
    cm = _by(lab, "ConfigMap", "olcf-enroll-script")[0]
    src = cm["data"]["enroll.py"]
    compile(src, "enroll.py", "exec")
    assert "print(token" not in src and "print(doc" not in src


def test_kubernetes_provider_rbac(prod, lab):
    role = _by(prod, "Role", "olcf-compute")[0]
    resources = {(r, v) for rule in role["rules"] for r in rule["resources"] for v in rule["verbs"]}
    for need in (("secrets", "create"), ("services", "create"), ("pods", "create"), ("pods", "delete"), ("pods/log", "get")):
        assert need in resources, need
    assert not any("*" in rule["verbs"] or "*" in rule["resources"] for rule in role["rules"])
    assert not _by(lab, "Role", "olcf-compute"), "no compute RBAC unless the provider is enabled"
    binding = _by(prod, "RoleBinding", "olcf-compute")[0]
    assert binding["subjects"][0]["namespace"] == "olcf" and binding["metadata"]["namespace"] == "olcf-compute"
    # nothing in the chart is cluster-scoped
    assert not any(o["kind"] in ("ClusterRole", "ClusterRoleBinding") for o in prod)


def test_workers_can_be_disabled():
    objs = _objects(_render("lab-values.yaml", sets=["workers.enabled=false"]))
    assert not _by(objs, "StatefulSet", "olcf-worker") and not _by(objs, "Job")


def test_existing_secrets_are_honoured():
    objs = _objects(_render("production-values.yaml", sets=["secrets.existingSecret=mine", "database.external.url=", "database.external.existingSecret=pg"]))
    assert not _by(objs, "Secret")
    env = {e["name"]: e for e in _by(objs, "Deployment")[0]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["OLCF_MASTER_KEY"]["valueFrom"]["secretKeyRef"]["name"] == "mine"
    assert env["OLCF_DB__URL"]["valueFrom"]["secretKeyRef"]["name"] == "pg"


# ------------------------------------------------------------------------------------------------------------------ runtime trust directory, extra env / volumes
def _render_with(tmp_path: Path, values: dict, base: str = "lab-values.yaml") -> list[dict]:
    extra = tmp_path / "extra-values.yaml"
    extra.write_text(yaml.safe_dump(values))
    return _objects(rc.render_chart(CHART, [CHART / "ci" / base, extra], []))


def _pod(objs: list[dict], kind: str) -> dict:
    """The control-plane Deployment or the worker StatefulSet (the lab values also render a bundled PostgreSQL StatefulSet)."""
    return _by(objs, kind, "olcf" if kind == "Deployment" else "olcf-worker")[0]["spec"]["template"]["spec"]


def _trust_mounts(pod: dict) -> tuple[list[dict], list[dict], list[dict]]:
    c = pod["containers"][0]
    return ([e for e in c["env"] if e["name"] == "OLCF_RUNTIME_TRUST_DIR"], [m for m in c["volumeMounts"] if m["name"] == "runtime-trust"],
            [v for v in pod["volumes"] if v["name"] == "runtime-trust"])


@pytest.mark.parametrize("kind", ["Deployment", "StatefulSet"])
def test_runtime_trust_is_not_mounted_by_default(lab, kind):
    env, mounts, volumes = _trust_mounts(_pod(lab, kind))
    assert not env and not mounts and not volumes


@pytest.mark.parametrize(("source", "volume_key", "name_key"), [("existingSecret", "secret", "secretName"), ("existingConfigMap", "configMap", "name")])
def test_runtime_trust_dir_is_mounted_read_only_and_exported_to_control_plane_and_workers(tmp_path, source, volume_key, name_key):
    objs = _render_with(tmp_path, {"runtimeTrust": {source: "ops-trust", "mountPath": "/etc/ops/trust"}})
    for kind in ("Deployment", "StatefulSet"):
        env, mounts, volumes = _trust_mounts(_pod(objs, kind))
        assert env == [{"name": "OLCF_RUNTIME_TRUST_DIR", "value": "/etc/ops/trust"}], kind
        assert mounts == [{"name": "runtime-trust", "mountPath": "/etc/ops/trust", "readOnly": True}], kind
        assert volumes[0][volume_key][name_key] == "ops-trust", kind
        assert len(volumes) == 1


def test_runtime_trust_default_mount_path_and_exclusive_sources(tmp_path):
    objs = _render_with(tmp_path, {"runtimeTrust": {"existingConfigMap": "k"}})
    assert _trust_mounts(_pod(objs, "Deployment"))[0][0]["value"] == "/etc/olcf/runtime-trust"
    with pytest.raises(rc.TemplateError, match="mutually exclusive"):
        _render_with(tmp_path, {"runtimeTrust": {"existingConfigMap": "k", "existingSecret": "s"}})


def test_runtime_trust_values_are_validated_by_the_schema(tmp_path):
    for bad in ({"existingSecret": 5}, {"mountPath": "relative/dir"}, {"unknown": "x"}):
        with pytest.raises(Exception, match="runtimeTrust|mountPath|unknown|existingSecret|Additional|pattern|type"):
            _render_with(tmp_path, {"runtimeTrust": bad})


def test_extra_env_volumes_and_mounts_reach_control_plane_and_workers(tmp_path):
    vol = {"name": "ca", "configMap": {"name": "corp-ca"}}
    mount = {"name": "ca", "mountPath": "/etc/ca", "readOnly": True}
    env = [{"name": "HTTPS_PROXY", "value": "http://proxy:3128"}]
    objs = _render_with(tmp_path, {"controlPlane": {"extraEnv": env, "extraVolumes": [vol], "extraVolumeMounts": [mount]},
                                   "workers": {"extraEnv": env, "extraVolumes": [vol], "extraVolumeMounts": [mount]}})
    for kind in ("Deployment", "StatefulSet"):
        pod = _pod(objs, kind)
        c = pod["containers"][0]
        assert env[0] in c["env"] and mount in c["volumeMounts"] and vol in pod["volumes"], kind
    # and they coexist with the trust directory
    both = _render_with(tmp_path, {"runtimeTrust": {"existingSecret": "t"}, "workers": {"extraVolumes": [vol], "extraVolumeMounts": [mount]}})
    pod = _pod(both, "StatefulSet")
    assert {v["name"] for v in pod["volumes"]} >= {"runtime-trust", "ca", "tmp", "enroll"} and {m["name"] for m in pod["containers"][0]["volumeMounts"]} >= {"runtime-trust", "ca"}


def test_generated_secret_has_a_valid_master_key(lab):
    import base64

    sec = _by(lab, "Secret", "olcf-secrets")[0]
    key = base64.b64decode(base64.b64decode(sec["data"]["master-key"]))
    assert len(key) == 32
    assert len(base64.b64decode(sec["data"]["jwt-signing-key"])) >= 32
    assert sec["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"


def test_names_fit_kubernetes_limits():
    objs = _objects(_render("lab-values.yaml", release="a-rather-long-release-name-for-olcf-in-production"))
    for o in objs:
        assert len(o["metadata"]["name"]) <= 63, o["metadata"]["name"]


@pytest.mark.skipif(not rc.find_kubeconform(), reason="set KUBECONFORM or put kubeconform on PATH to validate the manifests against the Kubernetes schemas")
@pytest.mark.parametrize("values", ["lab-values.yaml", "production-values.yaml"])
def test_kubeconform(values):
    docs = rc.manifests(_render(values))
    code, text = rc.run_kubeconform(rc.find_kubeconform(), docs)  # type: ignore[arg-type]
    assert code == 0, text
    assert "Invalid: 0" in text and "Errors: 0" in text


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm is not installed here; CI runs helm lint and helm template")
def test_helm_lint_and_template():
    for values in ("lab-values.yaml", "production-values.yaml"):
        for cmd in (["helm", "lint", str(CHART), "-f", str(CHART / "ci" / values)], ["helm", "template", "olcf", str(CHART), "-f", str(CHART / "ci" / values)]):
            res = subprocess.run(cmd, capture_output=True, text=True, check=False)
            assert res.returncode == 0, res.stdout + res.stderr


def test_renderer_cli(tmp_path):
    res = subprocess.run([sys.executable, str(DEPLOY / "scripts" / "render_chart.py"), str(CHART), "-f", str(CHART / "ci" / "lab-values.yaml"), "--out", str(tmp_path), "--quiet"],
                         capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr
    assert (tmp_path / "deployment.yaml").is_file() and (tmp_path / "NOTES.txt").is_file()
    bad = subprocess.run([sys.executable, str(DEPLOY / "scripts" / "render_chart.py"), str(CHART)], capture_output=True, text=True, check=False)
    assert bad.returncode == 1 and "controller URL" in bad.stderr


# ------------------------------------------------------------------------------------------------------------------ compose
DOCKER = shutil.which("docker")


@pytest.mark.skipif(DOCKER is None, reason="docker is not installed")
@pytest.mark.parametrize("profiles", [["allinone"], ["cluster"], ["allinone", "tls"], ["cluster", "tls"]])
def test_compose_config(profiles, tmp_path):
    env = tmp_path / "env"
    env.write_text("OLCF_MASTER_KEY=" + "A" * 43 + "=\nOLCF_JWT_SIGNING_KEY=" + "b" * 48 + "\nPOSTGRES_PASSWORD=abc123\nMINIO_ROOT_PASSWORD=secretsecret\n")
    cmd = [DOCKER, "compose", "--env-file", str(env), "-f", str(DEPLOY / "docker-compose.yml")]
    for p in profiles:
        cmd += ["--profile", p]
    res = subprocess.run([*cmd, "config", "-q"], capture_output=True, text=True, check=False)
    assert res.returncode == 0, res.stderr


def test_compose_static_hardening():
    doc = yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())
    for name in ("olcf", "control-plane", "worker-1", "worker-2"):
        svc = doc["services"][name]
        assert svc.get("read_only") is True and svc.get("cap_drop") == ["ALL"], name
        assert "no-new-privileges:true" in svc["security_opt"], name
        assert str(svc.get("user", "10001")).startswith("10001"), name
    # no secret is ever given a literal value in the compose file
    text = (DEPLOY / "docker-compose.yml").read_text()
    for line in text.splitlines():
        if any(k in line for k in ("MASTER_KEY:", "JWT_SIGNING_KEY:", "PASSWORD:")) and not line.lstrip().startswith("#"):
            assert "${" in line, line
    for svc_name, svc in doc["services"].items():
        for port in svc.get("ports", []):
            assert re.match(r"^(127\.0\.0\.1|\$\{[A-Z_]+:-127\.0\.0\.1\}):", str(port)), f"{svc_name}: {port} must bind to loopback unless the operator opts in"


def test_env_example_has_no_real_secrets():
    for line in (DEPLOY / ".env.example").read_text().splitlines():
        key, _, value = line.partition("=")
        if key in ("OLCF_MASTER_KEY", "OLCF_JWT_SIGNING_KEY", "OLCF_ADMIN_PASSWORD", "POSTGRES_PASSWORD", "MINIO_ROOT_PASSWORD"):
            assert value == "", f"{key} must be empty in .env.example"


# ------------------------------------------------------------------------------------------------------------------ shell scripts
SHELL_FILES = [DEPLOY / "docker" / "entrypoint.sh", DEPLOY / "docker" / "worker-entrypoint.sh", DEPLOY / "docker" / "enroll.sh", DEPLOY / "docker" / "bake-runtime.sh",
               DEPLOY / "scripts" / "gen-env.sh", ROOT / "scripts" / "make_offline_bundle.sh"]


@pytest.mark.parametrize("path", SHELL_FILES, ids=lambda p: p.name)
def test_shell_scripts_parse_and_are_clean(path):
    assert subprocess.run(["sh", "-n", str(path)], capture_output=True, text=True, check=False).returncode == 0
    text = path.read_text()
    assert "SPDX-License-Identifier: Apache-2.0" in text[:300] and "eval $" not in text
    if shutil.which("shellcheck"):
        res = subprocess.run(["shellcheck", "-s", "sh", "-S", "warning", str(path)], capture_output=True, text=True, check=False)
        assert res.returncode == 0, res.stdout


def _fake_bin(tmp_path: Path, name: str, body: str) -> Path:
    d = tmp_path / "bin"
    d.mkdir(exist_ok=True)
    p = d / name
    p.write_text("#!/bin/sh\n" + textwrap.dedent(body))
    p.chmod(0o755)
    return d


def _entrypoint_env(tmp_path: Path, bindir: Path, **extra: str) -> dict[str, str]:
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "OLCF_DATA_DIR": str(tmp_path / "data"), "LOG": str(tmp_path / "calls.log"), "OLCF_PORT": "9999",
           "OLCF_BIND_HOST": "127.0.0.1", "OLCF_ENV_DUMP": str(tmp_path / "env.txt")}
    env.update(extra)
    return env


FAKE_OLCF = """
    echo "olcf $*" >> "$LOG"
    case "$1" in
      init) cat > "${OLCF_DATA_DIR}/stdin.txt" 2>/dev/null || true; mkdir -p "$OLCF_DATA_DIR"; : > "$OLCF_DATA_DIR/config.toml";;
      serve) env | grep '^OLCF_' | sort > "$OLCF_ENV_DUMP";;
    esac
"""


def test_entrypoint_first_start_then_serve(tmp_path):
    bindir = _fake_bin(tmp_path, "olcf", FAKE_OLCF)
    (tmp_path / "data").mkdir()
    secret = tmp_path / "master"
    secret.write_text("S3CRET-KEY-VALUE")
    env = _entrypoint_env(tmp_path, bindir, OLCF_ADMIN_PASSWORD="pw-from-env-1234", OLCF_PUBLIC_URL="https://olcf.example.com", OLCF_MASTER_KEY_FILE=str(secret),  # gitleaks:allow
                          OLCF_SERVE_ARGS="--no-embedded-worker")
    res = subprocess.run(["sh", str(DEPLOY / "docker" / "entrypoint.sh"), "serve"], env=env, capture_output=True, text=True, timeout=60, check=False)
    assert res.returncode == 0, res.stderr
    calls = (tmp_path / "calls.log").read_text().splitlines()
    assert calls[0] == "olcf init --host 127.0.0.1 --port 9999 --public-url https://olcf.example.com --admin-password-stdin"
    assert calls[1] == "olcf serve --host 127.0.0.1 --port 9999 --no-embedded-worker --public-url https://olcf.example.com" or calls[1].startswith("olcf serve --host 127.0.0.1 --port 9999")
    assert "init" not in calls[1].split()
    assert (tmp_path / "data" / "stdin.txt").read_text().strip() == "pw-from-env-1234"
    assert "pw-from-env-1234" not in "\n".join(calls), "the administrator password must never be on a command line"
    assert "OLCF_MASTER_KEY=S3CRET-KEY-VALUE" in (tmp_path / "env.txt").read_text(), "<NAME>_FILE must be expanded for the server"
    # second start: config.toml exists, so no init
    (tmp_path / "calls.log").unlink()
    res = subprocess.run(["sh", str(DEPLOY / "docker" / "entrypoint.sh")], env=env, capture_output=True, text=True, timeout=60, check=False)
    assert res.returncode == 0, res.stderr
    assert [c.split()[1] for c in (tmp_path / "calls.log").read_text().splitlines()] == ["serve"]


def test_entrypoint_rejects_unreadable_secret_file_and_passes_commands_through(tmp_path):
    bindir = _fake_bin(tmp_path, "olcf", FAKE_OLCF)
    env = _entrypoint_env(tmp_path, bindir, OLCF_MASTER_KEY_FILE=str(tmp_path / "missing"))
    res = subprocess.run(["sh", str(DEPLOY / "docker" / "entrypoint.sh"), "serve"], env=env, capture_output=True, text=True, timeout=60, check=False)
    assert res.returncode == 64 and "not readable" in res.stderr
    env.pop("OLCF_MASTER_KEY_FILE")
    res = subprocess.run(["sh", str(DEPLOY / "docker" / "entrypoint.sh"), "echo", "hello"], env=env, capture_output=True, text=True, timeout=60, check=False)
    assert res.returncode == 0 and res.stdout.strip() == "hello"


def test_worker_entrypoint_uses_enrol_dir_token_and_flags(tmp_path):
    bindir = _fake_bin(tmp_path, "olcf-worker", 'echo "args: $*" > "$LOG"; echo "token: ${OLCF_WORKER_TOKEN:-none}" >> "$LOG"\n')
    enroll = tmp_path / "enroll"
    enroll.mkdir()
    (enroll / "w-0.token").write_text("TOK123")
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "LOG": str(tmp_path / "w.log"), "OLCF_WORKER_STATE_DIR": str(tmp_path / "state"), "OLCF_ENROLL_DIR": str(enroll),
           "OLCF_WORKER_NAME": "w-0", "OLCF_WORKER_URL": "http://olcf:8470", "OLCF_WORKER_POOL": "gpu", "OLCF_WORKER_INSECURE_WS": "true", "OLCF_WORKER_MAX_JOBS": "3",
           "OLCF_WORKER_LABELS": "zone=a"}
    res = subprocess.run(["sh", str(DEPLOY / "docker" / "worker-entrypoint.sh"), "run"], env=env, capture_output=True, text=True, timeout=60, check=False)
    assert res.returncode == 0, res.stderr
    log = (tmp_path / "w.log").read_text()
    assert "run --name w-0 --state-dir" in log and "--pool gpu" in log and "--labels zone=a" in log and "--max-jobs 3" in log and "--insecure-ws" in log
    assert "TOK123" not in log.split("token:")[0], "the token must come via the environment, not argv"
    assert "token: TOK123" in log


def test_worker_entrypoint_without_token_fails_clearly(tmp_path):
    bindir = _fake_bin(tmp_path, "olcf-worker", "exit 0\n")
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "OLCF_WORKER_STATE_DIR": str(tmp_path / "state"), "OLCF_ENROLL_DIR": str(tmp_path / "nonexistent"), "OLCF_WORKER_URL": "http://x"}
    res = subprocess.run(["sh", str(DEPLOY / "docker" / "worker-entrypoint.sh")], env=env, capture_output=True, text=True, timeout=60, check=False)
    assert res.returncode == 2 and "OLCF_WORKER_TOKEN" in res.stderr


# ------------------------------------------------------------------------------------------------------------------ runtime trust in the images
# No Docker daemon here: the Dockerfiles are checked statically; the scripts they run are executed for real with a fake `olcf` (and, once, the real CLI).
BAKE = DEPLOY / "docker" / "bake-runtime.sh"
FAKE_BAKE_OLCF = '''
    echo "args: $*" >> "$LOG"
    echo "env: ${OLCF_ENVIRONMENT:-unset}" >> "$LOG"
    echo "trust: ${OLCF_RUNTIME_TRUST_DIR:-unset}" >> "$LOG"
    echo "data: ${OLCF_DATA_DIR:-unset}" >> "$LOG"
'''


def _bake(tmp_path: Path, runtime: str, *, keys: tuple[str, ...] = (), files: tuple[str, ...] = ()) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    bindir = _fake_bin(tmp_path, "olcf", FAKE_BAKE_OLCF)
    trust, seed = tmp_path / "trust", tmp_path / "seed"
    trust.mkdir(exist_ok=True)
    for k in keys:
        (trust / k).write_text("-----BEGIN PUBLIC KEY-----\n-----END PUBLIC KEY-----\n")
    for f in files:
        (trust / f).write_text("{}")
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "LOG": str(tmp_path / "bake.log")}
    res = subprocess.run(["sh", str(BAKE), runtime, str(seed), str(trust)], env=env, capture_output=True, text=True, timeout=60, check=False)
    return res, trust, seed


def test_bake_without_operator_keys_trusts_the_development_key_only_for_that_one_install(tmp_path):
    res, _, seed = _bake(tmp_path, "olcf-rt-2.0.0")
    assert res.returncode == 0, res.stderr
    log = (tmp_path / "bake.log").read_text()
    assert "args: runtime install olcf-rt-2.0.0 --no-jdk" in log and "env: development" in log and "trust: unset" in log
    assert f"data: {seed}" in log
    assert (seed / ".dev-signed").is_file(), "the seed records that it is development-signed"


def test_bake_with_operator_keys_stays_production_and_uses_the_trust_dir(tmp_path):
    res, trust, seed = _bake(tmp_path, "olcf-rt-2.0.0.json", keys=("ops.pub",), files=("olcf-rt-2.0.0.json",))
    assert res.returncode == 0, res.stderr
    log = (tmp_path / "bake.log").read_text()
    assert f"args: runtime install {trust}/olcf-rt-2.0.0.json --no-jdk" in log
    assert "env: unset" in log, "production: OLCF_ENVIRONMENT must not be set to development when an operator key is given"
    assert f"trust: {trust}" in log
    assert not (seed / ".dev-signed").exists()


@pytest.mark.parametrize(("runtime", "keys", "files", "message"), [
    ("olcf-rt-2.0.0", ("ops.pub",), (), "bundled runtime id"),                  # operator keys + a dev-signed bundled id: would be refused, say so at build time
    ("olcf-rt-2.0.0.json", ("ops.pub",), (), "not found"),                        # the manifest is not in the trust directory
    ("olcf-rt-2.0.0.json", (), ("olcf-rt-2.0.0.json",), "public key"),            # a custom manifest without its signer's key
])
def test_bake_refuses_inconsistent_inputs_before_installing(tmp_path, runtime, keys, files, message):
    res, _, _ = _bake(tmp_path, runtime, keys=keys, files=files)
    assert res.returncode == 2 and message in res.stderr, res.stderr
    assert not (tmp_path / "bake.log").exists(), "nothing may be installed"


def test_bake_without_a_runtime_does_nothing(tmp_path):
    res, _, seed = _bake(tmp_path, "")
    assert res.returncode == 0 and seed.is_dir() and not (tmp_path / "bake.log").exists()


def test_bake_with_the_real_cli_lists_the_bundled_manifests_it_trusts(tmp_path):
    """Dev mode against the real `olcf`: an unknown id fails offline and the message lists the bundled ids - proof that OLCF_ENVIRONMENT=development reached the trust store
    (a production process lists none: "none ship with this build")."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "olcf").write_text(f'#!/bin/sh\nexec "{sys.executable}" -m olcf.cli.main "$@"\n')
    (bindir / "olcf").chmod(0o755)
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "PYTHONPATH": os.pathsep.join([str(ROOT / "src")]), "OLCF_CONFIG_DIR": str(tmp_path / "cli"), "HOME": str(tmp_path)}
    res = subprocess.run(["sh", str(BAKE), "olcf-rt-9.9.9", str(tmp_path / "seed"), str(tmp_path / "no-trust")], env=env, capture_output=True, text=True, timeout=120, check=False)
    assert res.returncode != 0 and "olcf-rt-2.0.0" in res.stderr and "none ship with this build" not in res.stderr, res.stderr


def _run_entrypoint(script: str, tmp_path: Path, bindir: Path, **extra: str) -> subprocess.CompletedProcess[str]:
    env = _entrypoint_env(tmp_path, bindir, OLCF_WORKER_STATE_DIR=str(tmp_path / "state"), OLCF_WORKER_TOKEN="t", OLCF_WORKER_URL="http://x", OLCF_ENROLL_DIR=str(tmp_path / "none"),
                          OLCF_SEED_DIR=str(tmp_path / "seed"), **extra)
    return subprocess.run(["sh", str(DEPLOY / "docker" / script), "run" if "worker" in script else "serve"], env=env, capture_output=True, text=True, timeout=60, check=False)


@pytest.mark.parametrize(("script", "binary"), [("entrypoint.sh", "olcf"), ("worker-entrypoint.sh", "olcf-worker")])
def test_entrypoints_export_the_image_trust_dir_only_when_it_has_a_key(tmp_path, script, binary):
    body = 'env | grep "^OLCF_RUNTIME_TRUST_DIR=" > "$OLCF_ENV_DUMP" || : > "$OLCF_ENV_DUMP"\n'
    bindir = _fake_bin(tmp_path, binary, 'case "$1" in init) cat >/dev/null; mkdir -p "$OLCF_DATA_DIR"; : > "$OLCF_DATA_DIR/config.toml";; esac\n' + body)
    (tmp_path / "data").mkdir(exist_ok=True)
    trust = tmp_path / "image-trust"
    trust.mkdir()
    res = _run_entrypoint(script, tmp_path, bindir, OLCF_IMAGE_TRUST_DIR=str(trust))
    assert res.returncode == 0, res.stderr
    assert (tmp_path / "env.txt").read_text() == "", "an empty trust directory must not become the (exclusive) trust anchor"
    (trust / "ops.pub").write_text("k")
    assert _run_entrypoint(script, tmp_path, bindir, OLCF_IMAGE_TRUST_DIR=str(trust)).returncode == 0
    assert (tmp_path / "env.txt").read_text().strip() == f"OLCF_RUNTIME_TRUST_DIR={trust}"
    assert _run_entrypoint(script, tmp_path, bindir, OLCF_IMAGE_TRUST_DIR=str(trust), OLCF_RUNTIME_TRUST_DIR="/mnt/mine").returncode == 0
    assert (tmp_path / "env.txt").read_text().strip() == "OLCF_RUNTIME_TRUST_DIR=/mnt/mine", "the operator's own variable wins"


def test_entrypoint_warns_when_a_production_container_carries_a_development_signed_seed(tmp_path):
    bindir = _fake_bin(tmp_path, "olcf", FAKE_OLCF)
    seed = tmp_path / "seed"
    seed.mkdir()
    (seed / ".dev-signed").write_text("")
    (tmp_path / "data").mkdir()
    res = _run_entrypoint("entrypoint.sh", tmp_path, bindir, OLCF_IMAGE_TRUST_DIR=str(tmp_path / "nokeys"), OLCF_ENVIRONMENT="production")
    assert res.returncode == 0 and "development key" in res.stderr and "OLCF_ENVIRONMENT=development" in res.stderr, res.stderr
    res = _run_entrypoint("entrypoint.sh", tmp_path, bindir, OLCF_IMAGE_TRUST_DIR=str(tmp_path / "nokeys"), OLCF_ENVIRONMENT="development")
    assert "development key" not in res.stderr


def test_dockerfiles_keep_the_final_image_production_and_trust_the_dev_key_nowhere_else():
    for name in ("Dockerfile", "Dockerfile.worker"):
        lines = [ln for ln in (DEPLOY / name).read_text().splitlines() if not ln.lstrip().startswith("#")]
        text = "\n".join(lines)
        assert "OLCF_ENVIRONMENT=development" not in text, f"{name}: the development key may only be trusted inside bake-runtime.sh (throwaway stage)"
        assert "ARG RUNTIME_TRUST_DIR" in text and "COPY ${RUNTIME_TRUST_DIR}/ /etc/olcf/runtime-trust/" in text, name
    main = (DEPLOY / "Dockerfile").read_text()
    seed_stage = main.split("AS runtime-seed", 1)[1].split("AS final", 1)[0]
    assert "olcf-bake-runtime" in seed_stage and "deploy/docker/bake-runtime.sh" in seed_stage
    final = main.split("AS final", 1)[1]
    assert "OLCF_ENVIRONMENT=production" in final and "bake-runtime" not in final, "the final image is production and does not carry the bake script"
    assert (DEPLOY / "runtime-trust").is_dir() and not list((DEPLOY / "runtime-trust").glob("*.pub")), "no key ships in the default trust directory"


def test_compose_passes_the_trust_build_arg_and_lets_a_tryout_pick_development():
    doc = yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())
    for name in ("olcf", "control-plane", "worker-1"):
        assert "RUNTIME_TRUST_DIR" in doc["services"][name]["build"]["args"], name
    assert "OLCF_ENVIRONMENT:-production" in doc["services"]["olcf"]["environment"]["OLCF_ENVIRONMENT"], "default stays production, a try-out can override it from .env"


def test_healthcheck_script():
    class H(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(200 if self.path == "/v1/health" else 404)
            self.end_headers()

        def log_message(self, *a):
            return

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = HTTPServer(("127.0.0.1", port), H)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    try:
        env = {**os.environ, "OLCF_PORT": str(port)}
        env.pop("OLCF_TLS__ENABLED", None)
        env.pop("OLCF_TLS_ENABLED", None)
        assert subprocess.run([sys.executable, "-I", str(DEPLOY / "docker" / "healthcheck.py")], env=env, check=False, timeout=30).returncode == 0
    finally:
        srv.shutdown()
        srv.server_close()
        t.join(5)
    assert subprocess.run([sys.executable, "-I", str(DEPLOY / "docker" / "healthcheck.py")], env={**env, "OLCF_PORT": str(port)}, check=False, timeout=30).returncode == 1


def test_healthcheck_script_probes_the_path_it_is_given_and_only_health_paths():
    """The Helm readiness probe passes ``/v1/health/ready``: a 503 there (database down, production trusting the development runtime key) must fail the probe."""
    class H(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response({"/v1/health": 200, "/v1/health/ready": 503}.get(self.path, 404))
            self.end_headers()

        def log_message(self, *a):
            return

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = HTTPServer(("127.0.0.1", port), H)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    env = {**os.environ, "OLCF_PORT": str(port)}
    env.pop("OLCF_TLS__ENABLED", None)
    env.pop("OLCF_TLS_ENABLED", None)
    script = str(DEPLOY / "docker" / "healthcheck.py")
    try:
        assert subprocess.run([sys.executable, "-I", script], env=env, check=False, timeout=30).returncode == 0  # liveness
        assert subprocess.run([sys.executable, "-I", script, "/v1/health/ready"], env=env, check=False, timeout=30).returncode == 1  # readiness 503
        assert subprocess.run([sys.executable, "-I", script, "/v1/admin/info"], env=env, check=False, timeout=30).returncode == 2  # not a health path
    finally:
        srv.shutdown()
        srv.server_close()
        t.join(5)


def test_helm_readiness_probe_uses_the_readiness_endpoint_while_liveness_and_startup_do_not(tmp_path):
    c = _pod(_render_with(tmp_path, {}), "Deployment")["containers"][0]
    assert c["readinessProbe"]["exec"]["command"][-1] == "/v1/health/ready"
    for probe in ("livenessProbe", "startupProbe"):
        assert c[probe]["exec"]["command"][-1] == "/usr/local/bin/olcf-healthcheck", probe


# ------------------------------------------------------------------------------------------------------------------ service files
def test_systemd_units_are_hardened():
    required = ["NoNewPrivileges=true", "PrivateTmp=true", "ProtectSystem=strict", "ProtectHome=true", "RestrictSUIDSGID=true", "CapabilityBoundingSet=",
                "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6", "LockPersonality=true", "User=olcf", "ProtectKernelTunables=true", "SystemCallFilter=@system-service"]
    for unit in (DEPLOY / "systemd").glob("*.service"):
        text = unit.read_text()
        for line in required:
            assert line in text, f"{unit.name} lacks {line}"
        # the JVM needs W+X memory: the hardening switch that forbids it must stay off
        assert not any(ln.strip() == "MemoryDenyWriteExecute=true" for ln in text.splitlines()), unit.name
        assert "ExecStart=/opt/olcf/current/bin/" in text
        assert "OLCF_MASTER_KEY=" not in "\n".join(ln for ln in text.splitlines() if not ln.startswith("#")), "secrets belong in the EnvironmentFile"
    server = (DEPLOY / "systemd" / "olcf.service").read_text()
    assert "--host 127.0.0.1" in server, "the shipped unit must not listen on all interfaces by default"


@pytest.mark.skipif(shutil.which("systemd-analyze") is None, reason="systemd-analyze is not available")
def test_systemd_analyze_verify(tmp_path):
    for unit in (DEPLOY / "systemd").glob("*.service"):
        copy = tmp_path / unit.name
        copy.write_text(unit.read_text())
        res = subprocess.run(["systemd-analyze", "verify", str(copy)], capture_output=True, text=True, check=False)
        bad = [ln for ln in res.stderr.splitlines() if "Unknown" in ln or "Invalid" in ln or "bad" in ln.lower()]
        assert not bad, (unit.name, bad)


def test_launchd_templates_and_placeholders():
    for plist in (DEPLOY / "launchd").glob("*.plist"):
        data = plistlib.loads(plist.read_bytes())
        assert data["Label"].startswith("org.olcf.") and data["RunAtLoad"] is True
        args = data["ProgramArguments"]
        assert args[0].startswith("__PREFIX__/current/bin/")
        assert not any(a.startswith("/usr/bin/python") or a.startswith("/usr/bin/java") for a in args), "must never use the Xcode stubs"
        assert "~" not in plist.read_text().split("-->", 3)[-1], "launchd does not expand ~"
        assert data["Umask"] == 0o077


def test_windows_task_xml_and_script_structure():
    xml = (DEPLOY / "windows" / "olcf-server-task.xml").read_text(encoding="utf-8")
    root = ET.fromstring(xml.split("?>", 1)[-1])
    ns = {"t": "http://schemas.microsoft.com/windows/2004/02/mit/task"}
    assert root.find(".//t:RunLevel", ns).text == "LeastPrivilege"
    ps = (DEPLOY / "windows" / "install-service.ps1").read_text(encoding="utf-8")
    assert ps.count("{") == ps.count("}") and ps.count("(") == ps.count(")")
    assert "Set-StrictMode" in ps and "-Verb RunAs" not in ps and "Invoke-Expression" not in ps and "iex " not in ps
    assert "RunLevel Limited" in ps


# ------------------------------------------------------------------------------------------------------------------ offline bundle script
def test_make_offline_bundle_validates_arguments(tmp_path):
    script = ROOT / "scripts" / "make_offline_bundle.sh"
    assert subprocess.run(["sh", str(script), "--help"], capture_output=True, text=True, check=False).returncode == 0
    res = subprocess.run(["sh", str(script), "--out", str(tmp_path / "o")], capture_output=True, text=True, check=False)
    assert res.returncode != 0 and "--wheel" in res.stderr
    not_wheel = tmp_path / "x.txt"
    not_wheel.write_text("x")
    res = subprocess.run(["sh", str(script), "--wheel", str(not_wheel), "--out", str(tmp_path / "o")], capture_output=True, text=True, check=False)
    assert res.returncode != 0 and "not an olcf wheel" in res.stderr
    full = tmp_path / "full"
    full.mkdir()
    (full / "f").write_text("x")
    wheel = tmp_path / "olcf-0.0.1-py3-none-any.whl"
    wheel.write_text("x")
    res = subprocess.run(["sh", str(script), "--wheel", str(wheel), "--out", str(full)], capture_output=True, text=True, check=False)
    assert res.returncode != 0 and "not empty" in res.stderr


def test_dockerfiles_run_as_non_root_with_healthcheck():
    for name in ("Dockerfile", "Dockerfile.worker"):
        text = (DEPLOY / name).read_text()
        assert "USER 10001:10001" in text and "HEALTHCHECK" in text and "ENTRYPOINT" in text, name
        assert "latest" not in [ln.split(":")[-1].strip() for ln in text.splitlines() if ln.startswith("FROM ")], f"{name}: pin base images"
        assert "curl | sh" not in text and "curl -fsSL" not in text.replace("curl -fsSL --retry", ""), f"{name}: no curl-pipe installs"


# ------------------------------------------------------------------------------------------------------------------ review F05: NetworkPolicy and Spark ports
_PORT_ROLES = {"masterRpc": "master_rpc", "masterUi": "master_ui", "workerRpc": "worker_rpc", "workerUi": "worker_ui", "driverUi": "driver_ui", "driver": "driver",
               "blockManager": "block_manager"}


def _policy(objs: list[dict], component: str) -> dict:
    (pol,) = [o for o in _by(objs, "NetworkPolicy") if o["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == component]
    return pol


def _covered(rules: list[dict], port: int) -> bool:
    for r in rules:
        end = r.get("endPort", r["port"])
        if isinstance(r["port"], int) and r["port"] <= port <= end:
            return True
    return False


def _rules_to(policy: dict, direction: str, component: str) -> list[dict]:
    """Port rules of the ingress ``from`` / egress ``to`` entries whose peer is a podSelector for ``component``."""
    key = "from" if direction == "ingress" else "to"
    out: list[dict] = []
    for rule in policy["spec"][direction]:
        for peer in rule.get(key) or []:
            if (peer.get("podSelector") or {}).get("matchLabels", {}).get("app.kubernetes.io/component") == component:
                out += rule.get("ports") or []
    return out


def _compute_ranges(values: dict) -> dict[str, tuple[int, int]]:
    return {role: (values["spark"]["ports"][name]["start"], values["spark"]["ports"][name]["end"]) for name, role in _PORT_ROLES.items()}


def test_f05_the_chart_and_the_control_plane_share_the_same_default_port_ranges():
    from olcf.core.config import ComputeSettings

    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert _compute_ranges(values) == ComputeSettings().port_ranges(), "values.yaml spark.ports must equal the control plane's built-in ranges, or a default install opens the wrong ports"


def test_f05_the_configmap_hands_the_chart_ranges_to_the_control_plane(tmp_path, monkeypatch):
    from olcf.core.config import load_settings

    sets = ["spark.ports.masterRpc.start=7500", "spark.ports.masterRpc.end=7507", "spark.ports.workerRpc.start=42000", "spark.ports.workerRpc.end=42049",
            "spark.ports.workerUi.start=42100", "spark.ports.workerUi.end=42149", "spark.ports.driver.start=43000", "spark.ports.driver.end=43009",
            "spark.ports.blockManager.start=43100", "spark.ports.blockManager.end=43119", "spark.ports.masterUi.start=19000", "spark.ports.masterUi.end=19003",
            "spark.ports.driverUi.start=4050", "spark.ports.driverUi.end=4059"]
    objs = _objects(_render("lab-values.yaml", sets=sets))
    (cm,) = [o for o in _by(objs, "ConfigMap") if o["metadata"]["name"].endswith("-config")]
    for k, v in cm["data"].items():
        if k.startswith("OLCF_COMPUTE__"):
            monkeypatch.setenv(k, v)
    got = load_settings(tmp_path).compute.port_ranges()
    assert got == {"master_rpc": (7500, 7507), "master_ui": (19000, 19003), "worker_rpc": (42000, 42049), "worker_ui": (42100, 42149), "driver_ui": (4050, 4059),
                   "driver": (43000, 43009), "block_manager": (43100, 43119)}


@pytest.mark.parametrize("values", ["lab-values.yaml", "production-values.yaml"])
@pytest.mark.parametrize("custom", [False, True])
def test_f05_rendered_policies_cover_every_configured_spark_port(values, custom):
    """For every role the control plane can allocate from (master RPC/UI, worker RPC/UI, driver UI, driver, block manager) the rendered policy lets the right peers connect -
    checked on the first and last port of each range, for the defaults and for moved/resized ranges."""
    sets = ["spark.ports.masterRpc.end=7100", "spark.ports.workerRpc.start=45000", "spark.ports.workerRpc.end=45010", "spark.ports.driver.start=46000",
            "spark.ports.driver.end=46002"] if custom else []
    objs = _objects(_render(values, sets=[*sets, "networkPolicy.egressAllowAll=false"]))  # the restrictive branch is the one that must name every port
    cfg = yaml.safe_load((CHART / "values.yaml").read_text())
    for s in sets:
        k, v = s.split("=")
        cur = cfg
        *path, leaf = k.split(".")
        for p in path:
            cur = cur[p]
        cur[leaf] = int(v)
    ranges = _compute_ranges(cfg)
    workers, cp = _policy(objs, "worker"), _policy(objs, "control-plane")
    for role, (lo, hi) in ranges.items():
        for port in (lo, hi):
            assert _covered(_rules_to(workers, "ingress", "control-plane"), port), f"control plane -> worker ingress misses {role} {port}"
            assert _covered(_rules_to(cp, "egress", "worker"), port), f"control plane -> worker egress misses {role} {port}"
            if role != "driver_ui":  # the driver UI is a control-plane / browser path, not a worker-to-worker one
                assert _covered(_rules_to(workers, "ingress", "worker"), port), f"worker <-> worker ingress misses {role} {port}"
                assert _covered(_rules_to(workers, "egress", "worker"), port), f"worker <-> worker egress misses {role} {port}"
    # nothing outside the configured ranges is opened to the Spark peers
    allowed = [r for lo_hi in ranges.values() for r in [lo_hi]]
    for rule in _rules_to(workers, "ingress", "control-plane"):
        assert any(lo <= rule["port"] and rule.get("endPort", rule["port"]) <= hi for lo, hi in allowed), rule


def test_f05_worker_ingress_is_selected_by_pod_labels_not_by_namespace_or_address():
    objs = _objects(_render("lab-values.yaml"))
    for pol in _by(objs, "NetworkPolicy"):
        for direction, key in (("ingress", "from"), ("egress", "to")):
            for rule in pol["spec"].get(direction) or []:
                for peer in rule.get(key) or []:
                    sel = peer.get("podSelector")
                    assert sel is None or sel.get("matchLabels", {}).get("app.kubernetes.io/instance") == "olcf", f"unlabelled pod peer in {pol['metadata']['name']}: {peer}"
    workers = _policy(objs, "worker")
    assert all("ipBlock" not in peer for rule in workers["spec"]["ingress"] for peer in rule.get("from", []))


def test_f05_restrictive_egress_branch_is_complete():
    objs = _objects(_render("production-values.yaml"))
    cp, workers = _policy(objs, "control-plane"), _policy(objs, "worker")
    egress = cp["spec"]["egress"]

    def has(rules, *, port=None, peer=None, comp=None):
        for r in rules:
            ports = [p["port"] for p in r.get("ports", [])]
            if port is not None and port not in ports:
                continue
            tos = r.get("to", [])
            if peer == "ipBlock" and not any("ipBlock" in t for t in tos):
                continue
            if peer == "dns" and not any("namespaceSelector" in t and "podSelector" in t for t in tos):
                continue
            if comp and not any((t.get("podSelector") or {}).get("matchLabels", {}).get("app.kubernetes.io/component") == comp for t in tos):
                continue
            return True
        return False

    assert has(egress, port=53, peer="dns"), "DNS"
    assert has(egress, port=5432, peer="ipBlock"), "the external database"
    assert has(egress, port=443, peer="ipBlock"), "HTTPS (object storage, identity provider)"
    assert has(egress, port=6443, peer="ipBlock"), "the Kubernetes API server of the compute provider (kubernetesProvider.enabled)"
    assert has(egress, comp="worker"), "control plane -> workers"
    wegress = workers["spec"]["egress"]
    assert has(wegress, port=53, peer="dns") and has(wegress, port="http", comp="control-plane") and has(wegress, comp="worker") and has(wegress, port=443, peer="ipBlock")
    # a policy that restricts egress must not also contain the allow-all rule
    assert {} not in egress and {} not in wegress
    # the bundled database is selected by labels, an external one by CIDR
    bundled = _objects(_render("lab-values.yaml", sets=["networkPolicy.egressAllowAll=false"]))
    assert has(_policy(bundled, "control-plane")["spec"]["egress"], port=5432, comp="postgres")
    # without the Kubernetes provider there is no API-server rule
    no_k8s = _objects(_render("production-values.yaml", sets=["kubernetesProvider.enabled=false"]))
    assert not has(_policy(no_k8s, "control-plane")["spec"]["egress"], port=6443)


def test_f05_monitoring_and_extra_rules_reach_the_restrictive_policies():
    sets = ["networkPolicy.egressAllowAll=false", "networkPolicy.allowIngressFromAnywhere=false",
            "networkPolicy.monitoring.egressTo=[{to: [{namespaceSelector: {matchLabels: {team: obs}}}], ports: [{port: 4317, protocol: TCP}]}]",
            "networkPolicy.monitoring.ingressFrom=[{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: monitoring}}}]",
            "networkPolicy.egress.extra=[{to: [{ipBlock: {cidr: 10.2.0.0/16}}], ports: [{port: 9000}]}]",
            "networkPolicy.workerIngressFrom=[{podSelector: {matchLabels: {role: external-driver}}}]"]
    objs = _objects(_render("lab-values.yaml", sets=sets))
    cp, workers = _policy(objs, "control-plane"), _policy(objs, "worker")
    flat = str(cp["spec"])
    assert "4317" in flat and "monitoring" in flat and "10.2.0.0/16" in flat
    assert "10.2.0.0/16" in str(workers["spec"]["egress"])
    assert any(peer.get("podSelector", {}).get("matchLabels") == {"role": "external-driver"} for rule in workers["spec"]["ingress"] for peer in rule.get("from", []))


def test_f05_embedded_worker_gets_driver_and_block_manager_ingress_from_the_workers():
    objs = _objects(_render("lab-values.yaml", sets=["controlPlane.embeddedWorker=true"]))
    rules = _rules_to(_policy(objs, "control-plane"), "ingress", "worker")
    assert _covered(rules, 41000) and _covered(rules, 41199) and not _covered(rules, 7077)
    assert not _rules_to(_policy(_objects(_render("lab-values.yaml")), "control-plane"), "ingress", "worker")


def test_f05_allow_all_egress_is_still_available_and_invalid_ranges_are_rejected():
    objs = _objects(_render("lab-values.yaml"))
    assert {} in _policy(objs, "control-plane")["spec"]["egress"]
    for bad in ("spark.ports.driver.start=0", "spark.ports.driver.end=70000"):
        with pytest.raises(rc.TemplateError, match="values.schema.json"):
            _render("lab-values.yaml", sets=[bad])
