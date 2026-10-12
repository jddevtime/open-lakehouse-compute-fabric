# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Import of Microsoft Fabric / Data Factory pipeline JSON (``POST /v1/pipelines/import-fabric``).

The importer translates the *supported subset* below into an OLCF pipeline definition (``olcf.pipeline/1``) and returns an **explicit report**: every activity is listed
as imported, imported with warnings, or unsupported, with the reason and what to do about it. Nothing is silently dropped or guessed. The machine-readable form of the
table is ``schemas/fabric-import-support.json`` (generated from :data:`SUPPORT`, checked by the test-suite).

Supported subset (Fabric activity type -> OLCF activity)
---------------------------------------------------------
=====================  ==============  =====================================================================================================================================
Fabric                 OLCF            Notes
=====================  ==============  =====================================================================================================================================
TridentNotebook        notebook        notebook id / name resolved with ``item_map``; Fabric workspace ids are ignored (the OLCF workspace of the pipeline is used)
SparkJobDefinition     spark_job       arguments / conf of the activity are reported as dropped (set them in the Spark job definition item)
Copy                   copy            lakehouse files <-> lakehouse files / tables (csv, parquet, json lines, binary); ``translator`` column mappings are dropped (warning);
                                       any other store (Azure, SQL, ...) is unsupported
WebActivity            http            ``url`` based requests; authentication secrets are never imported (re-add as a secret reference); connection-based (relativeUrl) unsupported
WebHook                webhook_wait    the callback URL is provided to the endpoint in the request body as ``callBackUri`` and in the OLCF-standard header
ExecutePipeline        child_pipeline  pipeline resolved with ``item_map`` (id or name)
Wait                   wait
SetVariable            set_variable    pipeline *return values* (``setSystemVariable``) are unsupported
AppendVariable         set_variable    mode ``append``
IfCondition            if
ForEach                for_each        ``isSequential``, ``batchCount``
Switch                 switch
Until                  until           ``timeout`` becomes the activity's ``timeoutSeconds`` policy
Fail                   fail
=====================  ==============  =====================================================================================================================================

Everything else is **unsupported** and is reported: Lookup, GetMetadata, Filter, Delete, Script, AzureFunction, Databricks*, RefreshDataflow, Office365Outlook, Teams,
Validation, SqlServerStoredProcedure, semantic-model refresh, KQL, Custom, HDInsight*, Synapse*, SSIS, ... (see :data:`UNSUPPORTED_HINTS`). With ``on_unsupported`` =

* ``placeholder`` (default) - a ``fail`` activity with the same name and dependencies keeps the DAG shape; the run fails exactly there with an explanatory message;
* ``skip``  - the activity is omitted (dependents that referenced it lose that dependency, reported);
* ``error`` - the import is rejected.

Expressions use the same syntax as Fabric (``@pipeline().parameters.x``, ``@activity('A').output...``, ``@variables('v')``, ``@item()``, ``@concat(...)``); each translated
expression is parsed by the OLCF expression engine and a parse failure is a warning at the exact path. Parameters / variables are mapped by type
(``securestring`` -> ``secret_ref`` without its default; variables String / Boolean / Array / Integer). The result is also validated with the normal pipeline validator.
"""

from __future__ import annotations

import re
from typing import Any

from olcf.pipelines import model as M
from olcf.pipelines.expressions import ExpressionError, analyze_value

PLACEHOLDER = "placeholder"
MAX_IMPORT_ACTIVITIES = M.MAX_ACTIVITIES

#: type -> (olcf type, status, note). ``status`` is ``supported`` or ``partial``.
SUPPORT: dict[str, dict[str, str]] = {
    "TridentNotebook": {"olcf": "notebook", "status": "supported", "note": "Notebook id/name resolved with item_map; Fabric workspace ids are ignored."},
    "SparkJobDefinition": {"olcf": "spark_job", "status": "partial", "note": "Activity-level arguments and conf are dropped (reported)."},
    "Copy": {"olcf": "copy", "status": "partial", "note": "Lakehouse files/tables only; column mappings (translator) are dropped (reported)."},
    "WebActivity": {"olcf": "http", "status": "partial", "note": "url-based requests only; credentials are never imported."},
    "WebHook": {"olcf": "webhook_wait", "status": "supported", "note": "Callback URL is sent as callBackUri in the request body."},
    "ExecutePipeline": {"olcf": "child_pipeline", "status": "supported", "note": "Pipeline resolved with item_map (id or name)."},
    "Wait": {"olcf": "wait", "status": "supported", "note": ""},
    "SetVariable": {"olcf": "set_variable", "status": "partial", "note": "Pipeline return values (setSystemVariable) are unsupported."},
    "AppendVariable": {"olcf": "set_variable", "status": "supported", "note": "mode=append"},
    "IfCondition": {"olcf": "if", "status": "supported", "note": ""},
    "ForEach": {"olcf": "for_each", "status": "supported", "note": "isSequential and batchCount are honoured."},
    "Switch": {"olcf": "switch", "status": "supported", "note": ""},
    "Until": {"olcf": "until", "status": "supported", "note": "timeout becomes the activity timeoutSeconds policy."},
    "Fail": {"olcf": "fail", "status": "supported", "note": ""},
}

UNSUPPORTED_HINTS: dict[str, str] = {
    "Lookup": "Use a sql activity (DuckDB over the lakehouse) and read activity('<name>').output.firstRow / .rows.",
    "GetMetadata": "Use a script or sql activity to list files / read table metadata.",
    "Filter": "Filter inside an expression (e.g. a sql activity or for_each with an if).",
    "Delete": "Use a script activity (python) that removes the files.",
    "Script": "Fabric Script activities run SQL against a warehouse connection; use a sql activity (DuckDB) or a spark_job/notebook activity.",
    "AzureFunction": "Use an http activity against the function URL (with a secret reference for the key).",
    "RefreshDataflow": "Dataflows Gen2 are a Fabric service and are not part of the open runtime.",
    "Office365Outlook": "Send e-mail through an alert channel or an http activity against your mail API.",
    "Teams": "Post to Teams with an http activity against an incoming webhook (store the URL as a secret).",
    "Validation": "Use an until activity polling with a sql/script activity.",
    "SqlServerStoredProcedure": "Not available; run the logic in a notebook or sql activity.",
    "PBISemanticModelRefresh": "Power BI semantic models are a Fabric service.",
}
UNSUPPORTED_PREFIX_HINTS = {"Databricks": "Databricks activities need a Databricks workspace; run the notebook on the OLCF compute fabric instead.",
                            "HDInsight": "HDInsight clusters are not part of the open runtime.", "Synapse": "Synapse artefacts are not part of the open runtime.",
                            "AzureML": "Azure ML activities are not part of the open runtime.", "ExecuteSSIS": "SSIS is not supported.", "Kusto": "KQL / Kusto activities are a Fabric service.",
                            "Custom": "Custom (Azure Batch) activities are not supported; use a script activity."}

_NAME_BAD = re.compile(r"[^A-Za-z0-9 _.\-]")
_DURATION = re.compile(r"^(?:(\d+)\.)?(\d{1,2}):(\d{2}):(\d{2})$")
_VAR_TYPES = {"string": "string", "boolean": "bool", "bool": "bool", "array": "array", "integer": "int", "int": "int", "float": "float", "object": "object"}
_PARAM_TYPES = {"string": "string", "int": "int", "integer": "int", "float": "float", "bool": "bool", "boolean": "bool", "array": "array", "object": "object", "securestring": "secret_ref"}
_FORMATS = {"delimitedtext": "csv", "parquet": "parquet", "json": "jsonl", "binary": "binary"}


def support_document() -> dict[str, Any]:
    """The machine-readable support table (written to ``schemas/fabric-import-support.json``)."""
    return {"$id": "https://olcf.dev/schemas/fabric-import-support.json", "title": "Fabric pipeline import support", "author": "Justin Dodrill", "target": M.SCHEMA_ID,
            "supported": SUPPORT, "unsupported_hints": {**UNSUPPORTED_HINTS, **{k + "*": v for k, v in UNSUPPORTED_PREFIX_HINTS.items()}},
            "on_unsupported": ["placeholder", "skip", "error"]}


class FabricImportError(Exception):  
    """A Fabric pipeline document cannot be imported; the message says why."""
    pass


class _Importer:
    def __init__(self, registry: Any, item_map: dict[str, str], on_unsupported: str) -> None:
        self.registry, self.item_map, self.on_unsupported = registry, {str(k).lower(): v for k, v in item_map.items()}, on_unsupported
        self.mapping: list[dict[str, Any]] = []
        self.unsupported: list[dict[str, Any]] = []
        self.warnings: list[dict[str, Any]] = []
        self.dropped_deps: list[dict[str, Any]] = []
        self.count = 0

    # -- reporting
    def warn(self, path: str, code: str, message: str) -> None:
        self.warnings.append({"path": path, "code": code, "message": message})

    def unsup(self, path: str, name: str, ftype: str, reason: str, remediation: str | None = None) -> None:
        self.unsupported.append({"path": path, "name": name, "type": ftype, "reason": reason, "remediation": remediation})

    # -- values
    def val(self, v: Any, path: str) -> Any:
        """Fabric ``{"value": "@...", "type": "Expression"}`` -> plain expression string; checks the expression syntax."""
        if isinstance(v, dict) and set(v) <= {"value", "type"} and "value" in v and str(v.get("type", "")).lower() in ("expression", ""):
            v = v["value"]
        if isinstance(v, dict):
            v = {k: self.val(x, f"{path}.{k}") for k, x in v.items()}
        elif isinstance(v, list):
            v = [self.val(x, f"{path}[{i}]") for i, x in enumerate(v)]
        else:
            try:
                analyze_value(v)
            except ExpressionError as exc:
                self.warn(path, "invalid_expression", f"the expression is not valid in OLCF: {exc.render()}")
        return v

    def seconds(self, v: Any, path: str) -> int | None:
        if v in (None, ""):
            return None
        v = self.val(v, path)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return int(v)
        if isinstance(v, str):
            m = _DURATION.match(v.strip())
            if m:
                d, h, mi, s = (int(x or 0) for x in m.groups())
                return d * 86400 + h * 3600 + mi * 60 + s
        self.warn(path, "unparsed_duration", f"cannot convert duration {v!r}; ignored")
        return None

    def item_ref(self, fabric_id: Any, fabric_name: Any, path: str, kind: str) -> str:
        for k in (fabric_id, fabric_name):
            if isinstance(k, str) and k.lower() in self.item_map:
                return self.item_map[k.lower()]
        ref = str(fabric_name or fabric_id or "")
        if not ref:
            self.warn(path, "missing_reference", f"the {kind} reference is missing")
        else:
            self.warn(path, "unresolved_reference", f"{kind} '{ref}' is a Fabric item; pass item_map {{'{ref}': '<OLCF item name or id>'}} or rename the OLCF item to match")
        return ref

    def name(self, raw: Any, path: str) -> str:
        n = str(raw or "")
        clean = _NAME_BAD.sub("_", n).strip()[:128]
        if clean != n:
            self.warn(path, "renamed", f"activity name '{n}' was changed to '{clean}' (allowed characters: letters, digits, space, _ . -)")
        return clean or "Activity"

    # -- activities
    def activities(self, acts: Any, path: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        removed: set[str] = set()
        for i, a in enumerate(acts or []):
            p = f"{path}[{i}]"
            if not isinstance(a, dict):
                self.warn(p, "invalid", "activity is not an object; skipped")
                continue
            self.count += 1
            if self.count > MAX_IMPORT_ACTIVITIES:
                raise FabricImportError(f"more than {MAX_IMPORT_ACTIVITIES} activities")
            res = self.activity(a, p)
            if res is None:
                removed.add(self.name(a.get("name"), p + ".name"))
                continue
            out.append(res)
        if removed:
            for a in out:
                before = a.get("dependsOn") or []
                kept = [d for d in before if d["activity"] not in removed]
                if len(kept) != len(before):
                    for d in before:
                        if d["activity"] in removed:
                            self.dropped_deps.append({"activity": a["name"], "dependency": d["activity"]})
                    a["dependsOn"] = kept
                    if not kept:
                        a.pop("dependsOn")
        return out

    def envelope(self, a: dict[str, Any], p: str) -> dict[str, Any]:
        out: dict[str, Any] = {"name": self.name(a.get("name"), p + ".name")}
        if a.get("description"):
            out["description"] = str(a["description"])[:1024]
        deps = []
        for d in a.get("dependsOn") or []:
            if isinstance(d, dict) and d.get("activity"):
                conds = [str(c).lower() for c in (d.get("dependencyConditions") or ["Succeeded"])]
                deps.append({"activity": self.name(d["activity"], p + ".dependsOn"), "conditions": sorted(set(conds), key=conds.index)})
        if deps:
            out["dependsOn"] = deps
        pol: dict[str, Any] = {}
        fp = a.get("policy") or {}
        if (t := self.seconds(fp.get("timeout"), p + ".policy.timeout")) and t > 0:
            pol["timeoutSeconds"] = t
        if isinstance(fp.get("retry"), int) and fp["retry"] > 0:
            pol["retries"] = min(int(fp["retry"]), 10)
        if isinstance(fp.get("retryIntervalInSeconds"), (int, float)) and fp["retryIntervalInSeconds"] > 0:
            pol["retryBackoffSeconds"] = float(fp["retryIntervalInSeconds"])
        for k in ("secureInput", "secureOutput"):
            if fp.get(k):
                pol[k] = True
        if pol:
            out["policy"] = pol
        up = a.get("userProperties")
        if isinstance(up, list) and up:
            out["userProperties"] = {str(x.get("name")): x.get("value") for x in up if isinstance(x, dict) and x.get("name")}
        return out

    def activity(self, a: dict[str, Any], p: str) -> dict[str, Any] | None:
        ftype = str(a.get("type") or "")
        env = self.envelope(a, p)
        tp = a.get("typeProperties") or {}
        fn = getattr(self, "_" + ftype, None) if ftype in SUPPORT else None
        if fn is None:
            return self.reject(env, ftype, p)
        try:
            props, extra_policy = fn(tp, p + ".typeProperties", env)
        except _Unsupported as exc:
            self.unsup(p, env["name"], ftype, exc.reason, exc.remediation)
            return self.reject(env, ftype, p, already=True, reason=exc.reason)
        olcf_type = SUPPORT[ftype]["olcf"]
        env["type"] = olcf_type
        env["typeProperties"] = props
        if extra_policy:
            env["policy"] = {**env.get("policy", {}), **extra_policy}
        self.mapping.append({"path": p, "name": env["name"], "fabric_type": ftype, "olcf_type": olcf_type})
        return env

    def reject(self, env: dict[str, Any], ftype: str, p: str, *, already: bool = False, reason: str | None = None) -> dict[str, Any] | None:
        hint = UNSUPPORTED_HINTS.get(ftype) or next((v for k, v in UNSUPPORTED_PREFIX_HINTS.items() if ftype.startswith(k)), None)
        reason = reason or f"activity type '{ftype}' is not supported by the import"
        if not already:
            self.unsup(p, env["name"], ftype, reason, hint)
        if self.on_unsupported == "error":
            raise FabricImportError(f"{p}: {reason}")
        if self.on_unsupported == "skip":
            return None
        msg = f"Imported from Microsoft Fabric: {reason}." + (f" {hint}" if hint else "")
        self.mapping.append({"path": p, "name": env["name"], "fabric_type": ftype, "olcf_type": "fail"})
        env["type"] = "fail"
        env["typeProperties"] = {"message": msg[:1000], "errorCode": "UnsupportedFabricActivity"}
        env.pop("policy", None)
        return env

    # each translator returns (typeProperties, extra policy)
    def _TridentNotebook(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        props: dict[str, Any] = {"notebook": self.item_ref(tp.get("notebookId"), tp.get("notebookName"), p + ".notebookId", "notebook")}
        if tp.get("workspaceId"):
            self.warn(p + ".workspaceId", "ignored", "the Fabric workspace id is ignored; the notebook is looked up in the pipeline's OLCF workspace")
        params = self._params(tp.get("parameters"), p + ".parameters")
        if params:
            props["parameters"] = params
        for k in ("sessionTag", "sparkPool", "conf"):
            if tp.get(k):
                self.warn(p + "." + k, "dropped", f"'{k}' has no OLCF equivalent here; configure it on the notebook or its environment")
        return props, {}

    def _SparkJobDefinition(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        props: dict[str, Any] = {"sparkJob": self.item_ref(tp.get("sparkJobDefinitionId"), tp.get("sparkJobDefinitionName"), p + ".sparkJobDefinitionId", "Spark job definition")}
        for k in ("commandLineArguments", "arguments", "conf", "configs", "executableFile", "mainClass"):
            if tp.get(k):
                self.warn(p + "." + k, "dropped", f"'{k}' is not imported; define it in the OLCF Spark job definition item")
        params = self._params(tp.get("parameters"), p + ".parameters")
        if params:
            props["parameters"] = params
        return props, {}

    def _params(self, raw: Any, p: str) -> dict[str, Any]:
        """Activity parameters ``{name: {"value": v, "type": t}}`` (or plain values) -> ``{name: v}``."""
        out: dict[str, Any] = {}
        for k, v in (raw or {}).items() if isinstance(raw, dict) else []:
            if isinstance(v, dict) and "value" in v and set(v) <= {"value", "type"}:
                v = v["value"]
            out[str(k)] = self.val(v, f"{p}.{k}")
        return out

    def _store(self, side: dict[str, Any], p: str, is_sink: bool) -> dict[str, Any]:
        ds = side.get("datasetSettings") or {}
        dtype = str(ds.get("type") or "")
        stype = str(side.get("type") or "")
        stores = side.get("storeSettings") or {}
        if "lakehouse" not in (str(stores.get("type") or "") + stype + dtype).lower() and not (ds.get("typeProperties") or {}).get("location", {}).get("type", "").lower().startswith("lakehouse"):
            raise _Unsupported(f"{'sink' if is_sink else 'source'} store '{stype or dtype or 'unknown'}' is not a lakehouse", "Only Lakehouse files / tables can be copied; use a script or notebook for other stores.")
        ext = ds.get("externalReferences") or {}
        conn = ext.get("connection")
        tpp = ds.get("typeProperties") or {}
        out: dict[str, Any] = {"lakehouse": self.item_ref(conn or tpp.get("workspaceId") or tpp.get("lakehouseId"), tpp.get("lakehouseName"), p + ".datasetSettings", "lakehouse")}
        loc = tpp.get("location") or {}
        if tpp.get("table") or "table" in dtype.lower() or "table" in stype.lower():
            tbl = tpp.get("table") or loc.get("table")
            schema = tpp.get("schema") or loc.get("schema")
            out["table"] = self.val(f"{schema}.{tbl}" if schema and "@" not in str(tbl) else tbl, p + ".table")
            if is_sink:
                out["mode"] = "overwrite" if str(side.get("tableActionOption") or "").lower() == "overwrite" else "append"
        else:
            folder = str(loc.get("folderPath") or "").strip("/")
            fname = str(loc.get("fileName") or "")
            if fname.startswith("@") or folder.startswith("@"):
                out["path"] = self.val("@concat('" + folder + "/', '" + fname + "')" if not folder.startswith("@") else folder, p + ".path")
            else:
                out["path"] = f"{folder}/{fname}".strip("/")
            fmt = _FORMATS.get(re.sub(r"(source|sink|location|dataset)$", "", dtype.lower()).strip())
            if fmt is None and dtype:
                raise _Unsupported(f"file format '{dtype}' is not supported", "Supported: DelimitedText (csv), Parquet, Json (json lines), Binary.")
            if fmt:
                out["format"] = fmt
            if not is_sink and stores.get("recursive") is not None:
                out["recursive"] = bool(stores["recursive"])
            if not is_sink and (stores.get("wildcardFileName") or stores.get("wildcardFolderPath")):
                out["pattern"] = str(stores.get("wildcardFileName") or "*")
                if stores.get("wildcardFolderPath"):
                    out["path"] = str(stores["wildcardFolderPath"]).strip("/")
        return out

    def _Copy(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        src, snk = tp.get("source"), tp.get("sink")
        if not isinstance(src, dict) or not isinstance(snk, dict):
            raise _Unsupported("the copy activity has no source / sink", None)
        props = {"source": self._store(src, p + ".source", False), "sink": self._store(snk, p + ".sink", True)}
        for k in ("translator", "enableStaging", "stagingSettings", "dataIntegrationUnits", "parallelCopies", "enableSkipIncompatibleRow", "logSettings"):
            if tp.get(k):
                self.warn(p + "." + k, "dropped", f"'{k}' is not imported" + ("; column mappings are not applied - check the schema of the result" if k == "translator" else ""))
        return props, {}

    def _WebActivity(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        url = tp.get("url")
        if not url:
            raise _Unsupported("connection-based web activity (relativeUrl): the base URL lives in a Fabric connection", "Use the full URL in an http activity.")
        props: dict[str, Any] = {"url": self.val(url, p + ".url"), "method": str(tp.get("method") or "GET").upper()}
        if isinstance(tp.get("headers"), dict) and tp["headers"]:
            props["headers"] = self.val(tp["headers"], p + ".headers")
        if "body" in tp and tp["body"] is not None:
            props["body"] = self.val(tp["body"], p + ".body")
        if (t := self.seconds(tp.get("httpRequestTimeout"), p + ".httpRequestTimeout")):
            props["timeoutSeconds"] = max(1, min(t, 600))
        auth = tp.get("authentication")
        if isinstance(auth, dict) and str(auth.get("type") or "anonymous").lower() not in ("anonymous", "none"):
            self.warn(p + ".authentication", "credentials_not_imported", f"authentication '{auth.get('type')}' was not imported; add auth {{type, secret: <secret id>}} referencing a secret")
        return props, {}

    def _WebHook(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        if not tp.get("url"):
            raise _Unsupported("webhook activity without url", None)
        props: dict[str, Any] = {"url": self.val(tp["url"], p + ".url"), "method": str(tp.get("method") or "POST").upper()}
        if props["method"] not in ("POST", "PUT", "PATCH", "GET"):
            raise _Unsupported(f"webhook method {props['method']} is not supported", None)
        if isinstance(tp.get("headers"), dict) and tp["headers"]:
            props["headers"] = self.val(tp["headers"], p + ".headers")
        if tp.get("body") is not None:
            props["body"] = self.val(tp["body"], p + ".body")
        t = self.seconds(tp.get("timeout"), p + ".timeout")
        if t:
            props["timeoutSeconds"] = t
        return props, {}

    def _ExecutePipeline(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        ref = tp.get("pipeline") if isinstance(tp.get("pipeline"), dict) else {}
        props: dict[str, Any] = {"pipeline": self.item_ref(tp.get("pipelineId") or ref.get("id"), ref.get("referenceName") or tp.get("pipelineName"), p + ".pipeline", "pipeline")}
        params = self._params(tp.get("parameters"), p + ".parameters")
        if params:
            props["parameters"] = params
        props["waitOnCompletion"] = bool(tp.get("waitOnCompletion", True))
        if not props["waitOnCompletion"]:
            self.warn(p + ".waitOnCompletion", "unsupported_value", "waitOnCompletion=false is not supported; the parent waits for the child")
            props["waitOnCompletion"] = True
        return props, {}

    def _Wait(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        return {"waitTimeInSeconds": self.val(tp.get("waitTimeInSeconds", 0), p + ".waitTimeInSeconds")}, {}

    def _SetVariable(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        if tp.get("setSystemVariable") or not tp.get("variableName"):
            raise _Unsupported("setting the pipeline return value (setSystemVariable) is not supported", "Use the pipeline 'outputs' section instead.")
        return {"variable": str(tp["variableName"]), "value": self.val(tp.get("value"), p + ".value")}, {}

    def _AppendVariable(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        return {"variable": str(tp.get("variableName") or ""), "value": self.val(tp.get("value"), p + ".value"), "mode": "append"}, {}

    def _IfCondition(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        props: dict[str, Any] = {"expression": self.val(tp.get("expression"), p + ".expression")}
        for k in ("ifTrueActivities", "ifFalseActivities"):
            if tp.get(k):
                props[k] = self.activities(tp[k], f"{p}.{k}")
        return props, {}

    def _ForEach(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        props: dict[str, Any] = {"items": self.val(tp.get("items"), p + ".items"), "activities": self.activities(tp.get("activities"), p + ".activities")}
        if tp.get("isSequential") is not None:
            props["isSequential"] = bool(tp["isSequential"])
        if isinstance(tp.get("batchCount"), int):
            props["batchCount"] = max(1, min(int(tp["batchCount"]), 50))
        return props, {}

    def _Switch(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        cases = []
        for i, c in enumerate(tp.get("cases") or []):
            cases.append({"value": self.val(c.get("value"), f"{p}.cases[{i}].value"), "activities": self.activities(c.get("activities"), f"{p}.cases[{i}].activities")})
        props: dict[str, Any] = {"on": self.val(tp.get("on"), p + ".on"), "cases": cases}
        if tp.get("defaultActivities"):
            props["defaultActivities"] = self.activities(tp["defaultActivities"], p + ".defaultActivities")
        return props, {}

    def _Until(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        props = {"expression": self.val(tp.get("expression"), p + ".expression"), "activities": self.activities(tp.get("activities"), p + ".activities")}
        t = self.seconds(tp.get("timeout"), p + ".timeout")
        return props, ({"timeoutSeconds": t} if t else {})

    def _Fail(self, tp: dict[str, Any], p: str, env: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:  # noqa: N802
        props = {"message": self.val(tp.get("message", "pipeline failed"), p + ".message")}
        if tp.get("errorCode") is not None:
            props["errorCode"] = self.val(str(tp["errorCode"]) if not isinstance(tp["errorCode"], (dict, list)) else tp["errorCode"], p + ".errorCode")
        return props, {}


class _Unsupported(Exception):
    def __init__(self, reason: str, remediation: str | None) -> None:
        super().__init__(reason)
        self.reason, self.remediation = reason, remediation


def import_fabric_pipeline(doc: dict[str, Any], registry: Any, *, item_map: dict[str, str] | None = None, on_unsupported: str = PLACEHOLDER, name: str | None = None) -> dict[str, Any]:
    """Translate a Fabric pipeline document. Returns ``{"name", "definition", "report", "validation"}``; raises :class:`FabricImportError` only for ``on_unsupported='error'`` / oversize input."""
    if on_unsupported not in ("placeholder", "skip", "error"):
        raise ValueError("on_unsupported must be placeholder, skip or error")
    props = doc.get("properties") if isinstance(doc.get("properties"), dict) else doc
    if not isinstance(props, dict) or not isinstance(props.get("activities"), list):
        raise ValueError("not a Fabric pipeline document: expected {\"properties\": {\"activities\": [...]}} or {\"activities\": [...]}")
    imp = _Importer(registry, item_map or {}, on_unsupported)
    acts = imp.activities(props["activities"], "properties.activities" if props is not doc else "activities")
    definition: dict[str, Any] = {"schema": M.SCHEMA_ID, "activities": acts}
    if props.get("description") or doc.get("description"):
        definition["description"] = str(props.get("description") or doc.get("description"))[:4096]
    params: dict[str, Any] = {}
    for k, v in (props.get("parameters") or {}).items():
        t = _PARAM_TYPES.get(str((v or {}).get("type", "string")).lower())
        if t is None:
            imp.warn(f"parameters.{k}", "unknown_type", f"parameter type '{(v or {}).get('type')}' is unknown; imported as string")
            t = "string"
        decl: dict[str, Any] = {"type": t}
        if t == "secret_ref":
            if (v or {}).get("defaultValue"):
                imp.warn(f"parameters.{k}", "secret_default_dropped", "the default of a secure string is not imported; supply a secret id at run time")
        elif (v or {}).get("defaultValue") is not None:
            decl["default"] = v["defaultValue"]
        params[str(k)] = decl
    if params:
        definition["parameters"] = params
    variables: dict[str, Any] = {}
    for k, v in (props.get("variables") or {}).items():
        t = _VAR_TYPES.get(str((v or {}).get("type", "string")).lower())
        if t is None:
            imp.warn(f"variables.{k}", "unknown_type", f"variable type '{(v or {}).get('type')}' is unknown; imported as string")
            t = "string"
        decl = {"type": t}
        if (v or {}).get("defaultValue") is not None:
            decl["default"] = v["defaultValue"]
        variables[str(k)] = decl
    if variables:
        definition["variables"] = variables
    if props.get("annotations"):
        definition["annotations"] = [str(a) for a in props["annotations"]][:50]
    validation = M.validate_definition(definition, registry)
    unresolved = [w for w in imp.warnings if w["code"] == "unresolved_reference"]
    report = {
        "source": {"activities": imp.count, "name": name or doc.get("name")},
        "imported": len(imp.mapping) - sum(1 for m in imp.mapping if m["olcf_type"] == "fail" and any(u["path"] == m["path"] for u in imp.unsupported)),
        "unsupported": imp.unsupported, "warnings": imp.warnings, "mapping": imp.mapping, "dropped_dependencies": imp.dropped_deps,
        "unresolved_references": [w["path"] for w in unresolved],
        "complete": not imp.unsupported and not imp.warnings,
        "runnable": validation.ok and not unresolved and not imp.unsupported,
        "on_unsupported": on_unsupported,
        "supported_subset": "schemas/fabric-import-support.json",
    }
    return {"name": name or doc.get("name"), "definition": validation.definition if validation.ok else definition, "report": report, "validation": validation.to_dict()}
