# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""SparkSession factory with OLCF defaults and **default-lakehouse semantics** (CONTRACTS sections 4.3, 12).

:func:`build_spark_conf` is a pure function (unit-testable without a JVM) that assembles the Spark configuration:

1. runtime defaults from the installed manifest (``spark_conf_defaults``),
2. master / app name / driver networking (``driver_host``, ``driver_port``, ``block_manager_port``),
3. jars: jars of the runtime lock that are *not* already on the Spark class path (``spark.jars``) + ``spark.jars`` /
   ``packages`` requested by the job,
4. **Delta Lake** (``DeltaSparkSessionExtension`` + ``DeltaCatalog`` + ``spark.sql.sources.default=delta``) when the
   runtime lock contains the Delta jars,
5. storage settings from ``storage_conf.spark`` (keys without ``spark.`` prefix are Hadoop options and get
   ``spark.hadoop.``),
6. job/controller conf and finally user overrides (``%%configure``), minus protected keys.

:func:`create_session` builds the session and then applies the lakehouse semantics Fabric users expect:
``warehouse.dir`` = the default lakehouse ``Tables`` location, a database named after the lakehouse mapped to that
location and made current, Delta tables found under ``Tables/`` registered as external tables (so ``spark.table("x")``,
``spark.sql("select * from x")`` and ``saveAsTable("x")`` just work), and relative ``Files/..`` / ``Tables/..`` paths
resolving against the default lakehouse (via the Hadoop filesystem working directory).
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from olcf.runtime_lib import paths
from olcf.runtime_lib.bootstrap import emit_event

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
ICEBERG_EXTENSION = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

#: Keys user code / ``%%configure`` may never change: they would break placement, networking or security.
PROTECTED_CONF = (
    "spark.master", "spark.submit.deployMode", "spark.driver.host", "spark.driver.bindAddress", "spark.driver.port",
    "spark.blockManager.port", "spark.driver.blockManager.port", "spark.authenticate", "spark.authenticate.*",
    "spark.network.crypto.*", "spark.ssl.*", "spark.jars.packages", "spark.jars.repositories", "spark.jars.ivy", "spark.jars.ivySettings",
)


def no_spark_error(spec: dict[str, Any]) -> Exception:
    """The error for code that asks for ``spark`` where no PySpark exists: a job on the host-Python runtime (the common first-run case) gets the explicit explanation."""
    from olcf.core.errors import Unsupported  # noqa: PLC0415
    from olcf.core.hostruntime import HOST_RUNTIME_ID, NO_SPARK_MESSAGE, NO_SPARK_REMEDIATION  # noqa: PLC0415

    rt = (spec.get("runtime") or {}).get("id")
    if rt == HOST_RUNTIME_ID:
        return Unsupported(f"{NO_SPARK_MESSAGE}. To fix: {NO_SPARK_REMEDIATION}")
    return Unsupported(f"PySpark is not installed in the Python environment of this job (runtime {rt or 'unknown'}). To fix: {NO_SPARK_REMEDIATION}")


def runtime_dir() -> Path | None:
    """The runtime directory this interpreter belongs to (``OLCF_RUNTIME_DIR`` or ``<venv>/..`` containing ``lock.json``)."""
    env = os.environ.get("OLCF_RUNTIME_DIR")
    if env and (Path(env) / "lock.json").is_file():
        return Path(env)
    cand = Path(sys.prefix).resolve().parent
    return cand if (cand / "lock.json").is_file() else None


def load_runtime_files() -> tuple[dict[str, Any], dict[str, Any]]:
    """``(lock, manifest)`` of the current runtime; empty dicts when running outside a managed runtime (e.g. tests)."""
    d = runtime_dir()
    if d is None:
        return {}, {}
    try:
        return json.loads((d / "lock.json").read_text()), json.loads((d / "manifest.json").read_text())
    except (OSError, ValueError):
        return {}, {}


def has_delta(lock: dict[str, Any]) -> bool:
    return any("delta-spark" in str(j.get("coordinate", "")) or str(j.get("file", "")).startswith("delta-spark") for j in lock.get("jars", []))


def has_iceberg(lock: dict[str, Any]) -> bool:
    return any("iceberg-spark-runtime" in str(j.get("coordinate", "")) for j in lock.get("jars", []))


def is_protected(key: str) -> bool:
    return any(fnmatch.fnmatch(key, pat) for pat in PROTECTED_CONF)


def _hadoop_key(key: str) -> str:
    return key if key.startswith("spark.") else "spark.hadoop." + key


def _csv_merge(*values: str | None) -> str:
    seen: list[str] = []
    for v in values:
        for item in (v or "").split(","):
            item = item.strip()
            if item and item not in seen:
                seen.append(item)
    return ",".join(seen)


def build_spark_conf(spec: dict[str, Any], *, lock: dict[str, Any] | None = None, manifest: dict[str, Any] | None = None,
                     overrides: dict[str, Any] | None = None, runtime_root: Path | None = None) -> dict[str, str]:
    """Assemble the final Spark configuration for ``spec`` (see module docstring for the layering)."""
    lock = lock if lock is not None else {}
    manifest = manifest if manifest is not None else {}
    sp = spec.get("spark") or {}
    conf: dict[str, str] = {}
    # 1. runtime defaults
    for k, v in (manifest.get("spark_conf_defaults") or {}).items():
        conf[str(k)] = _s(v)
    # 2. master / name / driver network
    master = sp.get("master") or "local[*]"
    conf["spark.master"] = master
    conf["spark.app.name"] = sp.get("app_name") or spec.get("name") or spec.get("job_id") or "olcf"
    if not master.startswith("local"):
        if sp.get("driver_host"):
            conf["spark.driver.host"] = str(sp["driver_host"])
            conf["spark.driver.bindAddress"] = str(sp.get("bind_address") or sp["driver_host"])
        if sp.get("driver_port"):
            conf["spark.driver.port"] = str(sp["driver_port"])
        if sp.get("block_manager_port"):
            conf["spark.blockManager.port"] = str(sp["block_manager_port"])
            conf["spark.driver.blockManager.port"] = str(sp["block_manager_port"])
        conf.setdefault("spark.port.maxRetries", "32")
    # 3. jars not already on the Spark class path
    runtime_jars: list[str] = []
    if runtime_root is not None:
        for j in lock.get("jars", []):
            if j.get("linked") in ("symlink", "copy", "existing"):
                continue  # visible to driver and executors through $SPARK_HOME/jars
            runtime_jars.append((runtime_root / "jars" / j["file"]).resolve().as_uri())
    extra_jars = [_file_uri(j) for j in (sp.get("jars") or [])]
    if runtime_jars or extra_jars:
        conf["spark.jars"] = _csv_merge(conf.get("spark.jars"), ",".join(runtime_jars), ",".join(extra_jars))
    if sp.get("packages"):
        conf["spark.jars.packages"] = _csv_merge(conf.get("spark.jars.packages"), ",".join(sp["packages"]))
    # 4. Delta / Iceberg
    exts: list[str] = []
    if has_delta(lock):
        exts.append(DELTA_EXTENSION)
        conf["spark.sql.catalog.spark_catalog"] = DELTA_CATALOG
        conf["spark.sql.sources.default"] = "delta"
    if has_iceberg(lock):
        exts.append(ICEBERG_EXTENSION)
    if exts:
        conf["spark.sql.extensions"] = _csv_merge(conf.get("spark.sql.extensions"), ",".join(exts))
    # 5. storage settings (secrets live here: never logged)
    for k, v in ((spec.get("storage_conf") or {}).get("spark") or {}).items():
        conf[_hadoop_key(str(k))] = _s(v)
    # default lakehouse -> warehouse dir
    dl = spec.get("default_lakehouse")
    if dl and dl.get("tables_uri"):
        conf["spark.sql.warehouse.dir"] = str(dl["tables_uri"])
    # 6. job/controller conf, then overrides (protected keys are dropped from the latter)
    for k, v in (sp.get("conf") or {}).items():
        conf[str(k)] = _s(v)
    # delta settings may not be undone by controller conf accidentally clearing extensions: merge, do not replace
    if has_delta(lock):
        conf["spark.sql.extensions"] = _csv_merge(conf.get("spark.sql.extensions"), DELTA_EXTENSION)
    for k, v in (overrides or {}).items():
        if is_protected(str(k)):
            raise PermissionError(f"Spark setting {k} is managed by the platform and cannot be changed from user code")
        conf[str(k)] = _s(v)
    return conf


def _s(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _file_uri(p: str) -> str:
    return p if "://" in p else Path(p).resolve().as_uri()


def redacted_conf(conf: dict[str, str]) -> dict[str, str]:
    """Conf safe to print: values of secret-looking keys are masked."""
    from olcf.core.redaction import REDACTED  # noqa: PLC0415

    sens = re.compile(r"(?i)(secret|password|token|key|credential|sas|auth)")
    return {k: (REDACTED if sens.search(k) else v) for k, v in conf.items()}


# --------------------------------------------------------------------------------------------- session
def create_session(spec: dict[str, Any], overrides: dict[str, Any] | None = None) -> Any:
    """Create the SparkSession for ``spec`` and apply the default-lakehouse semantics. Emits ``spark_app_started``."""
    try:
        from pyspark.sql import SparkSession  # noqa: PLC0415
    except ModuleNotFoundError as exc:
        if exc.name not in ("pyspark", "pyspark.sql"):
            raise
        raise no_spark_error(spec) from None

    paths.configure(spec)
    lock, manifest = load_runtime_files()
    root = runtime_dir()
    conf = build_spark_conf(spec, lock=lock, manifest=manifest, overrides=overrides, runtime_root=root)
    dl = spec.get("default_lakehouse")
    relative_paths = conf.pop("olcf.relativePaths", "true").lower() != "false"
    if dl and relative_paths and "://" in str(dl.get("root_uri", "")) and not str(dl["root_uri"]).startswith("file:"):
        # remote lakehouse: make its filesystem the default one so relative Files/.. resolve there
        from urllib.parse import urlparse  # noqa: PLC0415

        u = urlparse(str(dl["root_uri"]))
        conf.setdefault("spark.hadoop.fs.defaultFS", f"{u.scheme}://{u.netloc}")
    builder = SparkSession.builder
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("OLCF_SPARK_LOG_LEVEL", "WARN"))
    info = session_info(spark)
    job_dir = os.environ.get("OLCF_JOB_DIR")
    if job_dir:  # the runner reads this for result.json (the kernel is a separate process)
        try:
            (Path(job_dir) / "spark-app.json").write_text(json.dumps(info))
        except OSError:
            pass
    emit_event("spark_app_started", **info)
    if dl:
        try:
            summary = setup_lakehouses(spark, spec, relative_paths=relative_paths, delta=has_delta(lock))
            emit_event("lakehouse_ready", **summary)
        except Exception:
            spark.stop()
            raise
    _start_lineage_probe(spec)  # after the lakehouse set-up: its own CREATE DATABASE / CREATE TABLE statements are not user lineage
    return spark


def _start_lineage_probe(spec: dict[str, Any]) -> None:
    """Hook the PySpark classes so the reads, writes and SQL statements of user code are reported (:mod:`olcf.runtime_lib.lineage_probe`); never raises."""
    try:
        from olcf.runtime_lib import lineage_probe  # noqa: PLC0415

        if lineage_probe.install(spec) is not None:
            lineage_probe.install_spark()
    except Exception:  # noqa: BLE001 - observability must not fail session creation
        pass


def session_info(spark: Any) -> dict[str, Any]:
    sc = spark.sparkContext
    return {"application_id": sc.applicationId, "ui_url": sc.uiWebUrl, "app_name": sc.appName, "master": sc.master}


def _ident(name: str) -> str:
    """Valid Spark database/table identifier fragment for a lakehouse/table name (backtick-quoted by callers)."""
    return name.replace("`", "``")


def db_name_for(lakehouse: dict[str, Any]) -> str:
    return re.sub(r"[^0-9A-Za-z_]", "_", str(lakehouse.get("name") or "lakehouse"))


def setup_lakehouses(spark: Any, spec: dict[str, Any], *, relative_paths: bool = True, delta: bool = False) -> dict[str, Any]:
    """Create/use the default lakehouse database, register tables, set the relative-path working directory."""
    default = spec.get("default_lakehouse")
    summary: dict[str, Any] = {"default": None, "registered": {}, "warnings": []}
    for lh in [*([default] if default else []), *(spec.get("lakehouses") or [])]:
        is_default = lh is default
        try:
            registered = _register_lakehouse(spark, lh, delta=delta, summary=summary)
        except Exception as exc:  # noqa: BLE001
            if is_default:
                raise
            summary["warnings"].append(f"lakehouse {lh.get('name')}: {exc}")
            continue
        summary["registered"][db_name_for(lh)] = registered
    if default:
        spark.sql(f"USE `{_ident(db_name_for(default))}`")
        summary["default"] = db_name_for(default)
        if relative_paths and default.get("root_uri"):
            _set_working_directory(spark, str(default["root_uri"]))
    return summary


def _fs(spark: Any, uri: str) -> tuple[Any, Any]:
    jvm = spark._jvm  # noqa: SLF001
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path  # noqa: SLF001


def _set_working_directory(spark: Any, root_uri: str) -> None:
    """Relative paths are qualified against the default FileSystem's working directory: point it at the lakehouse root."""
    fs, path = _fs(spark, root_uri)
    fs.setWorkingDirectory(path)


def _register_lakehouse(spark: Any, lh: dict[str, Any], *, delta: bool, summary: dict[str, Any]) -> list[str]:
    db = db_name_for(lh)
    tables_uri = str(lh["tables_uri"])
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{_ident(db)}` LOCATION '{_sql_str(tables_uri)}'")
    fmt = str(lh.get("format") or "delta").lower()
    if fmt == "delta" and not delta:
        summary["warnings"].append(f"lakehouse {lh.get('name')}: Delta jars are not installed in this runtime; Delta tables under Tables/ were not registered")
        return []
    fs, tpath = _fs(spark, tables_uri)
    if not fs.exists(tpath):
        return []
    jvm = spark._jvm  # noqa: SLF001
    registered: list[str] = []
    schema_enabled = bool(lh.get("schema_enabled"))
    for st in fs.listStatus(tpath):
        if not st.isDirectory():
            continue
        name = st.getPath().getName()
        if name.startswith(("_", ".")):
            continue
        if _is_table_dir(fs, jvm, st.getPath(), fmt):
            _create_external(spark, db, name, st.getPath().toString(), fmt)
            registered.append(name)
        elif schema_enabled:
            schema_db = re.sub(r"[^0-9A-Za-z_]", "_", name)
            spark.sql(f"CREATE DATABASE IF NOT EXISTS `{_ident(schema_db)}` LOCATION '{_sql_str(st.getPath().toString())}'")
            for st2 in fs.listStatus(st.getPath()):
                if st2.isDirectory() and not st2.getPath().getName().startswith(("_", ".")) and _is_table_dir(fs, jvm, st2.getPath(), fmt):
                    _create_external(spark, schema_db, st2.getPath().getName(), st2.getPath().toString(), fmt)
                    registered.append(f"{schema_db}.{st2.getPath().getName()}")
    return registered


def _is_table_dir(fs: Any, jvm: Any, path: Any, fmt: str) -> bool:
    if fmt == "delta":
        return bool(fs.exists(jvm.org.apache.hadoop.fs.Path(path, "_delta_log")))
    # non-delta lakehouse (parquet/orc/csv/json): a directory that directly contains data files
    for st in fs.listStatus(path):
        if st.isFile() and not st.getPath().getName().startswith(("_", ".")):
            return True
    return False


def _create_external(spark: Any, db: str, table: str, location: str, fmt: str) -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS `{_ident(db)}`.`{_ident(table)}` USING {fmt} LOCATION '{_sql_str(location)}'")


def _sql_str(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")
