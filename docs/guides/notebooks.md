# Notebooks, environments and Spark jobs

This guide covers authoring and running code: notebooks, interactive sessions, parameters, dependency environments, variable sets and Spark job definitions. How the job runs on the cluster is in the [compute model](../concepts/compute-model.md); pipelines that run notebooks are in the [pipelines guide](pipelines.md).

## Notebooks

A notebook is an item of type `notebook`: an nbformat 4 document plus settings. In the UI it is under **Develop, Workspaces**, then the workspace, then a notebook; the editor has cells, a language per cell, run controls, a Spark progress panel, a version history and a run history.

**Languages.** Python (PySpark) is the default. A cell starting with `%%sql` runs Spark SQL (so it needs a Spark runtime: see [Running without Spark](#running-without-spark)). Scala, R and C# cells can be stored but not run: they are kept on import, marked with a warning, and refused at execution time instead of being silently dropped.

**Magics** available in OLCF kernels (interactive and batch):

| Magic | Behaviour |
|---|---|
| `%%sql [-o VAR] [--limit N]` | runs one or more Spark SQL statements; the last result is displayed and stored in `VAR` |
| `%%pyspark` | runs the cell as Python (the default) |
| `%%configure [-f]` + JSON | Spark settings applied before the session starts (`conf`, `driverMemory`, `driverCores`, `executorMemory`, `executorCores`, `numExecutors`, `jars`, `pyFiles`, `files`, `archives`, `name`, `defaultLakehouse`, `queue`). If the session already exists the cell fails unless `-f` is given, which restarts the session. Platform-managed keys (master, driver networking, authentication, `spark.jars.packages`) and unknown keys are rejected. Fabric's pool selectors are accepted and ignored |
| `%run <notebook> [{"param": value}]` | runs another notebook inline in this session; only notebooks that were handed to the job are found |
| `%pip install ...` | does not install by default; see [Python packages](#python-packages) |

**Utilities.** `notebookutils` and `mssparkutils` are available (`import notebookutils`, `from notebookutils import mssparkutils`). Implemented: `fs` (ls, mkdirs, cp, fastcp, mv, rm, head, put, append, exists), `notebook` (exit, run, runMultiple, as child jobs), `credentials` (getSecret for granted secrets, getToken for storage and OneLake audiences), `lakehouse` (get, list), `session.stop`, `env` (workspace, user and job names and ids) and `runtime.context`. Everything else raises a `NotSupportedError` with remediation instead of doing nothing. This is an independent, API-compatible subset, not Microsoft software. The [Fabric assumptions](fabric-assumptions.md) page lists what is and is not covered.

**Paths.** `Files/...` and `Tables/...` resolve against the notebook's default lakehouse; `olcf://<workspace>/<lakehouse>/Files/...` addresses any lakehouse you may use ([lakehouses](../concepts/workspaces-items-lakehouses.md#lakehouses-and-storage)).

### Engines, runtimes and resources

A notebook's settings choose `engine` (`spark` or `python`), the runtime family or exact runtime, the default lakehouse, the environment, a timeout, resources and a policy. A `python` job runs a plain interpreter without a Spark session and needs no Spark ports (and, when no installed runtime fits, on the host Python runtime: see below). A `spark` job gets a Spark session on a Spark Standalone cluster when a node can host one, and otherwise runs in local mode on a single node; the provider can be forced ([compute model](../concepts/compute-model.md)). Anything you can set on the notebook can be overridden per run.

### Running without Spark

A notebook whose `engine` is `python` does not need a Spark runtime. When no installed runtime fits (a fresh installation has none), a Python-engine notebook, job or session runs on the **worker's own Python interpreter**, the built-in runtime `olcf-host-python` (shown as "Host Python (no Spark)"; `olcf runtime list` prints `no Spark`). The job event `runtime_fallback` says so, and the job page in the console shows the banner "Running on the host Python runtime". The rules, the fingerprint recorded with every run and the limits are in the [compute model](../concepts/compute-model.md#the-host-python-runtime-no-spark).

What that runtime has is what OLCF itself uses: `deltalake`, `duckdb`, `pandas`, `pyarrow`, `numpy`, `ipykernel`, `nbformat`, `jupyter_client`. `ipykernel` and `pandas` are ordinary dependencies of the `olcf` package, so any installation can run such a notebook. What it does not have is `spark`, `sc`, `%%sql`, PySpark and the Delta-on-Spark jars. A cell that uses them fails with a message that says the runtime has no Spark and how to fix it (install a Spark runtime with `olcf runtime install`, set the notebook engine to `spark`, or rewrite the cell with DuckDB, `deltalake` or pandas); the job is classified as a `compatibility` failure.

**The sample notebooks.**

| Notebook | Engine | Needs |
|---|---|---|
| `01_Retail_Medallion_ETL` | `spark` | a Spark runtime. Without one the job waits in the queue with the reason `no_runtime` (`olcf job status <id>` prints `waiting: [no_runtime] runtime 'olcf-rt-2.0.0' is not installed on any worker` and the command that fixes it, and the console shows the waiting banner); it starts by itself once `olcf runtime install` has finished |
| `02_Retail_Explore_Python` | `python` | only `deltalake`, `duckdb` and `pandas`. It runs without Spark and without matplotlib |

**Charts need matplotlib.** Notebook 02 plots with matplotlib, which is *optional*. The base installation does not include it. Without it the first cell prints `matplotlib is not installed here, so charts are skipped and tables are shown instead` plus how to add it, and every cell still shows its table. To get the charts, install the `plots` extra into the OLCF installation (`pip install "olcf[plots]"`, `uv tool install "olcf[plots]"`, the installer's `--extras plots`, or `plots` added to `OLCF_EXTRAS` for the container build, whose default is `postgres,s3,k8s`; the `all` extra includes it), or put `matplotlib` into the notebook's [Environment](#environments), which builds it into a dependency layer on top of the host runtime (publishing an environment needs a package index or wheelhouse). When matplotlib is present, a batch run starts its kernel with the inline backend, so each figure is saved as an `image/png` output of the executed notebook (`olcf job output <id>`); script jobs use the `Agg` backend. An installation made earlier keeps the old notebook text until `olcf sample install --reinstall`.

### Interactive sessions

Starting a session from the notebook toolbar (`POST /v1/sessions`) places a kernel on a worker; the kernel lives on that node and survives browser reloads. A session is released after `compute.session_idle_timeout_seconds` (default 3600) of inactivity, and one user can hold `compute.max_sessions_per_user` (default 5). Interrupt and restart are in the toolbar; output of the last 500 messages is replayed when you reconnect.

### Parameters and runs

Tag one code cell `parameters`. Every top-level assignment of a literal (`top_n = 10`, `name: str = "x"`) becomes a typed parameter. A run with parameters inserts a cell tagged `injected-parameters` right after the parameter cell; values are rendered by a closed encoder that supports only strings, numbers, booleans, null, lists and dictionaries, so a parameter value can never become code. Values from the API, a schedule or a pipeline are coerced to the detected type (`"5"` to `5` for an integer).

```bash
olcf notebook parameters 02_Retail_Explore_Python -w "Sample Retail"
olcf notebook run 02_Retail_Explore_Python -w "Sample Retail" -p top_n=5 --follow
olcf notebook runs 02_Retail_Explore_Python -w "Sample Retail"
olcf job output <job-id> -o executed.ipynb
```

A run creates a [job](../concepts/compute-model.md#job-lifecycle) whose output is the executed notebook. Runs carry an idempotency key option, are retried as the job policy says, and appear in **Operate, Jobs**.

### Import, export and history

`olcf notebook import` reads `.ipynb`, `.py` and Fabric Git source files (with `--on-conflict skip|overwrite|rename|fail`, `--folder` and `--strip-outputs`); `olcf notebook export -f ipynb|py|fabric_git_source` writes them. The `py` format is percent format (`# %%` cell markers, magics written as `# MAGIC`). Serialisation is deterministic, so a notebook committed to Git produces line-level diffs. Every save creates a version; the notebook diff compares cells (matched by id, then content, then similarity) and ignores outputs and execution counts; a version can be restored. The Git integration for a whole workspace is in [workspaces](../concepts/workspaces-items-lakehouses.md#workspaces).

## Python packages

Packages come from the runtime and from the notebook's **environment**. Installing at notebook time is controlled by a policy with three levels:

| `pip_policy` | `%pip install` behaviour |
|---|---|
| `block` | refused |
| `declared` (default) | succeeds only when every requirement is already satisfied by the runtime or the environment; anything else fails with category `dependency` |
| `allow` | installs into a per-job layer directory that goes on `sys.path` |

The default and the most permissive level any notebook may select are platform settings (`environments.pip_policy_default`, `environments.pip_policy_max`). Public package indexes can be denied entirely or restricted to approved prefixes and local wheelhouses with `security.allow_public_package_install` and `security.allowed_package_indexes`; an environment setting can tighten these but never loosen them.

## Environments

An `environment` item declares a runtime choice plus:

* Python requirements (`pandas>=2,<3`), index settings (`index_url`, `extra_index_urls`, `find_links`, `no_index`), target platforms and whether source distributions are allowed;
* jars: Maven coordinates and files with a SHA-256;
* environment variables and Spark settings.

**Publishing** an environment resolves the requirements with the real `pip` resolver, downloads every artifact, and stores an immutable lock (`olcf.envlock/1`) containing exact versions and SHA-256 hashes, the index used (never credentials) and the environment settings. The lock hash is the SHA-256 of its canonical JSON, so identical inputs give an identical hash. Each publication is a numbered version; a job pinned to an environment installs exactly that lock with `pip install --require-hashes --no-deps`.

On the worker the lock becomes a **layer**: installed once per node under a key derived from the pins, hashes and Python ABI, shared read-only by every job with the same dependencies, and put in front of the runtime's site-packages. Different environments with identical dependencies share one layer. Layers are built from the index or wheelhouse named in the lock; nothing else touches the network.

Limits you should know: environment markers are evaluated on the machine that resolves, not on the target platform; source distributions are accepted only for the resolving host's platform and ABI and only when the environment allows them; hash locking proves integrity, not safety of a package.

API: `POST /v1/environments/{id}/publish`, `.../resolve`, `.../verify`, `.../lock`, `.../publications`, `.../diff`, and `.../requirements/from-notebook` (derive requirements from a notebook's imports).

## Variable sets

A `variable_set` item holds typed variables (`string`, `integer`, `number`, `boolean`, `array`, `object`, `secret`) with a default and per-stage overrides:

```json
{"stages": ["dev", "test", "prod"], "default_stage": "dev",
 "variables": [
   {"name": "storage_container", "type": "string", "default": "raw-dev", "overrides": {"prod": "raw"}},
   {"name": "api_key", "type": "secret", "default": "sec_01...", "overrides": {"prod": "sec_01..."}}]}
```

A `secret` variable holds a secret id, never a value, and the validator rejects anything else; non-secret variables whose name or value looks like a credential are rejected too. The stage used is the workspace's active stage (set on the workspace or by a deployment), then the set's `default_stage`. `${var:NAME}` placeholders in strings are expanded; a string that is exactly one placeholder keeps the variable's type. Jobs receive secrets as references that the controller resolves at launch.

## Spark job definitions

A `spark_job_definition` item describes an application rather than a notebook:

```json
{"language": "python",
 "main_file": "Files/jobs/etl.py",
 "args": ["--date", "2026-01-31"],
 "reference_files": ["Files/libs/helpers.py"],
 "settings": {"runtime": {"family": "fabric-2.0-compatible"},
              "default_lakehouse": {"name": "retail_lakehouse"},
              "policy": {"retries": 2, "timeout_seconds": 3600}}}
```

`language` is `python`, `scala` or `java`. Python takes `main_file` or inline `code`; Scala and Java take `main_class` and a jar as `main_file`. A Scala or Java **application** (a jar) runs; Scala, R and C# **notebook cells** do not execute (they are kept and reported as warnings).

**Entry point and arguments.** `main_class` and `args` are part of the job's source, not notebook parameters: a job submitted with `source: {"format": "jar", "artifact_uri": "...", "main_class": "com.example.Main", "args": ["--date", "2026-01-31"]}` (REST `JobSource.main_class`/`args`, SDK `JobSource(main_class=..., args=[...])`), or run from a Spark job definition item (which supplies the two fields from its definition), ends up as `spec.source.main_class` / `spec.source.args` and every provider reads them from there (`spark-submit`, Kubernetes `spark_submit` mode, Livy batch `className`/`args`). Entries of `args` are converted to strings. A jar job without `main_class` is refused (`422`, with the remediation to set it); `parameters.main_class` and `parameters.args` are no longer an entry-point channel, so a client that passed them to a Livy batch must move them. With `item_id` the definition's values are used; with an inline `source` the request's. The Livy batch path is covered by protocol tests. `POST /v1/spark-job-definitions/{id}/run` (or a run from the UI, a schedule or a pipeline activity) creates a `spark_job` job; `GET .../validate` checks the definition and `GET .../runs` lists runs.

## Bringing Fabric notebooks

Notebooks imported from Fabric (`ipynb` or the Git source format) can be analysed first (`olcf fabric analyze`) and are then run through the normal notebook path, or sent from Fabric itself. See the [Fabric integration guide](fabric-integration.md).
