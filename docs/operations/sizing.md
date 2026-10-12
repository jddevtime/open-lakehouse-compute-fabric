# Sizing

Honest guidance: the project has measured the footprint of the pieces on small machines, not benchmarked large clusters. The numbers below marked "measured" come from the development environment (2 vCPU, 7 GB, Linux); the rest follows from settings and from how Spark works, and you should confirm it with your workload using `olcf compute smoke`.

## Control plane

| Item | Figure |
|---|---|
| Idle memory of `olcf serve` with the sample workspace, after browsing and a few queries | about 550 MB resident (measured). DuckDB queries, previews and exports add memory while they run, bounded per query by `sql.duckdb_memory_mb` (1024) and by `sql.max_concurrent_queries` (4) |
| Embedded worker agent (idle) | about 55 MB (measured) |
| SQLite database with the sample workspace and a day of testing | about 7 MB plus WAL (measured) |
| Disk for a runtime | the pinned test runtime (`olcf-rt-test-4.1.1`: Spark 4.1.1 and its Python dependencies, without the Delta and Iceberg jars) measured 929 MB with `du`; a production runtime with the lakehouse jars was not measured and is larger, so plan for several GB per installed runtime family |
| Disk for jobs | `jobs/<id>/` holds the spec, work directory and logs for every job kept; bounded by retention (`retention.job_history_days` 365, `retention.log_days` 90) |
| API processes | `olcf serve --workers N` runs N API processes (needs PostgreSQL; disables the embedded worker) |

SQLite is right for one server process on one machine, a team or a laboratory. Move to PostgreSQL when you want replicas ([high availability](../guides/high-availability.md)), several API processes, or the database on a managed service. Both are tested. The control plane is not CPU bound for metadata work; SQL on DuckDB and report tiles are the heavy parts, and they are bounded by the limits above.

Put the control plane close to your object storage if users run large previews or DuckDB queries through it: that path moves data through the control plane (see [who touches the data](../concepts/components-and-data-flow.md#who-touches-the-data)). Spark jobs read storage directly from workers.

## Workers

A worker advertises its cores and memory, minus a reserve that keeps the machine usable:

* `compute.reserve_cores` (1) cores and `compute.reserve_memory_fraction` (15 %) of RAM are left to the operating system and the agent;
* `compute.driver_reserve_fraction` (25 %) of what remains is kept free for drivers and kernels next to Spark worker processes.

An agent started with `--max-cores` or `--max-memory-mb` advertises less than the machine has.

Jobs default to a 1 core, 1 GiB driver and 1 core, 1 GiB executors (`ResourceRequest`); a Spark JVM needs more than the heap you ask for, so leave headroom. In local mode a job without an explicit request gets about half the usable cores (at most 4) and a quarter of the memory (at most 4 GiB).

Rules of thumb (not measurements):

| Workload | Start with |
|---|---|
| Notebooks and small ETL on Spark local mode | one worker with 4 cores and 16 GB, 1 to 3 concurrent jobs |
| Python-only jobs (`engine: python`) | 1 core and 1 to 2 GB per job; no Spark ports used |
| Interactive sessions | each session keeps a kernel and, for Spark, a driver alive until it idles out (`compute.session_idle_timeout_seconds`, 3600) or the per-user limit (`compute.max_sessions_per_user`, 5) is reached; budget their memory as for drivers |
| A standalone Spark cluster | at least two workers; each cluster node runs a Spark worker and the agent; keep executor memory below node memory minus the reserve |
| Many small concurrent jobs | the limit is ports as well as cores: 100 driver ports per node by default (`compute.driver_port_range`) |

Disk on workers: the runtime directory, the per-job work directories under `jobs/`, and whatever temporary space Spark uses for shuffle and spill. Use fast local disk for shuffle-heavy jobs.

## Scaling out

Add workers with `olcf worker enroll-token` and `olcf-worker run`; group them in pools and label them for placement ([cluster guide](../guides/cluster-and-control-panel.md)). The scheduler sorts jobs by priority with aging, fair share and FIFO; the pool and label constraints are hard. The cap on concurrently executing pipeline runs is `pipelines.max_active_runs` (500); see the [configuration reference](../reference/configuration.md) for the other limits.

## Check it

* `olcf compute smoke` runs a real Spark job that exercises the driver, executors, shuffle and a Delta write. Use it after every sizing change.
* `GET /v1/metrics/names` and `/v1/metrics/series` show cores in use, queue depth and wait times over time.
