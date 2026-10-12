# ADR 0009: The pipeline engine is durable, with exactly-once trigger claims

Status: Accepted

## Context

Pipelines run for hours, call external systems and notebooks, and must survive control-plane restarts and leader changes. A trigger (a cron time, a webhook delivery, a file arrival) must start a pipeline once, even with several replicas and retries.

## Decision

* **The database is the state.** A run is a row with an immutable snapshot of the definition; each activity attempt is a row, created lazily, with a unique `(run_id, scope_key)` so creation is idempotent. Nothing important lives only in memory.
* One **dispatcher**, running on the leader, repeatedly advances every active run. Blocking and asynchronous activity code runs on a bounded thread pool; inline activities run on the dispatcher.
* An attempt is claimed with a random `lease_token` (compare-and-set), and every write that ends an attempt is conditional on that token, so a deposed leader cannot overwrite its successor.
* **Crash recovery is deterministic.** Asynchronous activities resume from persisted state (a job id, a child run id, a wake-up time, a callback token); job submission is idempotent per activity run and attempt. Blocking activities that are declared safe to repeat (`resumable`, or marked `policy.idempotent`) run again as the same attempt; any other interrupted activity fails with the code `interrupted` and is never silently run twice.
* **Exactly-once triggers.** Every firing first claims `(trigger_id, fire_key)` by inserting a `trigger_runs` row with a unique constraint. The `fire_key` is the fire time (schedule), the delivery id or signature (webhook), a content hash (file batch) or the event id (dependency). The started run carries the idempotency key `trg:<trigger>:<fire_key>`, which repairs a crash between claim and start.
* Schedules use cron with an IANA time zone, DST-aware, and a catch-up policy (`skip`, `run_once`, `run_all`). The clock is injectable.

## Consequences

* Any replica can take over in-flight runs. The cost is database writes per activity transition; the engine is meant for orchestration, not for per-record work.
* "Exactly once" is for starting runs and for activity attempts as described; side effects of a *blocking* activity that is neither resumable nor idempotent are at-most-once, by design.
* Resume-from-failure reuses successful results only when the activity path, the definition hash and all upstream reuse match.

## Alternatives considered

An external workflow engine (rejected: dependency); in-memory scheduling with a persisted log (rejected: failover would need replay logic anyway).

## Verified by

`src/olcf/pipelines/engine.py`, `src/olcf/pipelines/triggers.py`, `tests/orch/test_engine.py`, `tests/orch/test_recovery.py`, `tests/orch/test_triggers.py`.
