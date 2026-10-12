# ADR 0008: The audit log is a hash chain

Status: Accepted

## Context

Security-relevant actions (sign-ins, role changes, secret operations, destructive data operations, job submission, backups) need a record that an administrator cannot silently edit after the fact, without requiring an external service.

## Decision

Audit records are appended to a database table; each stores `hash = SHA-256(prev_hash || canonical(record))`, forming a chain. `AuditLog.verify` (CLI `olcf audit verify`) walks the chain and reports the first break. Records are redacted before storage and can be exported as JSON lines or CEF for a SIEM. Writes are serialised (a process lock, and a PostgreSQL advisory lock across replicas) so that two writers cannot fork the chain.

## Consequences

* The chain is **tamper-evident, not tamper-proof**: someone with database write access can rewrite the whole chain consistently. To make deletion or rewriting detectable, export the log (or its latest hash) to a system the database administrator cannot write to.
* Retention (`audit_days`, minimum 30) archives purged records to a gzip file with its SHA-256 and records an anchor (last purged sequence and hash) in the same transaction, so a purge is distinguishable from an attacker's truncation, and the newest record is always kept. Copy the head hash from `GET /v1/admin/retention/audit` to a system the database administrator cannot write to.
* Verification of a large chain reads every record.

## Alternatives considered

An external append-only log service (rejected: dependency); a signed log (the signing key would sit next to the database, adding little).

## Verified by

`src/olcf/security/audit.py`, `tests/unit/test_foundation_smoke.py::test_audit_chain_verifies_and_detects_tamper`, `tests/ops/test_retention.py`.
