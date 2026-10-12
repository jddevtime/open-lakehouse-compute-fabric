# ADR 0015: A built-in certificate authority and optional mutual TLS for workers

Status: Accepted

## Context

A control plane on a LAN usually has no public certificate, and workers need a way to prove their identity and to verify the server. Asking a laptop user to run a certificate authority is a barrier; forcing mutual TLS on everyone would make the first run harder.

## Decision

* The platform creates a private **EC P-256 certificate authority** under `<data dir>/pki` (valid 10 years). It can issue the server certificate (1 year, re-issued when its names change or fewer than 30 days remain, checked at server start) and **worker client certificates** (90 days), signed from a CSR at enrollment. Revoked certificates are listed on a revocation list that the hub checks.
* TLS is optional (`tls.enabled`); TLS in front of the server through a reverse proxy is equally supported. Mutual TLS for workers is a separate switch, `tls.require_client_cert_for_workers`, in which case the worker connection must present a certificate whose common name matches the worker.
* An operator can supply their own certificate and key instead.

## Consequences

* Mutual TLS adds identity that does not depend on the worker token alone: a stolen token is useless without the key.
* **Worker client certificates are renewed over the authenticated channel.** They are valid for `compute.worker_cert_lifetime_days` (90 by default); an agent renews inside the window `compute.worker_cert_renew_before_days` (30), or on demand with `olcf worker renew`; expiry is visible in the worker record, the `worker-certificates` health check and the alert rules `auth-expiry` and `worker-certificate-critical`. A worker that was offline until its certificate expired cannot renew and must be enrolled again. Covered by tests with test agents and a local server over plain HTTP; the mTLS handshake with a renewed certificate and a fleet over months were **not** exercised. This replaces the earlier known limitation that the code had no renewal.
* The CA key lives on the control plane's disk and in backups taken with `--include-keys`; protect both.

## Alternatives considered

Requiring an external PKI (rejected: first-run friction); trust-on-first-use pinning (rejected: weaker than a revocable certificate).

## Verified by

`src/olcf/security/pki.py`, `src/olcf/ops/tlsconfig.py`, `tests/security/test_tls.py`; the [TLS guide](../guides/tls-and-mtls.md).
