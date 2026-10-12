# ADR 0016: Apache-2.0 licence and DCO sign-off, no CLA

Status: Accepted

## Context

The project is meant to be used and extended by companies and individuals, including inside proprietary environments, and to accept outside contributions without a legal process that discourages them.

## Decision

* The licence is the **Apache License 2.0**, which includes an express patent grant. Every Python file carries an SPDX identifier and a copyright line (checked by `scripts/check_headers.py`).
* Contributions are accepted under the same licence (section 5 of Apache-2.0) with a **Developer Certificate of Origin** sign-off on each commit (`git commit -s`), not a contributor licence agreement.
* The release build generates `THIRD_PARTY_NOTICES.md` and fails on unknown or strong-copyleft licences (`scripts/licenses.py --check`). Third-party components keep their own licences ([NOTICE](../../NOTICE)).
* The Microsoft Fabric, Spark and other names are used only to describe compatibility; the project does not imply endorsement ([TRADEMARKS.md](../../TRADEMARKS.md)).

## Consequences

* Anyone may use, modify and redistribute, including commercially, keeping the notices.
* The project cannot relicense later without the consent of contributors, since there is no CLA.
* The development runtime-signing key is public by design ([ADR 0004](0004-signed-immutable-runtimes.md)); the licence says nothing about its use as a trust anchor, which is a security matter.

## Alternatives considered

MIT (no patent grant); a copyleft licence (rejected: would deter use inside enterprises); a CLA (rejected: friction).

## Verified by

`LICENSE`, `NOTICE`, `CONTRIBUTING.md`, `scripts/check_headers.py`, `scripts/licenses.py`.
