# ADR 0004: Spark runtimes are signed, hash-pinned and immutable

Status: Accepted

## Context

Notebook results depend on the exact versions of Python, PySpark and libraries. Machines in a fleet drift. A runtime that is "whatever pip resolved today" makes results irreproducible and is a supply-chain risk, since the agent downloads and executes code.

## Decision

A **runtime** is a versioned, immutable environment described by a manifest:

* Python packages are pinned with SHA-256 hashes for every supported platform and installed with `--require-hashes`; Maven artifacts are checked against their published checksum and their hashes are recorded in a per-node `lock.json`, re-verified on every runtime start.
* The manifest is signed with **Ed25519**; the signature covers the canonical JSON without its `signature` member. A trust store maps key ids to public keys.
* Runtimes install **side by side** under `<state>/runtimes/<id>/`; nothing is upgraded in place. Installation is transactional: built in a staging directory, verified with a smoke test, then moved into place with an atomic rename. A failed install leaves the previous state untouched. Retire and roll back are operations on the same layout.
* Three runtimes ship: `olcf-rt-2.0.0` (family `fabric-2.0-compatible`), `olcf-rt-1.3.0` (`fabric-1.3-compatible`) and `olcf-rt-native-1.0.0` (`olcf-native`). Jobs ask for a family or an exact id.
* Compute nodes reach runtimes by id; a job that asks for a runtime the node lacks is not placed there.

## Consequences

* Reproducible runs and a reviewable diff for every dependency change (`scripts/lock_runtime.py` regenerates the manifests; the diff is the audit trail).
* **The shipped manifests are signed with a development key whose private half is in the source tree**, deliberately, so that developers and tests can sign. Anyone can forge a manifest signed with it, so the platform **does not trust that key in production**: it is trusted only when `environment` is not `production` (or `runtime.trust.allow_development_key = true`); an unset environment is production, and `olcf init` / `olcf quickstart` write `environment = "development"` for local installations. A production installation therefore refuses the shipped manifests until the operator signs them with their own key and points `runtime.trust.dir` / `OLCF_RUNTIME_TRUST_DIR` (control plane) and `--runtime-trust-dir` / `OLCF_RUNTIME_TRUST_DIR` (external agents) at a directory holding the public key (`python -m olcf.runtime keygen|trust|sign`; [hardening](../operations/hardening.md#6-supply-chain)). The `runtime-trust` health check reports the state (`down`: the development key is trusted in production), but it is advisory: it does not stop the server. The consequences are an extra setup step for production and a changed default for existing data directories without `environment`; the shipped manifests themselves are still signed with the development key, so the artifacts a user downloads cannot be installed in production out of the box. This remains the largest operational friction in the default configuration; it replaced the earlier, larger gap of trusting the key everywhere.
* A run records the fingerprint of the runtime it ran on; `scripts/verify_dist.py` still allows only the documented development key into the artifacts.
* Disk use grows with each installed runtime; retire what is not used.
* Installing a runtime for the first time needs Internet access or an offline bundle.

## Alternatives considered

Floating `pip install` at job start (rejected: irreproducible, unsafe); containers per runtime (supported separately through the Kubernetes provider, but not required: a laptop should not need Docker); signing with a project-held secret (rejected for the open-source distribution, since the secret cannot be kept in a public repository).

## Verified by

`src/olcf/runtime/` (`manager.py`, `signing.py`, `locktool.py`), `tests/node/test_manifest_signing.py`, `tests/security/test_runtime_trust.py` (the trust policy, the `runtime-trust` check and the `trust` sub-command), `tests/node/test_runtime_lifecycle.py`, `tests/node/test_maven.py`, `scripts/verify_dist.py` (which allows only the documented development key into the artifacts).
