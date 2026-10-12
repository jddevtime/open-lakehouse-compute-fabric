# ADR 0005: Fabric notebooks are submitted as a signed capsule with a single-use nonce

Status: Accepted

## Context

A Fabric notebook that calls the SDK hands the control plane code to run on customer hardware. The request must be explicit about what is being run and with which parameters, must not carry credentials, must not be alterable in transit, and must not be replayable.

## Decision

The SDK submits an **execution capsule** (JSON Schema `schemas/capsule-v1.json`, generated from the server models): the normalised source, the Fabric context, the runtime request, the declared parameters, data references, policy and security claims. It is not an interpreter dump: only values the author declared as parameters travel, and a secret scanner (`find_secrets`) rejects credentials; secrets are references.

Signing and replay protection:

1. The SDK calls `POST /v1/integrations/fabric/challenge`. The server stores a single-use **nonce** bound to the caller and a fresh submission id, derives a per-submission HMAC key from its signing material, and returns it. The key is not stored; it is re-derived on verification.
2. The SDK canonicalises the capsule (sorted keys, compact separators, UTF-8, no NaN), and sends its SHA-256 digest and `HMAC-SHA256(key, "olcf.capsule.v1\0" + canonical)`.
3. The server recomputes both, compares in constant time, checks the timestamp window (`fabric.capsule_max_age_seconds`), the size limit and the nonce (exists, same caller, same submission, unexpired, unused), and marks the nonce used atomically. A retry of the same submission after a lost response is answered from the existing record.

## Consequences

* The signature protects **integrity and replay**. It does **not** prove who the author is: the key is handed to any authenticated caller. Identity comes from the bearer token (platform token or Entra JWT) and the integration's tenant and workspace allow-lists. A stolen token can obtain keys and sign anything its principal may submit, so revoke the token in that case.
* The capsule version is part of the contract; a new version must be accepted alongside the old one or rejected explicitly.
* Pull mode (the capsule carries identifiers and the control plane fetches the notebook through the Fabric REST API with a broker identity) shares the same signing path.

## Alternatives considered

Asymmetric signatures per user (rejected: needs key distribution to notebooks, which have no secure store); plain bearer authentication only (rejected: no integrity or replay protection for the body).

## Verified by

`src/olcf/fabric/capsule.py`, `src/olcf/fabric/signing.py`, `tests/fabric/test_capsule.py` and the signing and replay tests in `tests/fabric/`, `sdk/fabric/`.
