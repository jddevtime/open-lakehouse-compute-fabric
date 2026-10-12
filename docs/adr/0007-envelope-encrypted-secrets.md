# ADR 0007: Secrets are envelope-encrypted, write-only and resolved at the point of use

Status: Accepted

## Context

The platform holds credentials for storage accounts, identity providers, webhooks and Spark clusters. They must not appear in item definitions, exports, logs, capsules, events or API responses, and a leaked database must not reveal them.

## Decision

* A secret *value* is never part of connection metadata or an item definition. Those carry an opaque id (`sec_...`). The API can create, rotate and delete secrets but never read one back.
* The default provider (`file`) uses **envelope encryption**: each secret has its own data key, encrypted with AES-256-GCM, and the data key is wrapped by a **master key** identified by a key id. The master key comes from `OLCF_MASTER_KEY`, the operating-system keyring (optional extra) or a `0600` file under the data directory. HashiCorp Vault KV v2 is a second provider; other providers plug into `SecretProvider`.
* Services resolve a secret just in time with `SecretStore.resolve`. The job specification that carries them to the worker is written with mode `0600` in the job's working directory and deleted at job end, and secret values are removed from the job's logs.
* Everything user-visible passes `olcf.core.redaction`, which removes tokens, JWTs, storage keys, password assignments, private keys and credentials in URLs.

## Consequences

* A database backup alone does not reveal secrets, and restoring needs the master key: it can be included in a backup only together with an operator passphrase ([ADR 0013](0013-encrypted-logical-backup.md)). Losing the master key loses the secrets.
* The master key can be rotated in the code (`MasterKey.rotate`, `SecretStore.reencrypt_all`), but at version 0.1.0 **nothing calls these**: there is no CLI command or route for rotating the master key. Likewise `SigningKeys.rotate` has no caller. This is a known limitation.
* Redaction is pattern-based and conservative, so a secret in an unusual format could pass it; the rule is to never put one in a message in the first place.

## Alternatives considered

Storing secrets in plain text with file permissions (rejected); relying only on an external vault (supported, not required, since a laptop deployment has none).

## Verified by

`src/olcf/security/secrets.py`, `src/olcf/core/redaction.py`, `tests/security/test_leaks.py`, `tests/unit/test_foundation_smoke.py::test_secrets_never_leak`.
