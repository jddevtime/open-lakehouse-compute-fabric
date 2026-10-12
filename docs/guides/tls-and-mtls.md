# TLS and mutual TLS

OLCF can protect two things with certificates: the connection of every client to the control plane (TLS), and the connection of every worker agent (mutual TLS, where the worker also proves its identity). This guide shows the three ways to run it, and what each was verified against.

| Setup | Use when | Worker mTLS |
|---|---|---|
| Built-in TLS with the built-in CA | a lab, a small team, an isolated network | yes, supported |
| Built-in TLS with your own certificate | the server name must be trusted by browsers without distributing a CA | yes, with the built-in CA for worker certificates |
| TLS terminated by a reverse proxy (Caddy, nginx, an ingress) | you already run one; this is the usual production shape | only if the proxy forwards a verified client certificate; otherwise worker identity is the worker token |

Plain HTTP is for `localhost` only. The server binds to 127.0.0.1 by default; set `server.host` to expose it only together with TLS (the Certificates page shows a warning banner while HTTPS is off).

## Built-in TLS

```bash
olcf init --tls --public-url https://olcf.example.com --port 8470
olcf serve
```

or set `tls.enabled = true` and `server.public_url` in `config.toml` (`OLCF_TLS__ENABLED=true`). On the first start the control plane creates a private certificate authority in `<data dir>/pki`:

* `ca.crt`, `ca.key`: an EC P-256 root, valid for 10 years, key mode 0600. This is the trust anchor of the installation. It is part of a backup made with `--include-keys` and should not live on shared storage.
* `server.crt`, `server.key`: the server certificate, valid one year, issued for the host in `server.public_url`, the entries in `tls.extra_sans`, `localhost`, `127.0.0.1` and `::1`. It is reissued when the list of names changes or fewer than 30 days remain (checked when the server starts).

Checked on a test installation (`--tls`, public URL `https://localhost:18480`) while writing this page: `curl` without the CA failed, `curl --cacert pki/ca.crt` returned `{"status":"ready",...}`, and `openssl s_client` reported `Verification: OK` with issuer `OLCF Root CA`. The certificate carried the subject alternative names `127.0.0.1`, `::1`, `localhost` and the machine's host name.

Give clients the CA:

```bash
olcf --ca-bundle /path/to/ca.crt status                  # or OLCF_CA_BUNDLE
olcf context set prod --url https://olcf.example.com --ca-bundle /path/to/ca.crt --use
```

The SDK takes `Client(..., ca_bundle=...)`. Browsers need the CA imported into the trust store, or use your own certificate.

### Your own certificate

```toml
[tls]
enabled = true
cert_file = "/etc/olcf/tls/olcf.example.com.crt"      # full chain
key_file  = "/etc/olcf/tls/olcf.example.com.key"
```

On start the files are validated: readable, parseable, not expired, the key matches the certificate, and a warning is logged when the key file is readable by group or others. A failure stops the start with a message that names the file. Renewal is yours: replace the files and restart the server (or the replica, one at a time, in a high-availability setup). Worker certificates are still issued by the built-in CA, which exists regardless.

### Protocol and ciphers

TLS 1.2 uses only ECDHE with AES-GCM or ChaCha20-Poly1305 (`ECDHE+AESGCM:ECDHE+CHACHA20`); TLS 1.3 suites are AEAD by definition; SSLv3, TLS 1.0 and TLS 1.1 are refused by the OpenSSL defaults of every supported Python. With TLS on, responses carry `Strict-Transport-Security` (one year, include subdomains).

## Worker mutual TLS

```toml
[tls]
enabled = true
require_client_cert_for_workers = true
```

With this setting:

1. At enrollment the agent generates its own EC key and sends a certificate signing request. The control plane signs it with the built-in CA, forcing the subject to `OU=olcf-worker, CN=<worker id>` and the client-authentication usage, valid for **90 days**. The private key never leaves the machine (`<state dir>/client.key`, mode 0600; the certificate is `client.crt`, the pinned CA `ca.pem`).
2. The listener requests a client certificate. A certificate from another CA, or an expired one, fails the TLS handshake.
3. On `/v1/workers/ws` the hub then requires a certificate, requires its common name to equal the worker id belonging to the worker token, and rejects revoked serials. A connection that fails any of this is closed with code 4403.
4. Revoking a worker (`olcf worker revoke`) adds its certificate serial to the revocation list and disconnects it; re-enrolling revokes the old certificate.

Checked on the test installation: a worker enrolled with `olcf-worker run --url https://localhost:18480 --token olcf_enr_... --ca-pem pki/ca.crt`, connected over `wss://` and appeared as `online`; its certificate showed `CN = wrk_...`, issuer `OLCF Root CA`, extended key usage `TLS Web Client Authentication`, 90 days of validity. A second connection that presented the correct worker token but no certificate was closed with `4403 a client certificate is required on the worker channel (tls.require_client_cert_for_workers)`. The automated tests are in `tests/security/test_tls.py` (7 tests, passed) and the mTLS cases of `tests/ops`.

> **Certificate lifetime and renewal.** Worker certificates are issued for `compute.worker_cert_lifetime_days` (default 90) and are **renewed automatically over the authenticated control channel** when fewer than `compute.worker_cert_renew_before_days` (30) remain (with a random delay so a fleet does not ask at once; `compute.worker_cert_auto_renew=false` leaves it to the operator). The agent generates a new key pair on the node, the control plane signs it (subject `CN=<worker id>`), and the old certificate and token stay valid until the node's next connection authenticates with the new ones, or at most `compute.worker_token_grace_seconds` (24 h), then they are revoked. `olcf worker renew WORKER` renews on demand (`--no-cert` / `--no-token` choose what). Expiry shows in `olcf worker get`, the `worker-certificates` health check and the alert rules `auth-expiry` and `worker-certificate-critical`. A worker that is offline when its certificate expires cannot renew and, with `require_client_cert_for_workers`, can no longer connect: enrol it again with a new token (`olcf-worker run --re-enroll --token ...`). Without the setting, certificate expiry does not stop a worker (the token is the credential). The renewal protocol was tested over plain HTTP with test agents and against a local server; **the TLS handshake with a renewed client certificate has not been exercised end to end.**

The token alone is a bearer credential: anyone who copies `identity.json` can use it. mTLS is what makes a copied token useless without the key; set it on any network you do not fully control.

## Behind a reverse proxy

Do not enable `tls.enabled` on the control plane when a proxy terminates TLS. Instead:

* set `server.public_url` to the external address and `server.trusted_proxies` to the number of proxies in front (so that client addresses and schemes come from `X-Forwarded-*`);
* let the proxy pass WebSocket upgrades for `/v1/workers/ws` and `/v1/sessions/{id}/ws`, and not buffer `/v1/events/stream` ([ports and networking](../reference/ports-and-networking.md#reverse-proxy));
* for worker mTLS, make the proxy verify the client certificate against the platform CA (`pki/ca.crt`), forward the certificate URL-encoded in the `X-Client-Cert` header, and delete any `X-Client-Cert` header sent by the client. The control plane trusts the header only when `server.trusted_proxies` is greater than zero.

Working proxy configurations, `deploy/caddy/Caddyfile` and `deploy/nginx/nginx.conf`, are in the repository; Docker Compose has a `tls` profile that adds Caddy (`deploy/README.md`).

## Certificates page and expiry

**Security, Certificates** shows the transport state (address, HTTPS on or off), the number of active and revoked node identities, and the server and CA certificates with the days left. The same data comes from the `certificates` diagnostics (`cert:ca`, `cert:server`, and `cert:tls.cert_file` for your own certificate): `warn` below 30 days, `fail` when expired (`POST /v1/admin/diagnostics`). `olcf doctor` has a `tls` check for the server certificate, and the built-in alert rule "Credentials and certificates expiring" (type `auth_expiry`, default window 14 days) raises an alert for the CA and server certificates and for tokens that expire soon. **Worker client certificates are not covered by any of these.**

## Trouble

| Symptom | Cause |
|---|---|
| Client: `certificate verify failed` | the CA is not trusted. Pass `--ca-bundle` or install `pki/ca.crt` |
| Client: `hostname mismatch` | the name in the URL is not in the certificate. Add it to `tls.extra_sans` (or `server.public_url`) and restart; the certificate is reissued |
| Worker: closes with 4403 | mTLS is required and the agent has no valid certificate, or its CN does not match. Re-enroll with `--re-enroll` and a new token |
| Worker: TLS handshake error after months | the worker certificate expired (it was offline during its renewal window, or auto-renewal is off). Re-enroll |
| Start fails with `tls_config_invalid` | the custom certificate or key is unreadable, expired, or does not match; the message names the file |
| Workers cannot connect through the proxy | the proxy does not upgrade WebSockets, or `server.public_url` is wrong (enrollment commands embed it) |
