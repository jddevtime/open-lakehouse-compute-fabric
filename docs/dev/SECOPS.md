# Security and operations: operator notes

Load-balancer pattern for two or more OLCF control-plane nodes (see CONTRACTS.md, S8):

```
clients --> L7 balancer (TLS) --+--> olcf node 1 :8470  \
                                 +--> olcf node 2 :8470   >-- one PostgreSQL (OLCF_DB__URL)
workers --> L4 passthrough ------+--> (mTLS, worker port)  /
```

* Same `OLCF_DB__URL`, `OLCF_MASTER_KEY`, `OLCF_JWT_SIGNING_KEY` on every node.
* Balancer health check: `GET /v1/health/ready`. Disable response buffering and use idle timeouts of at least 60 s for `/v1/events/stream`, job log streams and session WebSockets.
* Set `server.trusted_proxies` to the number of proxy hops in front of the nodes.
* Worker mTLS: TCP passthrough to the node, or terminate at the balancer and forward the verified certificate in `X-Client-Cert` (strip client-supplied copies; the nodes must then be reachable only through the balancer).
* Failover: when the leader dies another node takes over within the lease ttl (15 s). Background loops run only on the leader; every node serves API requests.

Test suites: `pytest tests/security` (tenant isolation, escalation, 401 sweep, fuzzing, leaks, tokens, streams, TLS, federation) and `pytest tests/ops` (backup, updates, diagnostics, retention, HA). The HA test starts two uvicorn processes against PostgreSQL when `OLCF_TEST_POSTGRES_URL` is set (otherwise it is skipped); the fuzz seed and size come from `OLCF_FUZZ_SEED` / `OLCF_FUZZ_ROUNDS`. Hypothesis-based tests need `hypothesis` (`dev` extra).
