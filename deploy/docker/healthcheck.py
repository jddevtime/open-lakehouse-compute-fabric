# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Container health probe: GET /v1/health on the loopback interface (http, or https with the built-in CA when TLS is enabled).  Exit 0 = healthy.

An optional argument names another path of the API, for example ``/v1/health/ready``: the Helm chart's readiness probe uses it so a node that answers 503 there (database
unavailable, or production trusting the public development runtime-signing key) is taken out of the Service, while liveness and startup keep probing ``/v1/health``."""

import os
import ssl
import sys
import urllib.request

port = os.environ.get("OLCF_PORT", "8470")
path = sys.argv[1] if len(sys.argv) > 1 else "/v1/health"
if not path.startswith("/v1/health"):
    sys.exit(2)  # only the health endpoints are probed
tls = os.environ.get("OLCF_TLS__ENABLED", os.environ.get("OLCF_TLS_ENABLED", "false")).lower() == "true"
ctx = None
if tls:
    ca = os.path.join(os.environ.get("OLCF_DATA_DIR", "/data"), "pki", "ca.crt")
    ctx = ssl.create_default_context(cafile=ca if os.path.exists(ca) else None)
    ctx.check_hostname = False  # loopback probe: the certificate names the service, not 127.0.0.1
try:
    with urllib.request.urlopen(f"{'https' if tls else 'http'}://127.0.0.1:{port}{path}", timeout=3, context=ctx) as resp:  # noqa: S310
        sys.exit(0 if resp.status == 200 else 1)
except Exception:  # noqa: BLE001 - any failure means unhealthy
    sys.exit(1)
