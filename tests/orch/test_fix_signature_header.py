# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""FIX G: a webhook trigger's ``signature_header`` is a header *name* and is shown as configured; the secret and real signatures stay redacted."""

from __future__ import annotations

import json
import time

from olcf.core.redaction import REDACTED, redact_obj
from tests.orch.orch_support import OrchStack
from tests.orch.test_api_e2e import A, sign


def test_redaction_keeps_the_header_name_but_not_secrets():
    doc = redact_obj({"signature_header": "X-Hub-Signature-256", "signature": "sha256=abcdef0123456789", "secret": "s3cr3t-value", "client_secret": "x", "token": "t0k3n-value",
                      "headers": {"x-olcf-signature": "sha256=abcdef0123456789", "x-olcf-timestamp": "123"}, "secret_id": "sec_123", "timestamp_header": "X-OLCF-Timestamp"})
    assert doc["signature_header"] == "X-Hub-Signature-256" and doc["timestamp_header"] == "X-OLCF-Timestamp" and doc["secret_id"] == "sec_123"  # noqa: S105 - a secret *reference* (id), not a secret
    assert doc["signature"] == doc["secret"] == doc["client_secret"] == doc["token"] == REDACTED
    assert doc["headers"]["x-olcf-signature"] == REDACTED and doc["headers"]["x-olcf-timestamp"] == "123"


def test_trigger_api_returns_the_header_name_and_never_the_secret(stack: OrchStack, ws: str) -> None:
    h = stack.admin
    stack.pipeline(ws, "sig-target", [A("A", "wait", waitTimeInSeconds=0)])
    base = {"workspace_id": ws, "type": "webhook", "target": {"type": "pipeline", "ref": "sig-target"}}
    default = stack.http.post("/v1/triggers", headers=h, json={**base, "name": "sig-default"}).json()
    assert default["config"]["signature_header"] == "X-OLCF-Signature" and default["config"]["timestamp_header"] == "X-OLCF-Timestamp"
    custom = stack.http.post("/v1/triggers", headers=h, json={**base, "name": "sig-custom", "config": {"signature_header": "X-Hub-Signature-256"}})
    assert custom.status_code == 201, custom.text
    t = custom.json()
    secret = t["credentials"]["secret"]
    assert t["config"]["signature_header"] == "X-Hub-Signature-256"
    for view in (stack.http.get(f"/v1/triggers/{t['id']}", headers=h).json(), stack.http.get("/v1/triggers", headers=h, params={"workspace_id": ws}).json()):
        text = json.dumps(view)
        assert "X-Hub-Signature-256" in text and REDACTED not in json.dumps(view if "items" not in view else [i["config"] for i in view["items"]]) and secret not in text
    # the configured name is really what the public endpoint reads
    path = t["credentials"]["url"][len(stack.settings.base_url):]
    body = json.dumps({"count": 1}).encode()
    headers = sign(secret, body)
    headers["X-Hub-Signature-256"] = headers.pop("X-OLCF-Signature")
    assert stack.http.post(path, content=body, headers=headers).status_code == 202
    assert stack.http.post(path, content=body, headers=sign(secret, body, ts=int(time.time()) - 1)).status_code == 401
    # the stored fire record keeps the signature VALUE redacted
    fires = stack.http.get(f"/v1/triggers/{t['id']}/runs", headers=h).json()["items"]
    assert fires and "sha256=" not in json.dumps(fires)
