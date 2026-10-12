# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A real, in-process OIDC / Microsoft-Entra-shaped identity provider for tests (runs uvicorn on a local port).

It is a faithful *protocol* double, not a stub of OLCF code: discovery documents, a JWKS endpoint with **key rotation**, an authorization endpoint
that validates ``redirect_uri`` / PKCE parameters and auto-approves the configured user, a token endpoint (authorization_code with PKCE verification and
client-secret auth by Basic or POST body, ``client_credentials``, refresh), Entra-shaped claims (``tid``, ``oid``, ``roles``, ``groups``, ``idtyp=app``,
``azp``, group overage via ``_claim_names``) and a Microsoft Graph ``transitiveMemberOf`` endpoint.

Two issuers share one server:
* generic OIDC realm   issuer ``<base>/realm``;  endpoints ``/realm/authorize`` ``/realm/token``
* Entra tenant ``<tid>``  issuer ``<base>/<tid>/v2.0``;  endpoints ``/<tid>/oauth2/v2.0/authorize|token``
Tests drive misbehaviour through attributes (``tamper_*``) and forge tokens with :meth:`MockIdp.mint`.
"""

from __future__ import annotations

import base64
import hashlib
import json
import socket
import threading
import time
import uuid
from typing import Any
from urllib.parse import urlencode

import jwt
import uvicorn
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse

TENANT = "11111111-2222-3333-4444-555555555555"
OTHER_TENANT = "99999999-8888-7777-6666-555555555555"


def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


class Key:
    def __init__(self, kid: str | None = None) -> None:
        self.kid = kid or uuid.uuid4().hex[:12]
        self.private = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    @property
    def public_pem(self) -> bytes:
        return self.private.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)

    @property
    def private_pem(self) -> bytes:
        return self.private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())

    def jwk(self) -> dict[str, Any]:
        d = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(self.private.public_key()))
        d.update({"kid": self.kid, "use": "sig", "alg": "RS256"})
        return d


class MockIdp:
    def __init__(self, *, client_id: str = "olcf-web", client_secret: str = "idp-client-secret-VALUE", tenant: str = TENANT) -> None:  # noqa: S107 - fixture credential, not a secret
        self.client_id, self.client_secret, self.tenant = client_id, client_secret, tenant
        self.keys: list[Key] = [Key()]
        self.codes: dict[str, dict[str, Any]] = {}
        self.user: dict[str, Any] = {"sub": "user-1", "oid": "oid-user-1", "preferred_username": "alice@example.com", "email": "alice@example.com", "name": "Alice",
                                      "groups": ["g-analysts"], "roles": []}
        self.tamper: dict[str, Any] = {}  # id_token claim overrides applied at the token endpoint (e.g. {"aud": "evil"})
        self.drop_claims: set[str] = set()
        self.graph_groups: list[str] = []
        self.jwks_hits = 0
        self.token_calls = 0
        self.discovery_hits = 0
        self.fail_jwks = False
        self.id_token_key: Key | None = None  # sign id_tokens with this key instead (attack tests)
        self.id_token_alg = "RS256"  # noqa: S105 - fixture credential, not a secret
        self.disc_issuer_override: str | None = None  # discovery document claims another issuer (mix-up attack)
        self.port = 0
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self.app = self._build()

    # ------------------------------------------------------------------ lifecycle
    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def realm_issuer(self) -> str:
        return f"{self.base}/realm"

    @property
    def entra_issuer(self) -> str:
        return f"{self.base}/{self.tenant}/v2.0"

    def entra_issuer_for(self, tid: str) -> str:
        return f"{self.base}/{tid}/v2.0"

    def start(self) -> MockIdp:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        self.port = s.getsockname()[1]
        s.close()
        cfg = uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="error", lifespan="off")
        self._server = uvicorn.Server(cfg)
        self._thread = threading.Thread(target=self._server.run, daemon=True)
        self._thread.start()
        for _ in range(200):
            if self._server.started:
                return self
            time.sleep(0.02)
        raise RuntimeError("mock idp did not start")

    def stop(self) -> None:
        if self._server:
            self._server.should_exit = True
        if self._thread:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------ key management / token minting
    @property
    def key(self) -> Key:
        return self.keys[-1]

    def rotate(self, *, retire_old: bool = False) -> Key:
        k = Key()
        self.keys = [k] if retire_old else [*self.keys, k]
        return k

    def mint(self, claims: dict[str, Any], *, issuer: str | None = None, key: Key | None = None, alg: str = "RS256", headers: dict[str, Any] | None = None,
             include_kid: bool = True, secret: Any = None) -> str:
        k = key or self.key
        h = dict(headers or {})
        if include_kid:
            h.setdefault("kid", k.kid)
        now = int(time.time())
        body = {"iss": issuer or self.entra_issuer, "iat": now, "nbf": now - 1, "exp": now + 600, **claims}
        body = {k2: v for k2, v in body.items() if v is not None}
        if alg == "none":
            return b64u(json.dumps({"alg": "none", "typ": "JWT", **h}).encode()) + "." + b64u(json.dumps(body).encode()) + "."
        signing = secret if secret is not None else k.private_pem
        return jwt.encode(body, signing, algorithm=alg, headers=h)

    def access_claims(self, *, tid: str | None = None, aud: Any = "__default__", **extra: Any) -> dict[str, Any]:
        tid = tid or self.tenant
        base = {"aud": f"api://{self.client_id}" if aud == "__default__" else aud, "tid": tid, "ver": "2.0", "iss": self.entra_issuer_for(tid), "sub": "pairwise-sub-1",
                "oid": "oid-user-1", "preferred_username": "alice@example.com", "name": "Alice", "scp": "olcf.read", "azp": "client-app"}
        base.update(extra)
        return base

    def app_claims(self, **extra: Any) -> dict[str, Any]:
        c = self.access_claims()
        c.update({"oid": "oid-app-1", "sub": "oid-app-1", "azp": "app-client-1", "idtyp": "app", "roles": ["OLCF.Job.Submit"], **extra})
        c.pop("scp", None)
        c.pop("preferred_username", None)
        c.pop("name", None)
        return c

    # ------------------------------------------------------------------ app
    def _discovery(self, issuer: str, authz: str, token: str) -> dict[str, Any]:
        self.discovery_hits += 1
        return {"issuer": self.disc_issuer_override or issuer, "authorization_endpoint": f"{self.base}{authz}", "token_endpoint": f"{self.base}{token}", "jwks_uri": f"{self.base}/keys",
                "response_types_supported": ["code"], "id_token_signing_alg_values_supported": ["RS256"], "subject_types_supported": ["public"],
                "code_challenge_methods_supported": ["S256"]}

    def _build(self) -> FastAPI:
        app = FastAPI()
        idp = self

        @app.get("/realm/.well-known/openid-configuration")
        def disc_realm() -> dict[str, Any]:
            return idp._discovery(idp.realm_issuer, "/realm/authorize", "/realm/token")

        @app.get("/{tid}/v2.0/.well-known/openid-configuration")
        def disc_entra(tid: str) -> dict[str, Any]:
            return idp._discovery(idp.entra_issuer_for(tid), f"/{tid}/oauth2/v2.0/authorize", f"/{tid}/oauth2/v2.0/token")

        @app.get("/keys")
        def keys() -> Any:
            idp.jwks_hits += 1
            if idp.fail_jwks:
                return JSONResponse({"error": "down"}, status_code=503)
            return JSONResponse({"keys": [k.jwk() for k in idp.keys]}, headers={"Cache-Control": "max-age=3600"})

        def authorize(flavor: str, request: Request) -> Any:
            q = request.query_params
            if q.get("client_id") != idp.client_id:
                return JSONResponse({"error": "unauthorized_client"}, status_code=400)
            redirect_uri = q.get("redirect_uri", "")
            if not redirect_uri or q.get("response_type") != "code" or q.get("code_challenge_method") != "S256" or not q.get("code_challenge"):
                return JSONResponse({"error": "invalid_request", "detail": "PKCE S256 and response_type=code are required"}, status_code=400)
            if not q.get("state") or not q.get("nonce"):
                return JSONResponse({"error": "invalid_request", "detail": "state and nonce are required"}, status_code=400)
            code = uuid.uuid4().hex + uuid.uuid4().hex
            idp.codes[code] = {"flavor": flavor, "nonce": q["nonce"], "challenge": q["code_challenge"], "redirect_uri": redirect_uri, "scope": q.get("scope", ""),
                               "user": dict(idp.user), "used": False}
            if idp.user.get("__deny"):
                return RedirectResponse(f"{redirect_uri}?{urlencode({'error': 'access_denied', 'state': q['state']})}", status_code=302)
            return RedirectResponse(f"{redirect_uri}?{urlencode({'code': code, 'state': q['state']})}", status_code=302)

        @app.get("/realm/authorize")
        def authz_realm(request: Request) -> Any:
            return authorize("realm", request)

        @app.get("/{tid}/oauth2/v2.0/authorize")
        def authz_entra(tid: str, request: Request) -> Any:
            return authorize(f"entra:{tid}", request)

        def check_client(request: Request, client_id: str | None, client_secret: str | None) -> bool:
            auth = request.headers.get("authorization", "")
            if auth.lower().startswith("basic "):
                from urllib.parse import unquote

                raw = base64.b64decode(auth[6:]).decode()
                cid, _, sec = raw.partition(":")
                return unquote(cid) == idp.client_id and unquote(sec) == idp.client_secret
            return client_id == idp.client_id and client_secret == idp.client_secret

        async def token(flavor: str, request: Request) -> Any:
            idp.token_calls += 1
            form = await request.form()
            f = {k: str(v) for k, v in form.items()}
            if not check_client(request, f.get("client_id"), f.get("client_secret")):
                return JSONResponse({"error": "invalid_client"}, status_code=401)
            grant = f.get("grant_type")
            is_entra = flavor.startswith("entra:")
            tid = flavor.split(":", 1)[1] if is_entra else None
            issuer = idp.entra_issuer_for(tid) if tid else idp.realm_issuer
            now = int(time.time())
            if grant == "authorization_code":
                rec = idp.codes.get(f.get("code", ""))
                if not rec or rec["used"] or rec["flavor"] != flavor:
                    return JSONResponse({"error": "invalid_grant"}, status_code=400)
                rec["used"] = True
                if f.get("redirect_uri") != rec["redirect_uri"]:
                    return JSONResponse({"error": "invalid_grant", "detail": "redirect_uri mismatch"}, status_code=400)
                verifier = f.get("code_verifier", "")
                if b64u(hashlib.sha256(verifier.encode()).digest()) != rec["challenge"]:
                    return JSONResponse({"error": "invalid_grant", "detail": "PKCE verification failed"}, status_code=400)
                user = {k: v for k, v in rec["user"].items() if not k.startswith("__")}
                claims: dict[str, Any] = {"iss": issuer, "aud": idp.client_id, "iat": now, "exp": now + 300, "nonce": rec["nonce"], **user}
                if is_entra:
                    claims.update({"tid": tid, "ver": "2.0"})
                    claims.setdefault("oid", user.get("oid"))
                access = idp.mint({"iss": issuer, "aud": "https://graph.microsoft.com", "tid": tid, "scp": "User.Read", "oid": user.get("oid")}, issuer=issuer)
                claims.update(idp.tamper)
                for d in idp.drop_claims:
                    claims.pop(d, None)
                if rec["user"].get("__overage"):
                    claims.pop("groups", None)
                    claims["_claim_names"] = {"groups": "src1"}
                    claims["_claim_sources"] = {"src1": {"endpoint": "https://graph.windows.net/x/users/y/getMemberObjects"}}
                idt = idp.mint(claims, issuer=issuer, key=idp.id_token_key, alg=idp.id_token_alg)
                return {"access_token": access, "id_token": idt, "token_type": "Bearer", "expires_in": 3599, "refresh_token": "rt-" + uuid.uuid4().hex}
            if grant == "client_credentials":
                scope = f.get("scope", "")
                aud = scope.removesuffix("/.default") if scope.endswith("/.default") else (scope or "api://" + idp.client_id)
                tok = idp.mint({"aud": aud, "tid": tid, "oid": "oid-app-1", "sub": "oid-app-1", "azp": idp.client_id, "idtyp": "app", "roles": ["OLCF.Job.Submit"], "ver": "2.0"},
                               issuer=issuer)
                return {"access_token": tok, "token_type": "Bearer", "expires_in": 3599}
            return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

        @app.post("/realm/token")
        async def token_realm(request: Request) -> Any:
            return await token("realm", request)

        @app.post("/{tid}/oauth2/v2.0/token")
        async def token_entra(tid: str, request: Request) -> Any:
            return await token(f"entra:{tid}", request)

        @app.get("/graph/v1.0/me/transitiveMemberOf/microsoft.graph.group")
        def graph(request: Request) -> Any:
            if not request.headers.get("authorization", "").lower().startswith("bearer "):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
            return {"value": [{"id": g} for g in idp.graph_groups]}

        return app
