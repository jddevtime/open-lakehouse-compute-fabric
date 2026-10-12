# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""LDAP / Active Directory bridge (spec 22: "optional LDAP/AD integration through a supported bridge").

Two supported ways to use a directory with OLCF
-----------------------------------------------
1. **Recommended: front the directory with an OIDC bridge** (Dex, Keycloak, Authentik, AD FS, Entra Domain Services + Entra). OLCF then only
   speaks OIDC (the ``oidc`` / ``entra`` provider types) and never sees directory passwords. Nothing in this module is involved.
2. **Built-in bridge (this module, provider type ``ldap``)**: OLCF binds to the directory itself using the optional ``ldap3`` package
   (``pip install ldap3``). The login endpoint is ``POST /v1/identity/ldap/{provider}/login``. The flow is the standard *search-then-bind* pattern:

   * connect with TLS (``ldaps://`` or StartTLS; certificate verification is always on, ``ca_file`` adds a private CA; plain ``ldap://`` needs the
     explicit ``allow_insecure_ldap`` opt-in and is refused otherwise),
   * bind as the service account (``bind_dn`` + the provider's stored client secret) and search ``user_base_dn`` with ``user_filter``; the user name
     is *escaped* into the filter (RFC 4515) so ``*)(uid=*`` style injection is impossible, and anything but exactly one match is a failure,
   * re-bind as the found DN with the supplied password (an empty password is rejected up front - an LDAP simple bind with an empty password is an
     *unauthenticated* bind that many servers report as success),
   * collect groups from ``memberOf`` and/or a group search (``nested_groups`` uses the AD ``LDAP_MATCHING_RULE_IN_CHAIN`` rule),
   * map groups to roles with the same rule engine as OIDC claims (``claim: "groups"`` matches group CNs and full DNs).

Test coverage: the mapping, filter-escaping, bind sequencing and failure handling are unit-tested against ``ldap3``'s in-memory
``MOCK_SYNC`` directory. Tests against an AD / OpenLDAP server are marked ``live``.
"""

from __future__ import annotations

import logging
import re
import ssl
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from olcf.core.errors import BadRequest, Unauthenticated, Unavailable, Unsupported
from olcf.security.federation_mapping import LdapConfig

log = logging.getLogger("olcf.ldap")
_CTRL = re.compile(r"[\x00-\x1f\x7f]")


@dataclass
class LdapIdentity:
    """A user found in a directory: distinguished name, username, external id and e-mail."""
    dn: str
    username: str
    external_id: str
    email: str | None = None
    display_name: str | None = None
    groups: list[str] = field(default_factory=list)  # CNs and DNs, de-duplicated


def have_ldap3() -> bool:
    try:
        import ldap3  # noqa: F401
    except ImportError:
        return False
    return True


def _first(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, (list, tuple)):
        return str(v[0]) if v else None
    s = str(v)
    return s or None


def _cn_of(dn: str) -> str | None:
    m = re.match(r"(?i)^\s*cn=((?:\\.|[^,\\])+)", dn)
    return m.group(1).replace("\\,", ",") if m else None


class LdapBridge:
    """Search-then-bind authenticator for one configured directory."""

    def __init__(self, cfg: LdapConfig, bind_password: str | None, *, connection_factory: Callable[..., Any] | None = None) -> None:
        if not have_ldap3() and connection_factory is None:
            raise Unsupported("the LDAP bridge needs the optional 'ldap3' package", remediation="pip install ldap3 (or front your directory with an OIDC bridge such as Dex/Keycloak)")
        self.cfg, self.bind_password = cfg, bind_password
        self._factory = connection_factory
        parts = urlsplit(cfg.server_uri)
        if parts.scheme not in ("ldap", "ldaps") or not parts.hostname:
            raise BadRequest("ldap.server_uri must be ldap://host[:port] or ldaps://host[:port]")
        if parts.scheme == "ldap" and not (cfg.start_tls or cfg.allow_insecure_ldap):
            raise BadRequest("plain ldap:// is refused", remediation="use ldaps://, enable start_tls, or (development only) allow_insecure_ldap")

    # ------------------------------------------------------------------ connection plumbing
    def _server(self) -> Any:
        from ldap3 import Server, Tls

        parts = urlsplit(self.cfg.server_uri)
        tls = Tls(validate=ssl.CERT_REQUIRED, ca_certs_file=self.cfg.ca_file) if (parts.scheme == "ldaps" or self.cfg.start_tls) else None
        return Server(self.cfg.server_uri, use_ssl=parts.scheme == "ldaps", tls=tls, connect_timeout=self.cfg.timeout_seconds, get_info="NONE")

    def _connect(self, user: str | None, password: str | None) -> Any:
        """Open and bind a connection. Raises ``Unauthenticated`` for bad credentials and ``Unavailable`` for transport errors."""
        try:
            if self._factory is not None:
                conn = self._factory(user, password)
            else:
                from ldap3 import SIMPLE, Connection

                conn = Connection(self._server(), user=user, password=password, authentication=SIMPLE if user else "ANONYMOUS", receive_timeout=self.cfg.timeout_seconds,
                                  raise_exceptions=False, auto_bind=False)
                conn.open()
                if self.cfg.start_tls and not conn.start_tls():
                    raise Unavailable("LDAP StartTLS negotiation failed")
            if not conn.bound and not conn.bind():
                raise Unauthenticated("invalid username or password")
            return conn
        except (Unauthenticated, Unavailable):
            raise
        except Exception as exc:  # noqa: BLE001 - ldap3 raises many types
            raise Unavailable(f"LDAP connection failed: {type(exc).__name__}") from exc

    # ------------------------------------------------------------------ public API
    def authenticate(self, username: str, password: str) -> LdapIdentity:
        username = (username or "").strip()
        if not username or not password:
            raise Unauthenticated("invalid username or password")  # also blocks unauthenticated (empty password) simple binds
        if len(username) > 256 or _CTRL.search(username) or len(password) > 1024:
            raise Unauthenticated("invalid username or password")
        from ldap3.utils.conv import escape_filter_chars

        svc = self._connect(self.cfg.bind_dn, self.bind_password if self.cfg.bind_dn else None)
        try:
            flt = self.cfg.user_filter.replace("{username}", escape_filter_chars(username))
            attrs = [a for a in {self.cfg.username_attr, self.cfg.email_attr, self.cfg.display_name_attr, self.cfg.member_of_attr, "entryUUID", "objectGUID"} if a]
            ok = svc.search(self.cfg.user_base_dn, flt, search_scope="SUBTREE", attributes=attrs, size_limit=2)
            entries = list(svc.entries) if ok else []
            if len(entries) != 1:
                # unknown user and ambiguous filter look identical to the caller (no user enumeration); the log distinguishes them
                log.info("ldap search for %r returned %d entries", username, len(entries))
                raise Unauthenticated("invalid username or password")
            entry = entries[0]
            dn = str(entry.entry_dn)
            ed = entry.entry_attributes_as_dict

            user = self._connect(dn, password)  # raises Unauthenticated on wrong password
            try:
                user.unbind()
            except Exception:  # noqa: BLE001 - the credential check already succeeded
                pass

            groups = self._groups(svc, dn, ed)
            uname = _first(ed.get(self.cfg.username_attr)) or username
            ext = _first(ed.get("entryUUID")) or _first(ed.get("objectGUID")) or dn
            return LdapIdentity(dn=dn, username=uname, external_id=ext, email=_first(ed.get(self.cfg.email_attr)),
                                display_name=_first(ed.get(self.cfg.display_name_attr)), groups=groups)
        finally:
            try:
                svc.unbind()
            except Exception:  # noqa: BLE001
                pass

    def _groups(self, svc: Any, user_dn: str, attrs: dict[str, Any]) -> list[str]:
        from ldap3.utils.conv import escape_filter_chars

        dns: list[str] = []
        if self.cfg.member_of_attr:
            dns.extend(str(g) for g in (attrs.get(self.cfg.member_of_attr) or []))
        if self.cfg.group_base_dn:
            esc = escape_filter_chars(user_dn)
            flt = f"(member:1.2.840.113556.1.4.1941:={esc})" if self.cfg.nested_groups else self.cfg.group_filter.replace("{user_dn}", esc)
            if svc.search(self.cfg.group_base_dn, flt, search_scope="SUBTREE", attributes=["cn"], size_limit=5000):
                dns.extend(str(e.entry_dn) for e in svc.entries)
        out: list[str] = []
        for dn in dict.fromkeys(dns):
            cn = _cn_of(dn)
            if cn:
                out.append(cn)
            out.append(dn)
        return list(dict.fromkeys(out))

    def test(self) -> list[dict[str, Any]]:
        """Connectivity steps for the provider test endpoint (service-account bind + a search)."""
        import time

        steps: list[dict[str, Any]] = []

        def step(name: str, fn: Callable[[], str]) -> bool:
            t0 = time.perf_counter()
            try:
                detail, ok = fn(), True
            except Exception as exc:  # noqa: BLE001
                detail, ok = f"{type(exc).__name__}: {getattr(exc, 'message', exc)}", False
            steps.append({"name": name, "ok": ok, "detail": detail, "ms": round((time.perf_counter() - t0) * 1000, 1)})
            return ok

        holder: dict[str, Any] = {}

        def bind() -> str:
            holder["c"] = self._connect(self.cfg.bind_dn, self.bind_password if self.cfg.bind_dn else None)
            return f"bound as {self.cfg.bind_dn or 'anonymous'}"

        def search() -> str:
            c = holder["c"]
            c.search(self.cfg.user_base_dn, "(objectClass=*)", search_scope="BASE", attributes=["1.1"], size_limit=1)
            return f"user base {self.cfg.user_base_dn} readable"

        if step("bind", bind) and step("search", search):
            try:
                holder["c"].unbind()
            except Exception:  # noqa: BLE001
                pass
        return steps
