# Single sign-on and identity providers

OLCF has local accounts (password, scrypt-hashed) and can sign people in through an external identity provider. Three provider types exist:

| Type | For | Notes |
|---|---|---|
| `oidc` | any OpenID Connect provider: Keycloak, Okta, Auth0, Dex, Authentik, AD FS | authorization code flow with PKCE, handled on the server |
| `entra` | Microsoft Entra ID | the `oidc` flow with the Entra issuer built from `tenant`, tenant allow-lists and service-principal tokens |
| `ldap` | Active Directory or OpenLDAP | built-in search-then-bind bridge; needs the optional `ldap3` package (`pip install 'olcf[ldap]'`) |

The model behind these (token formats, sessions, roles) is in the [security model](../concepts/security-model.md#authentication). The UI is **Security, Identity providers** (needs `identity.federation.manage`, held by PlatformAdmin and SecurityAdmin); the API is `/v1/identity/providers`.

**Test coverage.** The OIDC and Entra flows, token validation, role mapping and the LDAP bridge are tested against a local identity provider and `ldap3`'s in-memory directory (`tests/security/test_federation_flow.py`, `test_federation_attacks.py`, `test_ldap_bridge.py`; run for this guide: 70 passed, 1 skipped). The configuration below follows the code and each provider's documented behaviour; test with your provider before relying on it.

## How a sign-in works

1. The user opens the sign-in page, which lists enabled providers (`GET /v1/identity/oidc/providers`), and picks one. The browser goes to `GET /v1/identity/oidc/{name}/login`; the server creates a login request with a random `state`, `nonce` and PKCE challenge and sets a browser-binding cookie (HttpOnly, SameSite=Lax).
2. The provider authenticates the user and redirects to `{server.public_url}/v1/identity/oidc/{name}/callback`. **That exact URL is what you register at the provider as the redirect URI.** It is always built from `server.public_url`, never from the request's Host header, so set `server.public_url` correctly.
3. The server consumes the state once, exchanges the code with its client secret (kept in the secret store), validates the ID token (signature, issuer, audience, expiry, nonce, tenant, `at_hash`), creates or updates the user, and **re-computes the user's role bindings from the mapping rules**. The browser receives a one-time code in the URL fragment (never sent to servers), which the UI exchanges for OLCF access and refresh tokens.

Identities are keyed by the provider and the token's subject, never by name: a provider user called `admin` cannot become the local `admin`. Bindings granted by a provider are tagged with their source (shown as "Identity provider NAME" in **Roles and access**) and are replaced to equal what the provider says at every login; bindings granted by hand are never touched.

## Create a provider

In the UI choose **New provider**, or call the API:

```python
from olcf.sdk import Client
c = Client.from_context()
c.identity.create_provider(
    name="company-sso", type="oidc", display_name="Company SSO", enabled=False,
    issuer="https://idp.example.com/realms/main",
    client_id="olcf", client_secret="...",              # stored encrypted, never returned
    config={"scopes": ["openid", "profile", "email", "groups"],
            "role_mappings": [
                {"claim": "groups", "value": "olcf-admins", "role": "PlatformAdmin"},
                {"claim": "groups", "pattern": "analytics-.*", "role": "Contributor",
                 "scope_type": "workspace", "workspace": "Sample Retail"}]})
```

Create it disabled, test it, preview the mapping, then enable it. The name must be lower-case letters, digits and dashes (it appears in the sign-in URLs). A provider is validated offline when saved; the issuer must be `https` (an `http` issuer needs `config.allow_insecure_http`, for tests only).

### Microsoft Entra ID

```python
c.identity.create_provider(
    name="entra", type="entra", display_name="Microsoft", client_id="<application id>", client_secret="...",
    tenant="<tenant id or domain>",
    config={"role_mappings": [{"claim": "roles", "value": "OLCF.Admin", "role": "PlatformAdmin"}]})
```

The issuer becomes `https://login.microsoftonline.com/<tenant>/v2.0` (`config.authority_host` changes the authority for sovereign clouds). `tenant` may be a tenant id, a domain, `organizations` or `common`; the multi-tenant values require `config.allowed_tenants` (a list of tenant GUIDs), otherwise no tenant would be accepted. In Entra, register a web application, add the redirect URI from the section above, and prefer **app roles** (the `roles` claim) over groups: Entra omits the `groups` claim for users in more than about 200 groups. With `config.resolve_group_overage` the server can fetch the groups from Microsoft Graph at interactive login (the token response needs a Graph-capable access token); for bearer tokens overage cannot be resolved, groups count as empty, and `deny_on_overage` refuses such tokens.

### LDAP and Active Directory

The recommended route is to put an OIDC bridge (Dex, Keycloak, AD FS, Entra) in front of the directory, so OLCF never sees directory passwords. The built-in bridge exists for sites that cannot:

```python
c.identity.create_provider(
    name="corp-ad", type="ldap", display_name="Corporate directory", client_secret="<bind password>",
    config={"ldap": {
        "server_uri": "ldaps://dc.example.com:636",          # or ldap:// with "start_tls": true
        "ca_file": "/etc/ssl/corp-ca.pem",
        "bind_dn": "CN=olcf-svc,OU=Service,DC=example,DC=com",
        "user_base_dn": "OU=People,DC=example,DC=com",
        "nested_groups": True,
        "group_rules": [{"claim": "groups", "value": "OLCF-Admins", "role": "PlatformAdmin"}]}})
```

The bridge connects with TLS (certificates are always verified; plain `ldap://` without StartTLS is refused unless `allow_insecure_ldap` is set), binds as the service account, finds exactly one user with `user_filter` (the user name is escaped into the filter), re-binds as that user with the supplied password (an empty password is rejected), reads groups from `memberOf` or a group search, and applies `group_rules`. Defaults suit Active Directory (`sAMAccountName`, `memberOf`); for OpenLDAP set `user_filter`, `username_attr`, `group_base_dn` and `group_filter`. The sign-in call is `POST /v1/identity/ldap/{name}/login`.

## Mapping identities to roles

Mappings are rules evaluated on the validated claims at every login (and for bearer tokens when the claim set changes):

```json
{"claim": "groups", "value": "olcf-admins", "role": "PlatformAdmin"}
{"claim": "realm_access.roles", "pattern": "olcf-.*-analyst", "role": "Contributor",
 "scope_type": "workspace", "workspace": "Sample Retail"}
```

* `claim` is a dotted path into the claims. A list contributes every element; the `scp` or `scope` claim is split on spaces.
* Give exactly one of `value` (exact) or `pattern` (a regular expression that must match the whole value; length-capped).
* `scope_type` is `org` (default) or `workspace` with `workspace` (a name) or `scope_id`. A rule whose workspace does not exist is skipped and reported; it is never widened to organisation scope.
* **An administrator can only map roles they hold themselves.** A SecurityAdmin cannot map a group to PlatformAdmin; only a PlatformAdmin can.
* A user with no mapped role cannot sign in unless `allow_login_without_roles` is set (they would have no permissions). Other switches: `auto_provision` (create users on first login, default on), `allow_login` and `allow_bearer` (use the provider for interactive login, for bearer tokens, or both), `require_email_verified`, `session_ttl_seconds`, `allowed_return_origins` (extra origins the UI may return to), `subject_claim`, `username_claim`, `email_claim`, `groups_claim`, `roles_claim`.

**Preview before you enable** (the claims are those you expect the provider to send; nothing is stored). On the sample installation, with the provider from above:

```python
c.identity.preview_mapping("<provider id>", {"sub": "u1", "email": "a@x.org", "groups": ["olcf-admins", "analytics-emea"]})
# {"bindings": [{"role": "Contributor", "scope_type": "workspace", "scope_id": "wks_..."},
#               {"role": "PlatformAdmin", "scope_type": "org", "scope_id": ""}],
#  "skipped": [], "service_principal": None, "would_login": True}
```

**Test connectivity** (`c.identity.test_provider(id)`, or the Test button) fetches the discovery document and signing keys and records the result. Without Internet access it fails with the reason and no change is made; in the sample installation, which has no Internet access, it returned `ok: false` with `Unavailable: discovery document: ProxyError`. The `identity-providers` health check reports the last stored results (and a live test in its deep mode).

## Bearer tokens from the provider

An `oidc` or `entra` provider with `allow_bearer` (default on) also lets clients call the API with a token issued by the provider. The server validates it exactly like an ID token (issuer, signature, audience, tenant, expiry), maps it to the same principal and roles, and keeps nothing else. `allowed_audiences` limits the audiences; `allowed_algs` limits the signature algorithms (asymmetric only; `none` and symmetric algorithms are never accepted).

Service principals (Entra `idtyp=app` tokens, client-credentials grants) are mapped only through `service_principals` entries, `{"match": {"oid": ..., "azp": ...}, "roles": [...], "token_scopes": [...]}`; a service token with no matching entry is refused. Delegated scopes (`scp`) are translated to OLCF token scopes with `scope_map` (`read`, `write`, `mcp`, `fabric`, or `*`).

## Disabling local login

Once an external administrator exists, you can require SSO:

```bash
curl -s -H "Authorization: Bearer $OLCF_TOKEN" "$OLCF_URL/v1/identity/local-login"      # {"enabled": true, "can_disable": ..., "blockers": [...]}
curl -s -X PUT -H "Authorization: Bearer $OLCF_TOKEN" -H 'Content-Type: application/json' -d '{"enabled": false}' "$OLCF_URL/v1/identity/local-login"
```

The call is refused when it would lock the organisation out, that is when no active federated PlatformAdmin of an enabled provider exists. **Break-glass:** start the control plane with `OLCF_BREAKGLASS_LOCAL_LOGIN=1` to force local login back on, fix the provider, and unset the variable. API tokens (`olcf_pat_...`) keep working when local login is off.

## Operating notes

* Role bindings from a provider change only at the user's next login; to remove access immediately, disable the user (**Security, Accounts**) or revoke their sessions and tokens. A bearer token's roles follow its claims at each validation.
* The provider's client secret lives in the secret store ([security model](../concepts/security-model.md#secrets)); update it with `PATCH /v1/identity/providers/{id}` and `client_secret`. Provider changes are audited.
* Behind several replicas, provider state and login requests are in the database, so any replica can finish a login another replica began.
* Discovery documents and signing keys are cached per process and refresh themselves (`jwks_min_refresh_seconds`, default 15). Clock skew tolerance is `clock_skew_seconds` (60).
* Provider endpoints (issuer, discovery, JWKS, token endpoint, Graph) must be absolute `https` URLs without credentials; link-local, multicast, unspecified and cloud-metadata addresses are refused. Private addresses are allowed, so an on-premises Keycloak works without extra settings. Other outbound features follow the [SSRF rules](../operations/hardening.md#7-outbound-requests).
