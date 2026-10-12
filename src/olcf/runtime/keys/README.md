# Development signing key (NOT FOR PRODUCTION)

`olcf-dev.key` / `olcf-dev.pub` are an Ed25519 key pair generated for development and testing by
Justin Dodrill. The **private key is public** (it ships with the source), so any manifest signed with it
is only as trustworthy as the source tree itself.

Production: generate your own pair (`python -m olcf.runtime keygen --out /etc/olcf/trust/my-org`), put only the
`.pub` into a directory, and point `OLCF_RUNTIME_TRUST_DIR` at it. When that variable is set the development key is
no longer trusted. Re-sign the manifests with `python -m olcf.runtime sign --key my-org.key manifests/*.json`.
