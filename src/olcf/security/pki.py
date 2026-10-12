# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Built-in private CA used for control-plane TLS and worker machine identities (mTLS).

* ``ca.key`` / ``ca.crt``: EC P-256 self-signed CA (10 years), key stored 0600 in ``<data_dir>/pki``.
* Server certificate: issued for the configured host names / IPs, re-issued automatically when the SAN
  list changes or <30 days remain.
* Worker certificate: issued from a worker-generated CSR at enrollment (the private key never leaves the node).
* Revocation: serial numbers in the ``kv`` table (``pki.revoked``); the API layer checks them on the worker channel.

Production deployments may instead supply their own certificate (``tls.cert_file`` / ``tls.key_file``) and
trust bundle; nothing else in the platform depends on the built-in CA.
"""

from __future__ import annotations

import datetime as dt
import ipaddress
import os
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def _write_private(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)


class PKI:
    """The built-in certificate authority. It creates the CA, issues the server certificate and signs worker certificate requests, and verifies client certificates; files live in the PKI directory.
    """
    def __init__(self, directory: Path, org: str = "Open Lakehouse Compute Fabric") -> None:
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.org = org
        self.ca_key_path, self.ca_cert_path = self.dir / "ca.key", self.dir / "ca.crt"
        self._ensure_ca()

    # ------------------------------------------------------------------ CA
    def _ensure_ca(self) -> None:
        if self.ca_key_path.exists() and self.ca_cert_path.exists():
            self.ca_key = serialization.load_pem_private_key(self.ca_key_path.read_bytes(), password=None)
            self.ca_cert = x509.load_pem_x509_certificate(self.ca_cert_path.read_bytes())
            return
        key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, self.org), x509.NameAttribute(NameOID.COMMON_NAME, "OLCF Root CA")])
        now = dt.datetime.now(dt.UTC)
        cert = (
            x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(minutes=5))
            .not_valid_after(now + dt.timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True, content_commitment=False,
                                         key_encipherment=False, data_encipherment=False, key_agreement=False,
                                         encipher_only=False, decipher_only=False), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .sign(key, hashes.SHA256())
        )
        _write_private(self.ca_key_path, key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
        self.ca_cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        self.ca_key, self.ca_cert = key, cert

    @property
    def ca_pem(self) -> str:
        return self.ca_cert.public_bytes(serialization.Encoding.PEM).decode()

    def ca_fingerprint(self) -> str:
        return self.ca_cert.fingerprint(hashes.SHA256()).hex()

    # ------------------------------------------------------------------ issuing
    @staticmethod
    def _sans(names: list[str]) -> x509.SubjectAlternativeName:
        out: list[x509.GeneralName] = []
        for n in dict.fromkeys(names):
            try:
                out.append(x509.IPAddress(ipaddress.ip_address(n)))
            except ValueError:
                out.append(x509.DNSName(n))
        return x509.SubjectAlternativeName(out)

    def _issue(self, subject: x509.Name, public_key, *, days: float, server: bool, sans: list[str] | None = None, client: bool = False) -> x509.Certificate:
        now = dt.datetime.now(dt.UTC)
        b = (
            x509.CertificateBuilder().subject_name(subject).issuer_name(self.ca_cert.subject).public_key(public_key)
            .serial_number(x509.random_serial_number()).not_valid_before(now - dt.timedelta(minutes=5))
            .not_valid_after(now + dt.timedelta(days=days))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(self.ca_key.public_key()), critical=False)
        )
        usages = []
        if server:
            usages.append(ExtendedKeyUsageOID.SERVER_AUTH)
        if client:
            usages.append(ExtendedKeyUsageOID.CLIENT_AUTH)
        b = b.add_extension(x509.ExtendedKeyUsage(usages), critical=False)
        if sans:
            b = b.add_extension(self._sans(sans), critical=False)
        return b.sign(self.ca_key, hashes.SHA256())

    def issue_server_cert(self, sans: list[str], *, days: int = 365) -> tuple[str, str]:
        """Return (cert_pem, key_pem). Always includes localhost/127.0.0.1."""
        names = list(dict.fromkeys([*sans, "localhost", "127.0.0.1", "::1"]))
        key = ec.generate_private_key(ec.SECP256R1())
        subject = x509.Name([x509.NameAttribute(NameOID.ORGANIZATION_NAME, self.org), x509.NameAttribute(NameOID.COMMON_NAME, names[0])])
        cert = self._issue(subject, key.public_key(), days=days, server=True, client=False, sans=names)
        return (
            cert.public_bytes(serialization.Encoding.PEM).decode(),
            key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode(),
        )

    def ensure_server_cert(self, sans: list[str]) -> tuple[Path, Path]:
        """Create/refresh ``server.crt``/``server.key`` for ``sans``; return their paths."""
        crt, key = self.dir / "server.crt", self.dir / "server.key"
        want = set(sans) | {"localhost", "127.0.0.1", "::1"}
        if crt.exists() and key.exists():
            try:
                cert = x509.load_pem_x509_certificate(crt.read_bytes())
                ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
                have = {str(v) for v in ext.get_values_for_type(x509.DNSName)} | {str(v) for v in ext.get_values_for_type(x509.IPAddress)}
                remaining = cert.not_valid_after_utc - dt.datetime.now(dt.UTC)
                if want <= have and remaining > dt.timedelta(days=30):
                    return crt, key
            except Exception:  # noqa: BLE001 - re-issue on any parse problem
                pass
        cert_pem, key_pem = self.issue_server_cert(sorted(want))
        crt.write_text(cert_pem)
        _write_private(key, key_pem.encode())
        return crt, key

    @staticmethod
    def generate_csr(common_name: str, sans: list[str] | None = None) -> tuple[str, str]:
        """Worker side: create a key and CSR. Returns (csr_pem, key_pem)."""
        key = ec.generate_private_key(ec.SECP256R1())
        b = x509.CertificateSigningRequestBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        if sans:
            b = b.add_extension(PKI._sans(sans), critical=False)
        csr = b.sign(key, hashes.SHA256())
        return (
            csr.public_bytes(serialization.Encoding.PEM).decode(),
            key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode(),
        )

    def sign_worker_csr(self, csr_pem: str, worker_id: str, *, days: float = 90) -> tuple[str, str]:
        """Controller side: sign a CSR, forcing the subject to ``CN=<worker_id>``. Returns (cert_pem, serial_hex)."""
        csr = x509.load_pem_x509_csr(csr_pem.encode())
        if not csr.is_signature_valid:
            raise ValueError("CSR signature invalid")
        subject = x509.Name([x509.NameAttribute(NameOID.ORGANIZATIONAL_UNIT_NAME, "olcf-worker"), x509.NameAttribute(NameOID.COMMON_NAME, worker_id)])
        cert = self._issue(subject, csr.public_key(), days=days, server=False, client=True)
        return cert.public_bytes(serialization.Encoding.PEM).decode(), format(cert.serial_number, "x")

    def verify_client_cert(self, cert_pem: str) -> tuple[bool, str]:
        """Verify a PEM client certificate chains to this CA and is in date. Returns (ok, worker_id_or_reason)."""
        try:
            cert = x509.load_pem_x509_certificate(cert_pem.encode())
            self.ca_cert.public_key().verify(cert.signature, cert.tbs_certificate_bytes, ec.ECDSA(cert.signature_hash_algorithm))
            now = dt.datetime.now(dt.UTC)
            if not (cert.not_valid_before_utc <= now <= cert.not_valid_after_utc):
                return False, "certificate outside validity period"
            cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
            return True, str(cn)
        except Exception as exc:  # noqa: BLE001
            return False, f"{type(exc).__name__}: {exc}"
