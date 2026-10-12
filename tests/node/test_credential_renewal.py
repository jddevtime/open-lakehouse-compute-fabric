# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Agent-side credential renewal: the atomic swap of the machine certificate/key and the worker token, and recovery from a crash at any point of it
(CONTRACTS 4.2 "Credential renewal"). The exchange with a live controller is covered in ``tests/compute/test_real_agent.py`` and ``test_worker_credentials.py``."""

# ruff: noqa: S105, S106  - fixture tokens, not credentials

from __future__ import annotations

import ssl
from pathlib import Path

import pytest

from olcf.security.pki import PKI
from olcf.worker import identity
from olcf.worker.identity import EnrollmentError, Identity

WID = "wrk_swap"


@pytest.fixture()
def pki(tmp_path: Path) -> PKI:
    return PKI(tmp_path / "pki")


def _issue(pki: PKI) -> tuple[str, str]:
    """(cert_pem, key_pem) as the controller + agent would produce them."""
    csr, key = PKI.generate_csr("node", sans=["node"])
    cert, _serial = pki.sign_worker_csr(csr, WID)
    return cert, key


def _enrolled(tmp_path: Path, pki: PKI) -> tuple[Path, Identity, str, str]:
    sd = tmp_path / "state"
    cert, key = _issue(pki)
    ident = Identity(worker_id=WID, worker_token="olcf_wrk_old", channel_url="ws://x/v1/workers/ws", url="http://x", name="node", ca_pem=pki.ca_pem, client_cert_pem=cert)
    identity.save_identity(sd, ident, key_pem=key)
    return sd, ident, cert, key


def _loads(sd: Path) -> bool:
    try:
        ssl.create_default_context().load_cert_chain(str(sd / "client.crt"), str(sd / "client.key"))
        return True
    except (ssl.SSLError, OSError):
        return False


def test_apply_renewal_swaps_certificate_key_and_token_together(tmp_path, pki):
    sd, ident, old_cert, old_key = _enrolled(tmp_path, pki)
    old_serial = identity.cert_info(sd)["serial"]
    new_cert, new_key = _issue(pki)
    applied = identity.apply_renewal(sd, ident, {"cert_pem": new_cert, "worker_token": "olcf_wrk_new", "ca_pem": pki.ca_pem}, new_key)
    assert applied == ["certificate", "token"]
    assert identity.cert_info(sd)["serial"] != old_serial and _loads(sd)
    assert (sd / "client.crt").read_text() == new_cert and (sd / "client.key").read_text() == new_key
    assert identity.load_identity(sd).worker_token == "olcf_wrk_new" and ident.worker_token == "olcf_wrk_new"
    assert not list(sd.glob("*.next")) and not list(sd.glob("*.tmp"))
    assert (sd / "client.key").stat().st_mode & 0o077 == 0 and (sd / "identity.json").stat().st_mode & 0o077 == 0


def test_token_only_renewal_leaves_the_certificate_alone(tmp_path, pki):
    sd, ident, cert, key = _enrolled(tmp_path, pki)
    assert identity.apply_renewal(sd, ident, {"worker_token": "olcf_wrk_new"}, None) == ["token"]
    assert (sd / "client.crt").read_text() == cert and (sd / "client.key").read_text() == key
    assert identity.load_identity(sd).worker_token == "olcf_wrk_new"


def test_a_certificate_that_does_not_match_the_new_key_changes_nothing(tmp_path, pki):
    sd, ident, cert, key = _enrolled(tmp_path, pki)
    other_cert, _ = _issue(pki)
    _, wrong_key = _issue(pki)
    with pytest.raises(EnrollmentError, match="does not match"):
        identity.apply_renewal(sd, ident, {"cert_pem": other_cert, "worker_token": "olcf_wrk_new"}, wrong_key)
    assert (sd / "client.crt").read_text() == cert and (sd / "client.key").read_text() == key and _loads(sd)
    assert identity.load_identity(sd).worker_token == "olcf_wrk_old", "a failed renewal must not leave a half-applied identity"
    assert not list(sd.glob("*.next"))
    with pytest.raises(EnrollmentError):  # a certificate with no request made
        identity.apply_renewal(sd, ident, {"cert_pem": other_cert}, None)


def test_crash_after_staging_both_files_is_finished_at_the_next_start(tmp_path, pki):
    """The process dies after writing ``client.crt.next``/``client.key.next`` (and identity.json) but before the swap."""
    sd, ident, _old_cert, _old_key = _enrolled(tmp_path, pki)
    new_cert, new_key = _issue(pki)
    (sd / "client.crt.next").write_text(new_cert)
    (sd / "client.key.next").write_text(new_key)
    assert identity.recover_staged(sd) == "installed"
    assert (sd / "client.crt").read_text() == new_cert and _loads(sd) and not list(sd.glob("*.next"))
    assert identity.recover_staged(sd) is None


def test_crash_between_the_two_renames_is_finished(tmp_path, pki):
    """The key was swapped, the certificate was not: ``load_identity`` completes the swap instead of leaving a mismatched pair."""
    sd, _ident, _old_cert, _old_key = _enrolled(tmp_path, pki)
    new_cert, new_key = _issue(pki)
    (sd / "client.key").write_text(new_key)
    (sd / "client.crt.next").write_text(new_cert)
    assert not _loads(sd)  # the mismatch a crashed swap would leave
    ident = identity.load_identity(sd)
    assert ident is not None and _loads(sd) and (sd / "client.crt").read_text() == new_cert and not list(sd.glob("*.next"))


def test_a_staged_pair_that_does_not_load_is_discarded(tmp_path, pki):
    sd, _ident, cert, key = _enrolled(tmp_path, pki)
    new_cert, _ = _issue(pki)
    _, other_key = _issue(pki)
    (sd / "client.crt.next").write_text(new_cert)
    (sd / "client.key.next").write_text(other_key)
    assert identity.recover_staged(sd) == "discarded"
    assert (sd / "client.crt").read_text() == cert and (sd / "client.key").read_text() == key and _loads(sd)


def test_leaving_the_fleet_removes_staged_files_too(tmp_path, pki):
    sd, _ident, _c, _k = _enrolled(tmp_path, pki)
    (sd / "client.crt.next").write_text("x")
    identity.delete_identity(sd)
    assert not list(sd.glob("client.*")) and not (sd / "identity.json").exists()


def test_cert_info_reports_serial_and_expiry(tmp_path, pki):
    sd, _ident, _c, _k = _enrolled(tmp_path, pki)
    info = identity.cert_info(sd)
    assert info and info["not_after"].endswith("Z") and int(info["serial"], 16) > 0
    assert identity.cert_info(tmp_path / "nothing") is None
