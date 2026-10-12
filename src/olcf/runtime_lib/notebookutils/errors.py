# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Errors raised by the ``notebookutils`` shim (stdlib only; this code runs inside runtime venvs).

:class:`NotSupportedError` is raised for every Fabric ``notebookutils`` / ``mssparkutils`` API the shim does not implement. It is never a silent no-op: the
message names the API, says why, and tells the author what to do. It subclasses :class:`AttributeError` so that ``hasattr(notebookutils.fs, "mount")``
is simply ``False`` (feature detection keeps working) while a direct call fails loudly. The message always contains ``not supported in OLCF`` and the
class carries ``olcf_category = "compatibility"`` so the failure is classified as a compatibility failure, not as a bug in user code.
"""

from __future__ import annotations


class NotSupportedError(AttributeError):
    """A Fabric-only ``notebookutils`` API that OLCF does not provide."""

    olcf_category = "compatibility"

    def __init__(self, api: str, reason: str = "", remediation: str = "") -> None:
        self.api = api
        self.reason = reason
        self.olcf_remediation = remediation or "Replace it with a supported API (notebookutils.help() lists them) or run the notebook in Fabric."
        text = f"{api} is not supported in OLCF"
        if reason:
            text += f": {reason.rstrip('.')}"
        text += f". {self.olcf_remediation}"
        super().__init__(text)


class LakehouseNotAttached(NotSupportedError, ValueError):
    """A ``/lakehouse/<name>/...`` path that names a lakehouse which is not attached to this run.

    It is a :class:`NotSupportedError` (actionable Fabric-style remediation) **and** a :class:`ValueError`, which is what the
    ``olcf.runtime_lib.paths`` contract promises for "path cannot be resolved" - callers catching either keep working.
    """


class SecretNotGranted(PermissionError):
    """``notebookutils.credentials.getSecret`` for a secret that was not explicitly granted to this job."""

    olcf_category = "authz"
    olcf_remediation = "Grant the secret to the capsule (olcf.run_remote(secrets=...)) and map it in the Fabric integration settings."


class LakehouseNotFound(LookupError):
    """``notebookutils.lakehouse`` asked for a lakehouse that is not attached to this job."""

    olcf_category = "user-code"
