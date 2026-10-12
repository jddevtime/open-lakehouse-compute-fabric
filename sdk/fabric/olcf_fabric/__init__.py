# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""olcf-fabric: submit a Microsoft Fabric notebook to your on-prem Open Lakehouse Compute Fabric (Fabric Runtime 2.0 Compatible).

    import olcf_fabric as olcf
    olcf.configure(endpoint="https://olcf.example.com", auth="keyvault", keyvault="https://<vault>.vault.azure.net/", secret_name="olcf-token")
    olcf.test_connection()
    olcf.run_remote()          # runs this whole notebook on the on-prem fabric and returns its exit value

Standard library only (Python >= 3.10). This package is not a Microsoft product and is not endorsed by Microsoft.
"""

__version__ = "0.1.0"

from .api import RemoteRun, cancel, extend, finish, logs, run_remote, status, test_connection  # noqa: E402
from .config import configure  # noqa: E402
from .config import current as settings
from .errors import (  # noqa: E402
    AuthenticationError,
    CompatibilityError,
    ConfigurationError,
    ConnectionFailed,
    OlcfFabricError,
    RemoteExecutionError,
    ServerError,
    SubmissionRejected,
)

__all__ = [
    "AuthenticationError", "CompatibilityError", "ConfigurationError", "ConnectionFailed", "OlcfFabricError", "RemoteExecutionError", "RemoteRun", "ServerError",
    "SubmissionRejected", "__version__", "cancel", "configure", "extend", "finish", "logs", "run_remote", "settings", "status", "test_connection",
]
