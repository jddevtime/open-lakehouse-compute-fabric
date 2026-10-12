# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Optional sandbox hook (spec section 31 "Execution"): wrap every job command in an administrator-defined sandbox.

Notebook code is inherently privileged compute; OLCF therefore does not pretend to sandbox it by itself. Administrators may
configure ``sandbox_command`` (agent config / ``--sandbox-command``): a command *prefix* that is placed in front of the runner
command. Placeholders ``{workdir}``, ``{runtime_dir}``, ``{state_dir}``, ``{home}`` are substituted. The wrapped command must be
transparent for stdio and exit codes and must **not** cut the network between driver, executors and the master.

Examples (adapt the bind mounts to your distribution)::

    # Linux, bubblewrap: read-only system, private /tmp, only the job dir + runtime writable, network shared
    ["bwrap", "--ro-bind", "/usr", "/usr", "--ro-bind", "/lib", "/lib", "--ro-bind", "/lib64", "/lib64", "--ro-bind", "/etc", "/etc",
     "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--bind", "{workdir}", "{workdir}", "--ro-bind", "{runtime_dir}",
     "{runtime_dir}", "--share-net", "--die-with-parent", "--unshare-pid", "--unshare-ipc", "--"]

    # macOS: sandbox-exec with a profile file written by the administrator
    ["sandbox-exec", "-f", "/etc/olcf/job.sb", "-D", "WORKDIR={workdir}"]

    # container: the runtime directory is mounted read-only
    ["docker", "run", "--rm", "--network=host", "-v", "{workdir}:{workdir}", "-v", "{runtime_dir}:{runtime_dir}:ro", "-w", "{workdir}",
     "olcf-job-image"]

Fail closed: if the sandbox binary is not installed the job **fails** (it is never run unsandboxed).
"""

from __future__ import annotations

import shutil
from pathlib import Path

from olcf.worker.protocol import CommandError


def wrap_command(cmd: list[str], sandbox: list[str], *, workdir: Path, runtime_dir: Path, state_dir: Path, home: Path) -> list[str]:
    """Prefix ``cmd`` with the configured sandbox command (no-op when none is configured)."""
    if not sandbox:
        return cmd
    subs = {"workdir": str(workdir), "runtime_dir": str(runtime_dir), "state_dir": str(state_dir), "home": str(home)}
    prefix: list[str] = []
    for tok in sandbox:
        for k, v in subs.items():
            tok = tok.replace("{" + k + "}", v)
        prefix.append(tok)
    if shutil.which(prefix[0]) is None and not Path(prefix[0]).exists():
        raise CommandError("sandbox_unavailable", f"sandbox command {prefix[0]!r} is configured but not installed on this node",
                           remediation="Install the sandbox tool or clear sandbox_command in the agent configuration; jobs are never run unsandboxed when one is configured.")
    return [*prefix, *cmd]
