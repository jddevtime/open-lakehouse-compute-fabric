# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Container entrypoint stand-in used by :class:`tests.compute.fake_kube.FakeKube`: ``python kube_launcher.py [--ro DIR]... -m MODULE [args...]``.

A real kubelet mounts Secret volumes read-only whatever ``readOnly`` says, and the test sandbox runs as root (where ``chmod`` cannot keep a process out), so the
read-only property is enforced here with an audit hook: every write-intent ``open`` and every mutating ``os.*`` call below a ``--ro`` directory fails with ``EROFS``
- the error a container gets. Only the launched Python process is covered (child processes such as notebook kernels are not); that is enough for the runner contract.
"""

from __future__ import annotations

import errno
import os
import runpy
import sys

_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
_ONE_PATH = {"os.mkdir", "os.rmdir", "os.remove", "os.chmod", "os.chown", "os.truncate", "os.utime", "os.mkfifo", "os.mknod", "os.setxattr", "os.removexattr"}
_TWO_PATHS = {"os.rename", "os.link", "os.symlink"}


def main() -> None:
    argv = sys.argv[1:]
    ro: list[str] = []
    while argv and argv[0] == "--ro":
        ro.append(os.path.realpath(argv[1]))
        argv = argv[2:]
    if len(argv) < 2 or argv[0] != "-m":
        raise SystemExit("usage: kube_launcher.py [--ro DIR]... -m MODULE [args...]")
    module, rest = argv[1], argv[2:]
    exact = set(ro)
    prefixes = tuple(p.rstrip(os.sep) + os.sep for p in ro)

    def inside(path: object) -> bool:
        if path is None or isinstance(path, int):
            return False
        try:
            real = os.path.realpath(os.fsdecode(path))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return False
        return real in exact or real.startswith(prefixes)

    def hook(event: str, args: tuple[object, ...]) -> None:
        if event == "open":
            path, _mode, flags = args
            if isinstance(flags, int) and flags & _WRITE_FLAGS and inside(path):
                raise OSError(errno.EROFS, "Read-only file system", os.fsdecode(path))  # type: ignore[arg-type]
        elif event in _ONE_PATH:
            if inside(args[0]):
                raise OSError(errno.EROFS, "Read-only file system", os.fsdecode(args[0]))  # type: ignore[arg-type]
        elif event in _TWO_PATHS and (inside(args[0]) or inside(args[1])):
            raise OSError(errno.EROFS, "Read-only file system", os.fsdecode(args[1]))  # type: ignore[arg-type]

    if ro:
        sys.addaudithook(hook)
    sys.argv = [module, *rest]
    runpy.run_module(module, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
