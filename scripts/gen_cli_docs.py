#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Generate ``docs/reference/cli.md`` from the real command line tools.

The page is the ``--help`` output of every ``olcf`` command and sub-command (the click tree is walked, not parsed from text) and of the argparse
tools ``olcf-worker``, ``python -m olcf.runtime``, ``olcf upgrade``, ``python -m olcf.ops.backup`` and ``python -m olcf.mcp.stdio``.  The work is done
by :mod:`gen_reference` (section ``cli``); this entry point exists so the CLI reference has a command of its own.

Usage::

    PYTHONPATH=src python scripts/gen_cli_docs.py            # write docs/reference/cli.md
    PYTHONPATH=src python scripts/gen_cli_docs.py --check    # exit 1 when the page is stale
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gen_reference  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    """Run the ``cli`` section of the reference generator, forwarding ``--check``."""
    args = sys.argv[1:] if argv is None else argv
    unknown = [a for a in args if a not in {"--check"}]
    if unknown:
        print(f"usage: gen_cli_docs.py [--check]   (unknown: {' '.join(unknown)})", file=sys.stderr)
        return 2
    sys.argv = ["gen_reference.py", "--only", "cli", *args]
    return gen_reference.main()


if __name__ == "__main__":
    sys.exit(main())
