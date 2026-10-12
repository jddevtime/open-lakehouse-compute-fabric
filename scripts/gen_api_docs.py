#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Generate the REST API documentation: ``docs/reference/api.md`` and the published OpenAPI document ``docs/api/openapi.json``.

Both come from the running application object (``olcf.api.app.create_app``), the same document that the server serves at ``/v1/openapi.json``.
:mod:`gen_api_reference` renders the Markdown; this entry point additionally writes the raw, key-sorted OpenAPI JSON so that the contract is
available without starting a server (``docs/api/openapi.json``).

Usage::

    PYTHONPATH=src python scripts/gen_api_docs.py            # write both files
    PYTHONPATH=src python scripts/gen_api_docs.py --check    # exit 1 when either file is stale
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import gen_api_reference  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OPENAPI = ROOT / "docs" / "api" / "openapi.json"


def main(argv: list[str] | None = None) -> int:
    """Render ``api.md`` and write (or, with ``--check``, compare) ``openapi.json``."""
    args = sys.argv[1:] if argv is None else argv
    unknown = [a for a in args if a not in {"--check"}]
    if unknown:
        print(f"usage: gen_api_docs.py [--check]   (unknown: {' '.join(unknown)})", file=sys.stderr)
        return 2
    check = "--check" in args
    spec, perms = gen_api_reference.build_spec()
    rc = 0
    text = json.dumps(spec, indent=2, sort_keys=True) + "\n"
    if check:
        if not OPENAPI.exists() or OPENAPI.read_text(encoding="utf-8") != text:
            print(f"{OPENAPI.relative_to(ROOT)} is out of date; run scripts/gen_api_docs.py", file=sys.stderr)
            rc = 1
    else:
        OPENAPI.parent.mkdir(parents=True, exist_ok=True)
        OPENAPI.write_text(text, encoding="utf-8")
        print(f"wrote {OPENAPI.relative_to(ROOT)} ({len(text) // 1024} KiB, {len(spec.get('paths', {}))} paths)")
    md = gen_api_reference.render(spec, perms)
    if check:
        cur = gen_api_reference.OUT.read_text(encoding="utf-8") if gen_api_reference.OUT.exists() else ""
        if cur != md:
            print(f"{gen_api_reference.OUT.relative_to(ROOT)} is out of date; run scripts/gen_api_docs.py", file=sys.stderr)
            rc = 1
    else:
        gen_api_reference.OUT.write_text(md, encoding="utf-8")
        print(f"wrote {gen_api_reference.OUT.relative_to(ROOT)} ({len(md.splitlines())} lines)")
    return rc


if __name__ == "__main__":
    sys.exit(main())
