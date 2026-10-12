# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Generate (or check) the shipped JSON documents derived from the code.

``python -m olcf.pipelines.gen_schemas``          writes ``schemas/pipeline-v1.json`` (the pipeline definition schema with the built-in activity types) and
                                                  ``schemas/fabric-import-support.json`` (what ``POST /v1/pipelines/import-fabric`` supports);
``python -m olcf.pipelines.gen_schemas --check``  exits 1 when the files on disk differ from what the code generates (used by the tests and CI).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from olcf.pipelines import model as M
from olcf.pipelines.activities import build_registry
from olcf.pipelines.fabric_import import support_document


def documents() -> dict[str, dict[str, Any]]:
    """``{file name: document}`` generated from the built-in activity registry (entry-point plugins are deliberately excluded)."""
    return {"pipeline-v1.json": M.build_schema(build_registry(plugins=False)), "fabric-import-support.json": support_document()}


def render(doc: dict[str, Any]) -> str:
    return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"


def schemas_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "schemas"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="olcf.pipelines.gen_schemas", description=__doc__)
    ap.add_argument("--check", action="store_true", help="fail when the shipped files are out of date instead of writing them")
    ap.add_argument("--dir", type=Path, default=None, help="output directory (default: <repo>/schemas)")
    args = ap.parse_args(argv)
    out = args.dir or schemas_dir()
    stale: list[str] = []
    for name, doc in documents().items():
        text = render(doc)
        path = out / name
        current = path.read_text(encoding="utf-8") if path.is_file() else None
        if current == text:
            continue
        if args.check:
            stale.append(name)
        else:
            out.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
            print(f"wrote {path}")  # noqa: T201
    if stale:
        print("out of date: " + ", ".join(stale) + " (run: python -m olcf.pipelines.gen_schemas)", file=sys.stderr)  # noqa: T201
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
