#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Five minutes with the OLCF Python SDK: sign in, create a workspace and a lakehouse, load a CSV as a Delta table, query it, clean up.

    olcf quickstart                                  # starts a local server (first time: prints the administrator password once)
    export OLCF_URL=http://127.0.0.1:8470
    export OLCF_USERNAME=admin OLCF_PASSWORD=...     # or:  export OLCF_TOKEN=...  (olcf token create sdk)
    python examples/sdk_quickstart.py

Everything created here is removed again at the end (use --keep to look at it in the UI).  The SDK is synchronous (`olcf.sdk.Client`); the
asynchronous twin `olcf.sdk.AsyncClient` has the same methods, awaited.  SQL runs read-only on DuckDB; tables are created by loading files.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid

from olcf.sdk import ApiError, Client, ConflictError


def connect() -> Client:
    url = os.environ.get("OLCF_URL", "http://127.0.0.1:8470")
    if os.environ.get("OLCF_TOKEN"):
        return Client(url, token=os.environ["OLCF_TOKEN"])
    user, password = os.environ.get("OLCF_USERNAME"), os.environ.get("OLCF_PASSWORD")
    if not (user and password):
        sys.exit("set OLCF_TOKEN, or OLCF_USERNAME and OLCF_PASSWORD (see the header of this file)")
    client = Client(url)
    client.login(user, password)
    return client


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--keep", action="store_true", help="do not delete the demo workspace afterwards")
    args = ap.parse_args(argv)
    client = connect()
    me = client.identity.me()
    print(f"signed in as {me['principal']['name']} on {client.base_url}")

    name = f"sdk-quickstart-{uuid.uuid4().hex[:6]}"
    ws = client.workspaces.create(name, description="created by examples/sdk_quickstart.py")
    print(f"workspace: {ws['name']} ({ws['id']})")
    try:
        lh = client.lakehouses.create(ws["name"], "demo")
        print(f"lakehouse: {lh['name']} ({lh['id']})")

        # a query needs no data at all ...
        print("answer:", client.sql.execute("SELECT 6 * 7 AS answer")["rows"][0][0])

        # ... and a CSV in the lakehouse's Files area becomes a Delta table with one call
        csv = "n\n" + "\n".join(str(i) for i in range(1, 11)) + "\n"
        client.lakehouses.write_file(lh["id"], "Files/numbers.csv", csv.encode(), overwrite=True)
        table = client.lakehouses.create_table(lh["id"], "numbers", "Files/numbers.csv")
        print(f"table {table['name']}: version {table['version']}, {table['source_files']} source file(s)")

        total = client.sql.execute("SELECT sum(n) AS total FROM demo.numbers", lakehouse="demo", workspace_id=ws["id"])
        print("sum of 1..10:", total["rows"][0][0])
        print("tables:", [t["name"] for t in client.lakehouses.list_tables(lh["id"])])
    except ConflictError as exc:
        print("name already taken:", exc, file=sys.stderr)
        return 1
    except ApiError as exc:
        print(f"the server refused: {exc}", file=sys.stderr)
        return 1
    finally:
        if not args.keep:
            client.workspaces.delete(ws["id"])
            print("cleaned up")
    return 0


if __name__ == "__main__":
    sys.exit(main())
