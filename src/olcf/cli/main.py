# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""``olcf`` - command line interface of the Open Lakehouse Compute Fabric.

The CLI is a thin, scriptable layer over two things: the REST API (through :mod:`olcf.sdk`) and the local installation (data directory,
runtime manager, service files).  It is *rich-free* (plain ANSI, optional), never needs developer tools on macOS (see
:mod:`olcf.cli.hostsafe`) and keeps a stable contract for automation:

* ``-o json`` / ``--json`` on every command; messages go to stderr, data to stdout;
* documented exit codes (see ``olcf --help`` and :mod:`olcf.cli.state`);
* credentials only through the environment (``OLCF_TOKEN``), the per-user context file (mode 0600) or the OS keyring - never echoed.
"""

from __future__ import annotations

import os
import sys
from typing import Any

import click

from olcf.cli import (
    hostsafe,
    psfmt,  # noqa: F401  (registers PowerShell completion)
)
from olcf.cli.output import Output
from olcf.cli.state import State, report_exception
from olcf.core.errors import OlcfError
from olcf.sdk.errors import OlcfSdkError
from olcf.version import __version__

EXIT_CODE_HELP = """\b
Exit codes:
  0 success                      1 failure / doctor found a problem   2 usage error
  3 not authenticated (login)    4 permission denied                  5 not found
  6 conflict / precondition      7 request rejected (400/422)         8 server error or unreachable
  9 --wait timed out             10 awaited job did not succeed       11 prerequisite missing
  130 interrupted

Environment: OLCF_URL, OLCF_TOKEN, OLCF_CONTEXT, OLCF_WORKSPACE, OLCF_DATA_DIR, OLCF_OUTPUT, OLCF_CA_BUNDLE, NO_COLOR.
Docs: docs/guides/cli.md   Quick start: olcf quickstart"""

FORMATS = ("table", "json", "yaml", "csv")


class OlcfGroup(click.Group):
    """Maps SDK exceptions to the documented exit codes, for the real entry point and for ``CliRunner`` alike."""

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except OlcfSdkError as exc:
            st = ctx.find_object(State)
            raise SystemExit(report_exception(st.out if st else Output("table"), exc)) from None
        except KeyboardInterrupt:
            raise SystemExit(130) from None
        except (click.exceptions.ClickException, click.exceptions.Exit, click.exceptions.Abort):
            raise
        except OlcfError as exc:
            # local failures (runtime manager, services, config) carry a message and a remediation: show them like an API error, traceback only with -v
            st = ctx.find_object(State)
            if st is not None and st.verbose:
                raise
            (st.out if st else Output("table")).error(exc.message, exc.remediation)
            raise SystemExit(1) from None
        except Exception as exc:  # noqa: BLE001 - anything else is a bug or an environment problem: one clear line, the traceback with -v
            st = ctx.find_object(State)
            if st is None or st.verbose:
                raise
            st.out.error(f"{type(exc).__name__}: {exc}", "re-run with -v for the full traceback; `olcf doctor` checks the installation")
            raise SystemExit(1) from None


@click.group(cls=OlcfGroup, context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 110, "auto_envvar_prefix": None}, epilog=EXIT_CODE_HELP,
             help="Open Lakehouse Compute Fabric - run a lakehouse, Spark and a Fabric compute extension on your own machines.")
@click.version_option(__version__, "-V", "--version", prog_name="olcf", message="%(prog)s %(version)s")
@click.option("--context", "-c", envvar="OLCF_CONTEXT", default=None, help="Saved connection to use (see `olcf context list`).")
@click.option("--url", envvar="OLCF_URL", default=None, help="Server URL (default http://127.0.0.1:8470).")
@click.option("--token", envvar="OLCF_TOKEN", default=None, help="API token. Prefer the OLCF_TOKEN variable or `olcf login`; flags show up in process lists.")
@click.option("--data-dir", envvar="OLCF_DATA_DIR", type=click.Path(file_okay=False), default=None, help="OLCF data directory (default ~/.olcf).")
@click.option("-o", "--output", "output", envvar="OLCF_OUTPUT", type=click.Choice(FORMATS), default=None, help="Output format (default table).")
@click.option("--json", "as_json", is_flag=True, help="Shortcut for -o json.")
@click.option("--no-color", is_flag=True, help="Disable colours (also: NO_COLOR).")
@click.option("-q", "--quiet", is_flag=True, help="Only print results and errors.")
@click.option("-v", "--verbose", count=True, help="More detail (repeat for more).")
@click.option("--timeout", type=float, default=60.0, show_default=True, help="HTTP timeout in seconds.")
@click.option("--ca-bundle", envvar="OLCF_CA_BUNDLE", type=click.Path(dir_okay=False), default=None, help="PEM file with extra trusted CAs (private or enterprise CA).")
@click.pass_context
def cli(ctx: click.Context, context: str | None, url: str | None, token: str | None, data_dir: str | None, output: str | None, as_json: bool,
        no_color: bool, quiet: bool, verbose: int, timeout: float, ca_bundle: str | None) -> None:
    fmt = "json" if as_json else (output or "table")
    out = Output(fmt, color=False if no_color else None, quiet=quiet)
    ctx.obj = State(out=out, fmt=fmt, context=context, url=url, token=token, ca_bundle=ca_bundle, data_dir=data_dir, timeout=timeout, verbose=verbose)
    hostsafe.protect_environment()  # macOS without developer tools: keep the "install Xcode tools" dialog away from every child process


def _register() -> None:
    from olcf.cli.commands import (
        admin,
        auth,
        compute,
        governance,
        intelligence,
        jobs,
        lifecycle,
        resources,
        service,
    )

    for mod in (lifecycle, auth, resources, jobs, compute, admin, service, intelligence, governance):
        mod.register(cli)


_register()


def main(argv: list[str] | None = None) -> int:
    """Console-script entry point; returns the exit status."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]  # Windows consoles default to a legacy code page
        except (AttributeError, ValueError):
            pass
    try:
        cli.main(args=argv, prog_name="olcf", standalone_mode=False)
        return 0
    except click.exceptions.Exit as exc:
        return int(exc.exit_code)
    except click.ClickException as exc:
        exc.show()
        return int(exc.exit_code)
    except click.Abort:
        print("Aborted.", file=sys.stderr)
        return 130
    except SystemExit as exc:
        return int(exc.code) if isinstance(exc.code, int) else (0 if exc.code is None else 1)
    except KeyboardInterrupt:
        return 130
    except BrokenPipeError:
        try:
            sys.stdout = open(os.devnull, "w")  # noqa: SIM115 - avoid a second error at interpreter shutdown
        except OSError:
            pass
        return 0


if __name__ == "__main__":
    sys.exit(main())
