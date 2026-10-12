# Installing on a Mac without the Xcode Command Line Tools

OLCF was designed for a Mac on which the Xcode Command Line Tools (CLT) cannot be installed, for example because of a managed-device policy or an unsupported macOS version. This page explains the problem, what OLCF does about it, what you can and cannot do on such a Mac, and what to check when something goes wrong.

> **Test coverage.** The tests put dialog-opening stubs for `git`, `java`, `python3` and others on `PATH` (with a `uname` that reports macOS); each stub records a marker when executed, and the tests assert that no marker appears. The tests are `tests/unit/test_hostbin.py`, `tests/tooling/test_hostsafe.py` and `tests/tooling/test_installer.py::test_mac_without_developer_tools_never_runs_a_stub`.

## The problem

On a stock Mac without the CLT, `/usr/bin/git`, `/usr/bin/python3`, `/usr/bin/java`, `clang`, `make` and others are not real programs. They are small shims that open a graphical dialog ("The 'git' command requires the command line developer tools" or "No Java runtime present") and then fail. A program that merely *probes* `git --version` or `java -version` would freeze or spam the desktop. So OLCF must never run them, not even to see whether they work.

## What OLCF does

| Need | How OLCF gets it without the CLT |
|---|---|
| Python | The installer downloads one static `uv` binary (SHA-256 verified), and `uv` provides a private Python 3.13 (python-build-standalone) under the install prefix. `uv` is told never to use a system Python. |
| Compiling | Nothing is compiled. Packages are installed from wheels only (`--only-binary :all:`). |
| Java for Spark | `olcf runtime install --with-jdk` (the installer's default) downloads an Eclipse Temurin JDK from the Adoptium API and verifies its SHA-256 before extracting it into the data directory. It does that only if no Java 17 or 21 is found. |
| `git` | Not needed for anything except workspace Git sync (below). |

Every lookup of `git`, `python`, `pip` or `java` in the server, the worker, the runtime manager and the CLI goes through `olcf.core.hostbin.find_real_binary`. It treats a file as an installer stub when all of these hold: the platform is macOS, the file sits directly in `/usr/bin`, its name is one of the known shim names, and the tools behind it do not exist (`xcode-select -p` fails; for Java, `/usr/libexec/java_home` reports no JDK). It never executes a stub. `xcode-select -p` and `/usr/libexec/java_home` are the only programs it runs, because both are real and never open a dialog. When it rejects a stub it continues with the next `PATH` entry (Homebrew, python.org, SDKMAN, a JDK that OLCF downloaded).

Two further layers apply:

* **JDK discovery by reading, not running.** `olcf.cli.hostsafe.find_jdks` finds JDKs by reading each candidate's `release` file in the usual places (`$OLCF_JAVA_HOME`, `$JAVA_HOME`, the OLCF data directory, `/Library/Java/JavaVirtualMachines`, `~/Library/Java/JavaVirtualMachines`, Homebrew, SDKMAN).
* **Guard shims.** On macOS the `olcf` command prepends a per-user directory (`<config dir>/stubguard`, normally `~/.config/olcf/stubguard`) to `PATH` for itself and every process it starts, including the embedded worker. The directory holds harmless replacements for `git`, `java`, `javac`, `python3`, `python`, `pip3`, `cc`, `clang`, `gcc` and `make`. If one is called, it looks for a real tool further down `PATH` and otherwise prints a one-line explanation and exits 127, so code that still runs `subprocess.run(["python3", ...])` never reaches the dialog. The `java` shim forwards to a real JDK when one exists. If the CLT are installed, only `java` and `javac` are shadowed. Set `OLCF_NO_STUB_GUARD=1` to switch the guard off.

The installer scripts follow the same rule. `scripts/install.sh` needs only `sh`, `uname`, `curl` (or `wget`), `tar` and a SHA-256 tool, all of which ship with macOS. It prints `no Xcode Command Line Tools - none are needed` and carries on.

## Installing

```sh
curl -fsSL https://raw.githubusercontent.com/jddevtime/open-lakehouse-compute-fabric/main/scripts/install.sh | sh
# or from a checkout / release download:
sh scripts/install.sh --dry-run
sh scripts/install.sh --wheel dist/olcf-0.1.0-py3-none-any.whl
```

Then open a new terminal (the installer adds `<prefix>/bin` to your shell profile unless you pass `--no-modify-path`) and run `olcf quickstart`. The installer supports `arm64`/`aarch64` and `x86_64`; a shell running under Rosetta on Apple silicon gets the native arm64 builds.

What you get without Java or the Spark runtime: `olcf quickstart --no-runtime` (or a quickstart whose runtime download failed) leaves a working server, the sample workspace and notebooks that run on the built-in `olcf-host-python` runtime, which is the interpreter OLCF itself runs in, so it needs no JDK and no download. Only Spark notebooks, Spark SQL and Spark jobs wait (`olcf job status <id>` prints `waiting: [no_runtime] ...` with the fix) until `olcf runtime install --with-jdk` has succeeded.

If you cannot run shell scripts at all, use the Docker route: Docker Desktop does not need the CLT.

```sh
sh deploy/scripts/gen-env.sh            # uses only sh, openssl (or /dev/urandom, base64 and od)
docker compose -f deploy/docker-compose.yml --profile allinone up -d --build
```

## What does not work without a real `git`

Workspace Git sync (connect a workspace to a repository, commit, pull, promote through Git) runs the real `git` program and does not work without one. When none is found, every Git endpoint answers `503 git_unavailable` with remediation text, never a 500, and OLCF does not run `git` to find out. `GET /v1/workspaces/{ws}/git/capabilities` still works and reports `available: false` with the reason. There is no pure-Python fallback: it is not implemented.

Getting a real `git` without the CLT: `brew install git`, a graphical Git installer, or any distribution that puts `git` somewhere other than `/usr/bin`. OLCF picks it up automatically from `PATH`. Everything else in the product works without it, including workspace export and import as a ZIP archive.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| A dialog about developer tools or Java still appears | Something other than OLCF started a stub. Run `olcf doctor` and check which process; set `JAVA_HOME` to a real JDK or run `olcf runtime install --with-jdk`. Please report it as a bug with the output of `olcf doctor`: OLCF is meant never to trigger it. |
| `olcf doctor` says `jdk: no Java 17/21` | Run `olcf runtime install --with-jdk`. Behind a proxy, set `HTTPS_PROXY` and `SSL_CERT_FILE` first; the download goes to the Adoptium API. |
| `runtime install` cannot reach the network | Use the offline route: `python -m olcf.runtime export` on a connected machine and `import` on the Mac, see [runtimes](../concepts/compute-model.md#runtimes). |
| `git_unavailable` | Install a real `git` (see above) or work without Git sync. |
| `uv` download blocked | `sh scripts/install.sh --uv-bin /path/to/uv` or `OLCF_UV_BASE_URL=<mirror>`. |
| You installed the CLT later | A running process notices within 30 seconds (it caches the `xcode-select` answer that long); the guard shims shadow only `java`/`javac` from then on. |

If an `olcf` command hangs, press Ctrl-C and run it again with `-vv`.
