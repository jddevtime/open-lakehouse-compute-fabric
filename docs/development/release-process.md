# Release process

A release is a git tag `vX.Y.Z` on the default branch. Everything downstream (gates, build, checks, signatures, images, a **draft** GitHub release) is done by `.github/workflows/release.yml`; publishing the reviewed draft by hand is the last step, and it is what triggers the optional PyPI upload (`.github/workflows/pypi.yml`). This page lists what the maintainer does by hand and what the automation does, so that a release can be reproduced and audited. What must still be done and verified before each kind of release or claim is the checklist in [release readiness](release-readiness.md) (R01 to R14).

The workflows and the build script were read and their individual steps run where possible in the project's sandbox (`actionlint` passes on every workflow file); the CI, container and security workflows now run on every push to `main`, and the release workflow runs for the first time with the first tag. Permissions, runner behaviour, action availability and nested reusable-workflow behaviour cannot be validated by syntax checks. Treat the first release as a rehearsal: start with `workflow_dispatch` (its `dry_run` input defaults to true; it runs the gates and the build job and creates, signs and pushes nothing).

A release cannot be cut until the repository identity is resolved: `scripts/check_release_config.py` (run by the build with `--require-identity`) fails while owner, repository or contact placeholders remain in the public files. That is R01 in the readiness checklist.

## Version numbers

OLCF follows [Semantic Versioning](https://semver.org/). Until 1.0.0, a minor version may break compatibility; the break is listed under *Changed* or *Removed* in [CHANGELOG.md](../../CHANGELOG.md).

The version appears in several places that must agree. The build script and `scripts/verify_dist.py` fail the build when `pyproject.toml`, `src/olcf/version.py` and the Fabric SDK version differ, and the release workflow fails when the tag differs from `pyproject.toml`. The others are your responsibility:

| Place | Checked by |
|---|---|
| `pyproject.toml` `version` | build script, workflow, `verify_dist.py` |
| `src/olcf/version.py` `__version__` | build script, `verify_dist.py` |
| `sdk/fabric/olcf_fabric/__init__.py` `__version__` (the `olcf-fabric` package, versioned together with the server) | build script (`SDK version`), `verify_dist.py`, `scripts/build_fabric_sdk.sh`, `tests/fabric/test_sdk_packaging.py` |
| `deploy/helm/olcf/Chart.yaml` `version` and `appVersion` | the workflow packages the chart with `--version` and `--app-version` set to the package version, so the file may lag, but keep it equal |
| `CITATION.cff` `version` and `date-released` | nobody; update by hand |
| `CHANGELOG.md` | rename *Unreleased* to the new version and date |

Other contract versions are independent of the release number and change only when the contract changes: the REST prefix (`API_VERSION = "v1"`), the worker protocol (`WORKER_PROTOCOL_VERSION = "1.0"`), the capsule schema (`CAPSULE_SCHEMA_VERSION = "1.0"`), the analyzer ruleset (`COMPAT_RULESET_VERSION`), the pipeline document (`olcf.pipeline/1`), the activity plug-in API (`ACTIVITY_API_VERSION = "1"`), and the backup format (`OLCFBAK1`). Anything that persists data or crosses a process boundary has to be versioned, and old versions must either keep working or be rejected explicitly.

## Checklist for a maintainer

1. Make sure the default branch is green: lint, types, tests, Spark, browser, installer and docs jobs.
2. Regenerate everything generated and commit it: the references (`gen_reference.py`, `gen_api_docs.py`, `gen_sdk_docs.py`, `gen_cli_docs.py`, `gen_api_reference.py`, see [setup](setup.md#generated-documentation)), runtime manifests if dependencies changed (below), `THIRD_PARTY_NOTICES.md` (`scripts/build_release.sh --update-notices`).
3. Update the versions in the table above and the changelog. Add migration notes if the database schema changed (`olcf db status` shows the streams).
4. Run the build locally: `bash scripts/build_release.sh --out /tmp/olcf-release` into a **new or empty** directory (a non-empty one is refused, never cleaned). It needs `uv` and network access. Without `--require-identity` an unresolved repository identity is only a warning.
5. Try the upgrade path from the previous release on a copy of real data: `olcf upgrade --wheel ...` and `olcf upgrade --rollback` ([runbook](../operations/runbook.md)). The project's CI cannot do this for you.
6. Merge, then tag and push: `git tag -s vX.Y.Z -m "OLCF X.Y.Z" && git push origin vX.Y.Z`. Every `0.x` tag and every tag with a hyphen (`v0.2.0-rc.1`) is a GitHub *prerelease*, and neither moves the `latest` image tag; only a stable tag from 1.0.0 on does.
7. Watch the workflow. Approve the `release` environment when you want the versioned container images pushed (see the caveat below). When it finishes, a **draft** release exists: verify a download (below) from a clean machine, run the install command in the release notes (it installs the exact wheel of that tag), and read the notes.
8. Publish the draft by hand. If PyPI publication is enabled, that triggers `pypi.yml`. Announce, and move the open items of the release into the next *Unreleased* section.

## What `scripts/build_release.sh` does

```sh
bash scripts/build_release.sh [--out DIR] [--extras LIST] [--no-sbom] [--no-install-test] [--update-notices] [--require-identity]
```

Run it with `bash` (it is a bash script; the workflows invoke it that way). `DIR` must not exist or must be empty: a directory that already holds files is an error and is left untouched, because the script used to delete it.

| Step | What happens | Fails when |
|---|---|---|
| 0. output and identity | refuses a non-empty `DIR`; runs `scripts/check_release_config.py` (owner, repository and contact placeholders in the public files) | `DIR` is not empty; the identity check fails **and** `--require-identity` was given (otherwise only a warning, so local development builds work) |
| 1. versions | reads `pyproject.toml`, `olcf/version.py` and `sdk/fabric/olcf_fabric/__init__.py`; when run from a tag build, also compares the tag; checks that `sdk/fabric` has its `LICENSE` and `NOTICE` | versions differ, `README.md` is missing (it is the package readme), the SDK license files are missing |
| 2. build | `uv build` produces the server wheel and sdist, then the Fabric SDK (`sdk/fabric`) wheel and sdist, all in `DIR`, with reproducible timestamps from `SOURCE_DATE_EPOCH` or the last commit | any of the four files is missing |
| 3. gate | `scripts/verify_dist.py DIR --require-sdk` inspects all four artifacts (rules below) | any check fails |
| 4. install test, SBOMs, notices | installs the server wheel (wheels only, extras `all` by default) into a throw-away virtual environment, checks that `olcf version` reports the package version and runs `olcf init`; installs the SDK wheel into a second, empty environment and checks that `olcf_fabric` imports without the server; writes CycloneDX SBOMs `olcf-<version>.cdx.json` and `olcf_fabric-<version>.cdx.json` with `cyclonedx-py`; writes `THIRD_PARTY_NOTICES.md` with `scripts/licenses.py` | an install or an SBOM fails, a dependency has no identifiable license or a strong-copyleft one |
| 5. checksums | writes `SHA256SUMS` over all files and re-verifies it | a mismatch |

`--no-install-test` and `--no-sbom` exist for offline machines; do not use them for a release.

License identification in `scripts/licenses.py`: the SPDX `License-Expression`, then the legacy `License` field, then the `License ::` classifiers, then a small curated table. A placeholder `License` value (`UNKNOWN`, `NONE`, `NOASSERTION`) counts as missing, so a package such as ptyprocess (`License: UNKNOWN` plus the ISC classifier) is identified as ISC; a package that nothing identifies still fails the notices step.

### Source archive versus installed wheel

`scripts/verify_dist.py` applies two sets of rules, because a source archive is meant to be complete and an installed wheel is not:

| | Wheel (`olcf`, `olcf-fabric`) | Source archive (sdist) |
|---|---|---|
| `tests/`, `.github/` | refused | allowed |
| `deploy/.env.example` | refused | allowed, **exactly that path** (`deploy/.env.example.bak`, `deploy/docker/.env.example` and every other env file are refused) |
| private keys, `.pem`, `.key`, `.p12` | refused, except the documented public development key `src/olcf/runtime/keys/olcf-dev.key` | refused, same exception |
| real `.env` files, databases (`.db`, `.sqlite`), bytecode, tool caches (`.hypothesis`, `.pytest_cache`, `.mypy_cache`), virtual environments, `.git`, `.DS_Store` | refused | refused |
| license files | `LICENSE` and `NOTICE` in `.dist-info/licenses/` | `LICENSE`, `NOTICE`, `README.md`, `PKG-INFO` at the top |

The Fabric SDK wheel has its own gate: the same version as the server, `Requires-Python >=3.10`, Apache-2.0 metadata with `License-File: LICENSE` and `NOTICE`, **no** `Requires-Dist` (it is standard library only), exactly the `olcf_fabric` modules (and nothing of the server), an SPDX header on every module and no import of `olcf`. The SDK source archive is checked with the source-archive rules plus its required files. A source archive built from a working copy contains whatever is not git-ignored, so keep caches such as `.hypothesis/` out of the tree (`.gitignore` does); the gate refuses them rather than shipping them.

## What the release workflow does

Triggered by a tag matching `vX.Y.Z` or `vX.Y.Z-*`, or by hand (`workflow_dispatch`, `dry_run` true by default).

| Job | Does | Needs |
|---|---|---|
| `verify`, `containers`, `security` | **call** `ci.yml`, `container.yml` and `security.yml` as reusable workflows (`uses: ./.github/workflows/...`; each also keeps its own push/pull-request triggers and has `workflow_call`). The release stops if any of them fails | |
| `build` | checks that the tag equals the package version; runs `bash scripts/build_release.sh --out dist --require-identity`; copies the installers (`install.sh`, `install.ps1`, `uninstall.*`, `make_offline_bundle.sh`) and the packaged Helm chart next to the artifacts; regenerates `SHA256SUMS`; uploads everything as the `release-dist` artifact | `verify`, `containers`, `security` |
| `sign` | tags only. Signs every artifact **keylessly** with `cosign sign-blob`, producing `<file>.sigstore.json` bundles tied to the workflow's OIDC identity and the public transparency log; creates a build provenance attestation for the wheels and sdists (the subjects are two separate newline-separated patterns, `dist/*.whl` and `dist/*.tar.gz`) | `build` |
| `images` | tags only, in the protected environment `release`. Builds `deploy/Dockerfile` (`olcf`) and `deploy/Dockerfile.worker` (`olcf-worker`) for `linux/amd64` and `linux/arm64`, pushes them to `ghcr.io/jddevtime/` (lower-cased) with an SBOM and `provenance: mode=max`, tags `X.Y.Z` and `X.Y`, and signs each image by digest. `latest` is created **only** by the explicit rule for stable tags (`latest=false` in the metadata action's flavor): never for `0.x`, never for a tag with a hyphen | `build` |
| `release` | tags only. Writes release notes (the beta statement and its limitations, and the install command for the exact wheel and SDK wheel of the tag) and creates a **draft** GitHub release with every file in `dist/` and generated notes. It is marked prerelease for every `0.x` tag and every tag with a hyphen | `build`, `sign`, `images` |

Reusable-workflow details: a called workflow's `github.workflow` is the caller's name, so the three gates put it in their concurrency group (`ci-gate-<caller>-<ref>`, ...) and only cancel superseded runs of their own stand-alone trigger; the release's group is distinct from all of them, so the caller can neither cancel nor be blocked by a callee. The `security` call grants `security-events: write` because CodeQL uploads results; a caller can only narrow, never widen, the permissions of the workflow it calls.

**Caveat: images are pushed before the draft is reviewed.** A tag push still builds and pushes the versioned GHCR images (`X.Y.Z`, `X.Y`) as soon as the `images` job runs; the draft is a gate for the GitHub release and for PyPI, not for GHCR. The `release` environment is the gate for images: add required reviewers to it (Settings > Environments) and nothing is pushed until a maintainer approves. A `workflow_dispatch` dry run skips signing, images and the release altogether. Without reviewers the environment does not block.

### PyPI (`.github/workflows/pypi.yml`)

Separate from the release workflow, triggered by `release: published`, that is, when a maintainer publishes the reviewed draft. It runs only when the repository variable `PUBLISH_PYPI` is `true`, in the protected `pypi` environment. It does not rebuild: it downloads the wheels and source archives attached to the release, verifies that exactly the four expected files (`olcf` and `olcf-fabric`, wheel and sdist each) are present and match `SHA256SUMS`, and uploads each project separately with trusted publishing (`pypa/gh-action-pypi-publish`, OIDC, no stored token). Before enabling it, create both projects on PyPI and register `pypi.yml` + the `pypi` environment as the trusted publisher of each, and decide the version convention: GitHub's prerelease flag does not make PyPI `0.1.0` a pre-release, and a PyPI file can never be replaced (readiness R02). Until then, the install commands in the release notes use the exact release wheel URL, not a package name.

No long-lived secret is stored: signing uses the workflow's OIDC token, the registry login uses `GITHUB_TOKEN`, and PyPI uses trusted publishing. The one-time repository settings are listed at the top of `release.yml`.

The gate workflows: `ci.yml` (on pushes and pull requests), `container.yml` (builds and smoke-tests the images on changes under `deploy/` or `src/`; it never pushes), `security.yml` (pip-audit with `--disable-pip --no-deps` on the already-resolved requirements of each platform, OSV-Scanner, CodeQL, and gitleaks; on every pull request and weekly). All three are also called by the release. `dependabot.yml` does weekly updates for pip, GitHub Actions and Docker.

Secret scanning uses `.gitleaks.toml`: the default rules plus an allowlist for generated bytecode and the exact material of the public development signing key. Tests and source are scanned; synthetic credentials in test fixtures carry a per-line `# gitleaks:allow` comment. The history scan is informational until the published history is clean.

## Verifying a release

```sh
sha256sum -c SHA256SUMS
cosign verify-blob --bundle olcf-X.Y.Z-py3-none-any.whl.sigstore.json \
  --certificate-identity-regexp '^https://github.com/jddevtime/open-lakehouse-compute-fabric/\.github/workflows/release\.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  olcf-X.Y.Z-py3-none-any.whl
```

`olcf_fabric-X.Y.Z-py3-none-any.whl` and the two source archives verify the same way. `gh attestation verify olcf-X.Y.Z-py3-none-any.whl --repo jddevtime/open-lakehouse-compute-fabric` checks the provenance. For an image: `cosign verify ghcr.io/jddevtime/olcf:X.Y.Z` with the same identity and issuer. The `cosign verify-blob` command is the one written into the workflow header; none of these commands has been run, because no release has been published. Replace `jddevtime/open-lakehouse-compute-fabric` with the real repository once it exists (readiness R01).

## Runtime manifests

A Spark runtime is described by a signed, hash-pinned manifest in `src/olcf/runtime/manifests/` (`olcf-rt-2.0.0`, `olcf-rt-1.3.0`, `olcf-rt-native-1.0.0`). Regenerate them when a Python dependency or a Spark version changes:

```sh
python scripts/lock_runtime.py --all                    # signs with the development key
python scripts/lock_runtime.py olcf-rt-2.0.0 --key my-org.key --out /tmp/manifests
```

`lock_runtime.py` resolves the top-level requirements with `uv` for all supported platforms, looks up the SHA-256 of every installable file on PyPI, and signs the manifest with Ed25519. It needs the network. Commit the JSON: the diff is the audit trail of every dependency change. Maven jars are not hashed here; their hashes are recorded on each node at first resolution.

The manifests shipped in the repository are signed with a **development key whose private half is in the source tree** (`src/olcf/runtime/keys/`, with a README stating so). This is deliberate for the open-source distribution, and it means those signatures prove integrity of the files against accident, not authenticity against an attacker with write access to the repository. Since the review remediation a production installation (`environment` unset or `production`) **does not trust the development key**, so a downloaded release cannot install the shipped runtimes in production as it is: the operator generates their own key, re-signs the manifests and points `runtime.trust.dir` / `OLCF_RUNTIME_TRUST_DIR` at the public key ([hardening](../operations/hardening.md#6-supply-chain)). Release notes must say so, and a project that publishes its own runtimes must sign them with a key that is never committed and ship its public key for the trust directory. The `runtime-trust` health check and the changelog entry for the default are the places users will look. A release gate that is **not** automated: install a runtime from the built wheel with `environment = "production"` and your trust directory on a clean machine (with Docker, bake an operator-signed manifest: `--build-arg RUNTIME_TRUST_DIR=<dir> --build-arg BAKE_RUNTIME=<manifest>.json`, see the [deploy README](../../deploy/README.md#2-docker-compose); with Helm set `runtimeTrust.existingConfigMap`; neither has been run on a real Docker daemon or cluster).

## Update manifests

The control plane's update service (`/v1/admin/updates`, see the [runbook](../operations/runbook.md#updates)) accepts only manifests signed by a key the operator trusts. Create keys and sign a payload with:

```sh
python -m olcf.ops.manifest keygen                         # prints a key pair as JSON; keep the private key offline
OLCF_UPDATE_SIGNING_KEY=<private key> python -m olcf.ops.manifest sign payload.json -o manifest.json
python -m olcf.ops.manifest verify manifest.json --public-key <base64 public key> --channel stable
```

The private key is read from an environment variable and never from the command line. The payload format is described in the module docstring of `olcf/ops/manifest.py`. Runtime manifests are signed with a different tool (`python -m olcf.runtime keygen|sign`).

## Offline bundles

`scripts/make_offline_bundle.sh --wheel dist/olcf-X.Y.Z-py3-none-any.whl --platform macos-arm64 --out DIR` builds, on any machine with network access, the directory that `scripts/install.sh --offline-bundle DIR` consumes on an air-gapped machine: the static `uv` binary (SHA-256 verified when downloaded), a wheel for every dependency, and Python archives laid out as a mirror. Nothing is compiled on the target. It is attached to a release by hand today; the workflow does not build bundles.

## What is not covered

* `scripts/verify_spark_delta.py` (Spark with Delta Lake and Iceberg jars from Maven Central) runs in the `delta` job on pushes, never on pull requests.
* The Windows and macOS installers run in the `installers` job on GitHub's runners; the Linux-hosted tests also cover a Mac without Command Line Tools.
* The CI job `windows` (targeted: the cross-platform file lock with real `msvcrt`, application start-up with every module, test layout, Git sync and dependency layers) and the macOS legs of the test matrix are configured to run on GitHub's runners; the Linux-hosted tests cover the same code with the platform modules hidden.
* Kubernetes, Livy, Helm and `kubeconform` are covered in CI by the chart rendered with `deploy/scripts/render_chart.py` and the pod behaviour tested with `FakeKube`; `helm lint`, `kubeconform` and the network policy are checked when those tools are present (their tests skip otherwise).
* `tests/web/test_remember_auth.py` needs Node (`OLCF_REQUIRE_NODE=1` turns a missing Node into a failure, as in CI); the PostgreSQL variants of the key-ring tests need `OLCF_TEST_POSTGRES_URL`.
* There is no automatic changelog check, and no job that tests the upgrade from the previous release.
* Third-party GitHub Actions are referenced by tag, not by commit SHA; there is no SBOM or license audit of the vendored JavaScript and the runtime/JDK manifests beyond the notices script (readiness R11).

## Compatibility policy

What users may rely on across releases, and what may change. The worker-protocol, capsule, ruleset and plug-in rows describe mechanisms that exist in the code; the rows about the REST API, the SDK, the CLI and supported platforms are the **policy this documentation sets out for the maintainer to confirm**, because no earlier policy was written down.

| Surface | Promise |
|---|---|
| REST API under `/v1` | Additive changes only within `v1` (new routes, new optional fields, new enum values that older clients can ignore). A breaking change needs a new prefix. The OpenAPI document (`docs/api/openapi.json`) is the contract and is regenerated with every change. |
| Python SDK and CLI | Follow the REST API. Before 1.0.0, a minor release may rename or remove a CLI command or SDK method; the changelog lists it under *Changed* or *Removed*. |
| Worker protocol | Versioned (`WORKER_PROTOCOL_VERSION`). A control plane accepts agents of the same major version and refuses the connection (`unsupported_protocol`) otherwise; the agent reports its own version on `hello`. Upgrade the control plane first, then the agents. |
| Execution capsule | Versioned (`CAPSULE_SCHEMA_VERSION`, schema `schemas/capsule-v1.json`). The control plane accepts capsules of the supported versions and rejects others with an explicit error. |
| Fabric analyzer ruleset | Versioned. A change in classification between rulesets is a behaviour change and is called out in the changelog; the corpus under `tests/corpus` pins it. |
| Pipeline documents | `olcf.pipeline/1`. Older documents keep running. |
| Backups | `OLCFBAK1` containers made by one release can be restored by later releases of the same major version; the restore runs the migrations. |
| Database | Migrations only move forward in normal operation. `olcf db rollback` exists for streams that define a reverse step; rolling back after an upgrade is done by restoring the pre-update backup. |
| Plug-in interfaces | Storage provider, compute provider and activity interfaces are public. The activity interface has its own version (`ACTIVITY_API_VERSION`); a plug-in with a different major version is rejected at load. |
| Runtimes | Immutable once published. A new Spark or library version is a new runtime id. |
| Supported platforms | Python 3.11 to 3.13 on Linux, macOS and Windows; PostgreSQL and SQLite. |
| Security fixes | Applied to the latest release; see [SECURITY.md](../../SECURITY.md) for the supported versions and how to report. |

Behaviour the project does not promise: parity with Microsoft Fabric's runtime or performance, the format of log lines, or the internals of the web console.
