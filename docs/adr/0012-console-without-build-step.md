# ADR 0012: The web console has no build step and no CDN

Status: Accepted

## Context

The console must work on an air-gapped network and be maintainable by contributors who know Python. A JavaScript toolchain (Node, a bundler, `node_modules`) adds a supply-chain surface and a build requirement to every release and every `pip install -e`.

## Decision

The console is plain JavaScript modules served as static files from `src/olcf/web/static`, using Preact and htm (a tagged-template alternative to JSX) and CodeMirror 5 for editing, all **vendored unmodified** under `vendor/` with their licences and checksums recorded in `vendor/VENDOR.md` (the one change, a rewritten import in Preact's hooks module, is listed there). Fonts are self-hosted. There is no CDN, no remote font and no network access at run time. Rendering goes through Preact (which escapes text), no code uses `innerHTML`, and HTML from notebook outputs passes `js/notebook/sanitize.js`.

## Consequences

* Edit a file and reload; nothing to install. The wheel contains the console as it is.
* No TypeScript, tree shaking or minification; payload size is larger than an optimised build.
* Adding a library means vendoring it by hand and updating `VENDOR.md`.
* Browser behaviour is tested with Playwright (`tests/e2e`), which asserts that no console errors, failed requests or HTTP errors occur in any test.

## Alternatives considered

A React/Vite build (rejected: toolchain); server-rendered pages (rejected: interactive notebook and designer UIs).

## Verified by

`src/olcf/web/static/` (`vendor/VENDOR.md`), `tests/e2e/`.
