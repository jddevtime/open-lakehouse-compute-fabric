<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright (c) 2026 Justin Dodrill -->
# Claude Desktop and OLCF

`claude_desktop_config.json` in this directory is the shape Claude Desktop expects. **Do not hand-edit it if you can avoid it**: let OLCF
write it for you, with the right absolute paths and a freshly created, narrowly scoped token:

```sh
olcf mcp config --client claude-desktop            # read-only SQL and catalog browsing (scope "mcp")
olcf mcp config --client claude-desktop --write    # also allow submitting and cancelling jobs (scope "mcp:write")
```

Merge the printed JSON into Claude Desktop's `claude_desktop_config.json` (Settings, Developer, Edit Config) and restart Claude Desktop.

* `command` is OLCF's own launcher inside its private environment (`<prefix>/current/venv/bin/olcf`). It is deliberately **not** `python3`:
  on a Mac without the Xcode Command Line Tools `/usr/bin/python3` is a stub that opens the installer dialog.
* The token is shown once and lives only in that file. Treat the file like a password (`chmod 600`), and revoke the token any time with
  `olcf token revoke <id>`. Windows paths use `%LOCALAPPDATA%\olcf\current\venv\Scripts\olcf.exe`.
* For an https server with a private CA, add `"SSL_CERT_FILE": "/path/to/ca.crt"` to `env` (the generator does this when your CLI context has a CA bundle).
* Clients that speak Streamable HTTP natively (Claude Code, many IDEs) do not need the bridge:
  `claude mcp add --transport http olcf http://127.0.0.1:8470/mcp --header "Authorization: Bearer <token>"`.
