# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Safe ``git`` command-line runner.

* ``subprocess`` with an argument list - never a shell; every ref / path argument is validated before it gets here (:func:`check_ref`, :func:`check_folder`);
* a scrubbed environment: no system / global git config, no terminal prompts, no hooks (``core.hooksPath`` points at an empty directory), no credential helpers,
  only the transports the caller allows (``GIT_ALLOW_PROTOCOL``);
* credentials (connection + secret) are handed to git through ``GIT_ASKPASS``: a throw-away 0700 directory holds a 0700 script and 0600 files with the user name and
  the password / token; the directory is deleted when the command ends.  They never appear in a URL, an argument, the environment or a log line, and all output
  returned to callers passes :func:`olcf.core.redaction.redact_text` with the secret itself masked as well;
* hard timeouts, bounded output.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from olcf.core import hostbin
from olcf.core.errors import BadRequest, OlcfError, Unavailable, ValidationFailed
from olcf.core.redaction import redact_text

_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$")
_SEG = re.compile(r"^[A-Za-z0-9._-]{1,100}$")
MAX_OUTPUT = 16 * 1024 * 1024


def git_binary() -> str | None:
    """The real ``git`` executable, or ``None``.  Never the macOS ``/usr/bin/git`` installer shim (see :mod:`olcf.core.hostbin`)."""
    return hostbin.find_real_binary("git")


class GitUnavailable(Unavailable):
    """Git sync needs the ``git`` command line tool and none that is safe to run was found (HTTP 503, code ``git_unavailable``)."""

    code = "git_unavailable"


_VERSION_CACHE: dict[str, str | None] = {}


def git_capabilities() -> dict[str, object]:
    """What the UI needs to know before it offers Git sync: ``{available, path, version, backend, remediation, platform}``.

    Only a *real* git is ever executed (``--version``); on a Mac without developer tools nothing is run at all.
    """
    exe = git_binary()
    version: str | None = None
    if exe:
        if exe not in _VERSION_CACHE:
            try:
                out = subprocess.run([exe, "--version"], capture_output=True, text=True, timeout=10, check=False, stdin=subprocess.DEVNULL)  # noqa: S603
                _VERSION_CACHE[exe] = out.stdout.strip().removeprefix("git version ").strip() if out.returncode == 0 else None
            except (OSError, subprocess.SubprocessError):
                _VERSION_CACHE[exe] = None
        version = _VERSION_CACHE[exe]
    ok = bool(exe) and version is not None
    return {"available": ok, "path": exe if ok else None, "version": version, "backend": "git-cli" if ok else None, "platform": sys.platform,
            "remediation": None if ok else git_unavailable_remediation(),
            "detail": "git sync uses the git command line tool" if ok else "git sync is disabled until a real git executable is installed"}


def git_unavailable_remediation() -> str:
    """Operator-facing text for "git is not installed" (mentions the macOS developer-tools situation when it applies)."""
    base = "install git (>= 2.30) on the control plane and restart OLCF"
    return f"{base}. {hostbin.explain_missing('git')}"


def check_ref(name: str, what: str = "branch") -> str:
    """A conservative branch-name check (stricter than ``git check-ref-format``): no leading ``-``, no ``..``, no ``@{``, no ``.lock`` suffix, no empty segments."""
    if not isinstance(name, str) or not _REF.match(name) or ".." in name or "//" in name or name.endswith(("/", ".", ".lock")) or "@{" in name or "/." in name:
        raise ValidationFailed(f"invalid {what} name {str(name)[:60]!r}", remediation="use letters, digits, '.', '_', '-' and '/' (no leading '-', no '..')")
    return name


def check_folder(folder: str) -> str:
    """A repository sub-folder (``''`` = repository root)."""
    folder = (folder or "").strip("/")
    if not folder:
        return ""
    for seg in folder.split("/"):
        if seg in (".", "..") or not _SEG.match(seg) or seg == ".git":
            raise ValidationFailed(f"invalid repository folder {folder[:80]!r}", remediation="use plain path segments such as 'workspaces/dev'")
    return folder


@dataclass
class Credentials:
    """Username and password (or token) handed to a Git subprocess for one command."""
    username: str
    password: str


@dataclass
class GitResult:
    """Outcome of a Git subprocess: return code, standard output and standard error."""
    returncode: int
    stdout: bytes
    stderr: str

    @property
    def text(self) -> str:
        return self.stdout.decode("utf-8", errors="replace")


class GitRunner:
    """Runs git inside one repository directory."""

    def __init__(self, repo: Path, tmp_root: Path, *, timeout: float = 120.0, allow_protocols: str = "https:http", user_agent: str = "olcf-git-sync") -> None:
        self.repo = repo
        self.tmp_root = tmp_root
        self.timeout = timeout
        self.allow_protocols = allow_protocols
        self.git = git_binary()
        self.user_agent = user_agent

    def available(self) -> bool:
        return self.git is not None

    def _env(self, home: Path, askpass: Path | None, identity: tuple[str, str] | None) -> dict[str, str]:
        env = {k: os.environ[k] for k in ("PATH", "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "GIT_SSL_CAINFO", "HTTP_PROXY", "HTTPS_PROXY",
                                              "NO_PROXY", "http_proxy", "https_proxy", "no_proxy", "TMPDIR",
                                              # Windows: without these a child cannot initialise sockets (SYSTEMROOT) or find a temp directory, so git cannot reach any HTTP server
                                              "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP") if k in os.environ}
        env.update({"HOME": str(home), "XDG_CONFIG_HOME": str(home / "xdg"), "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0",
                    "GCM_INTERACTIVE": "never", "GIT_ALLOW_PROTOCOL": self.allow_protocols, "LC_ALL": "C", "GIT_OPTIONAL_LOCKS": "0", "GIT_HTTP_USER_AGENT": self.user_agent,
                    "GIT_ASKPASS": askpass.as_posix() if askpass else "/bin/false", "SSH_ASKPASS": "/bin/false", "GIT_SSH_COMMAND": "/bin/false"})
        if identity:
            env.update({"GIT_AUTHOR_NAME": identity[0], "GIT_AUTHOR_EMAIL": identity[1], "GIT_COMMITTER_NAME": identity[0], "GIT_COMMITTER_EMAIL": identity[1]})
        return env

    def run(self, args: list[str], *, creds: Credentials | None = None, stdin: bytes | None = None, check: bool = True, timeout: float | None = None,
            identity: tuple[str, str] | None = None, cwd: Path | None = None) -> GitResult:
        """Run ``git <args>``; raises :class:`Unavailable` / :class:`BadRequest` on failure when ``check`` (messages are redacted and never contain the credentials)."""
        if not self.git:
            raise GitUnavailable("the 'git' command is not installed on the control plane", remediation=git_unavailable_remediation())
        self.tmp_root.mkdir(parents=True, exist_ok=True)
        work = Path(tempfile.mkdtemp(prefix="olcf-git-", dir=str(self.tmp_root)))
        os.chmod(work, 0o700)
        try:
            (work / "home").mkdir()
            (work / "hooks").mkdir()
            askpass: Path | None = None
            if creds is not None:
                for name, value in (("user", creds.username), ("pass", creds.password)):
                    fd = os.open(work / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(fd, "w") as fh:
                        fh.write(value)
                askpass = work / "askpass.sh"
                fd = os.open(askpass, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
                with os.fdopen(fd, "w") as fh:
                    fh.write(f'#!/bin/sh\ncase "$1" in\n  [Uu]sername*) cat "{work.as_posix()}/user" ;;\n  *) cat "{work.as_posix()}/pass" ;;\nesac\n')
            cmd = [self.git, "-c", "credential.helper=", "-c", f"core.hooksPath={work / 'hooks'}", "-c", "core.fsmonitor=false", "-c", "gc.auto=0", "-c", "core.autocrlf=false",
                   "-c", "core.quotepath=false", "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false", "-c", "advice.detachedHead=false", "-c", "protocol.file.allow=user" if "file" in self.allow_protocols else "protocol.file.allow=never", *args]
            t0 = time.monotonic()
            try:
                proc = subprocess.run(cmd, input=stdin, capture_output=True, timeout=timeout or self.timeout, check=False, cwd=str(cwd or self.repo),  # noqa: S603
                                      env=self._env(work / "home", askpass, identity))
            except subprocess.TimeoutExpired as exc:
                raise Unavailable(f"git {args[0]} timed out after {time.monotonic() - t0:.0f}s") from exc
            except OSError as exc:
                raise Unavailable(f"cannot run git: {exc}") from exc
            err = proc.stderr.decode("utf-8", errors="replace")
            err = self._mask(err, creds)
            if len(proc.stdout) > MAX_OUTPUT:
                raise BadRequest("git produced too much output")
            res = GitResult(proc.returncode, proc.stdout, err)
            if check and proc.returncode != 0:
                raise GitFailure(args[0], proc.returncode, err)
            return res
        finally:
            shutil.rmtree(work, ignore_errors=True)

    @staticmethod
    def _mask(text: str, creds: Credentials | None) -> str:
        if creds:
            for secret in (creds.password, creds.username):
                if secret and len(secret) >= 4:
                    text = text.replace(secret, "***")
        return redact_text(text)[:4000]


class GitFailure(Exception):
    """A git command exited non-zero; ``stderr`` is redacted."""

    def __init__(self, command: str, returncode: int, stderr: str) -> None:
        super().__init__(f"git {command} failed ({returncode}): {stderr.strip()[:500]}")
        self.command, self.returncode, self.stderr = command, returncode, stderr

    def explain(self) -> str:
        low = self.stderr.lower()
        if "authentication failed" in low or "could not read username" in low or "403" in low or "401" in low:
            return "the Git server rejected the credentials"
        if "could not resolve host" in low or "unable to access" in low or "connection refused" in low or "timed out" in low:
            return "the Git server could not be reached"
        if "repository not found" in low or "does not appear to be a git repository" in low or "not found" in low:
            return "the repository was not found"
        if "non-fast-forward" in low or "rejected" in low or "fetch first" in low:
            return "the remote branch has new commits"
        return self.stderr.strip().splitlines()[-1][:300] if self.stderr.strip() else f"git {self.command} failed"


class GitRemoteError(OlcfError):
    """The Git remote refused or could not be reached (HTTP 502); the message is redacted and never contains credentials."""

    status = 502
    code = "git_remote_error"


def remote_error(exc: GitFailure, what: str) -> GitRemoteError:
    return GitRemoteError(f"{what}: {exc.explain()}", details={"git_command": exc.command}, remediation="check the repository URL, branch and the connection's credentials")


def _cquote(path: str) -> str:
    out = []
    for b in path.encode("utf-8"):
        c = chr(b)
        out.append(f"\\{b:03o}" if b < 0x20 or b > 0x7E or c in '"\\' else c)
    return '"' + "".join(out) + '"'


@dataclass
class Tree:
    """Files of a repository folder at one revision (``path relative to the folder -> bytes``)."""

    files: dict[str, bytes]
    skipped: list[str]


class Repo:
    """A bare local repository used by Git sync.  Only blobs that are listed by ``ls-tree`` are ever read; nothing is checked out, so a hostile repository cannot
    write outside this directory (no symlinks, no hooks, no work tree)."""

    MAX_FILES = 50_000
    MAX_BLOB = 32 * 1024 * 1024
    MAX_TOTAL = 512 * 1024 * 1024

    def __init__(self, runner: GitRunner, url: str) -> None:
        self.r = runner
        self.url = url

    def init(self) -> None:
        self.r.repo.mkdir(parents=True, exist_ok=True)
        if not (self.r.repo / "HEAD").exists():
            self.r.run(["init", "--bare", "-q", "--initial-branch=olcf-sync", "."])

    def ls_remote(self, creds: Credentials | None) -> dict[str, str]:
        try:
            res = self.r.run(["ls-remote", "--heads", "--", self.url], creds=creds)
        except GitFailure as exc:
            raise remote_error(exc, "cannot list branches") from exc
        out: dict[str, str] = {}
        for line in res.text.splitlines():
            sha, _, ref = line.partition("\t")
            if ref.startswith("refs/heads/") and re.fullmatch(r"[0-9a-f]{40,64}", sha):
                out[ref[len("refs/heads/"):]] = sha
        return out

    def fetch(self, branch: str, creds: Credentials | None) -> str | None:
        """Fetch ``branch`` into ``refs/remotes/origin/<branch>``; ``None`` when the remote has no such branch."""
        check_ref(branch)
        try:
            self.r.run(["fetch", "--no-tags", "--no-recurse-submodules", "--force", "--quiet", "--", self.url, f"+refs/heads/{branch}:refs/remotes/origin/{branch}"], creds=creds)
        except GitFailure as exc:
            if "couldn't find remote ref" in exc.stderr.lower():
                self.r.run(["update-ref", "-d", f"refs/remotes/origin/{branch}"], check=False)
                return None
            raise remote_error(exc, "fetch failed") from exc
        return self.tip(branch)

    def tip(self, branch: str) -> str | None:
        res = self.r.run(["rev-parse", "--verify", "-q", f"refs/remotes/origin/{check_ref(branch)}^{{commit}}"], check=False)
        return res.text.strip() or None if res.returncode == 0 else None

    def has_commit(self, sha: str) -> bool:
        return bool(re.fullmatch(r"[0-9a-f]{40,64}", sha or "")) and self.r.run(["cat-file", "-e", f"{sha}^{{commit}}"], check=False).returncode == 0

    def tree_id(self, rev: str | None, folder: str) -> str | None:
        if not rev:
            return None
        spec = f"{rev}:{folder}" if folder else f"{rev}^{{tree}}"
        res = self.r.run(["rev-parse", "--verify", "-q", spec], check=False)
        return res.text.strip() if res.returncode == 0 else None

    def read_tree(self, rev: str | None, folder: str) -> Tree:
        """All regular files below ``folder`` at ``rev`` (an unknown revision / folder is an empty tree).  Symlinks and submodules are skipped and reported."""
        if not rev or self.tree_id(rev, folder) is None:
            return Tree({}, [])
        res = self.r.run(["ls-tree", "-r", "-z", "--full-tree", rev, "--", f"{folder}/" if folder else "."])
        entries: list[tuple[str, str]] = []
        skipped: list[str] = []
        prefix = f"{folder}/" if folder else ""
        for rec in res.stdout.split(b"\0"):
            if not rec:
                continue
            head, _, path_b = rec.partition(b"\t")
            mode, typ, sha = head.decode().split(" ")
            path = path_b.decode("utf-8", errors="replace")
            if typ != "blob" or mode not in ("100644", "100755"):
                skipped.append(path)
                continue
            if not path.startswith(prefix) or ".." in path.split("/") or "\\" in path:
                skipped.append(path)
                continue
            entries.append((path[len(prefix):], sha))
        if len(entries) > self.MAX_FILES:
            raise ValidationFailed(f"the repository folder has too many files ({len(entries)} > {self.MAX_FILES})")
        files: dict[str, bytes] = {}
        total = 0
        if entries:
            shas = sorted({s for _, s in entries})
            proc = self._cat_batch(shas)
            for rel, sha in entries:
                blob = proc[sha]
                total += len(blob)
                if total > self.MAX_TOTAL:
                    raise ValidationFailed("the repository folder is too large")
                files[rel] = blob
        return Tree(files, skipped)

    def _cat_batch(self, shas: list[str]) -> dict[str, bytes]:
        res = self.r.run(["cat-file", "--batch"], stdin=("\n".join(shas) + "\n").encode(), timeout=max(self.r.timeout, 300))
        data, pos, out = res.stdout, 0, {}
        for _ in shas:
            nl = data.index(b"\n", pos)
            head = data[pos:nl].decode().split(" ")
            if len(head) != 3 or head[1] != "blob":
                raise ValidationFailed("unexpected object in repository")
            size = int(head[2])
            if size > self.MAX_BLOB:
                raise ValidationFailed(f"a file in the repository is larger than {self.MAX_BLOB // 1024 // 1024} MiB")
            out[head[0]] = data[nl + 1:nl + 1 + size]
            pos = nl + 1 + size + 1
        return out

    def commit(self, parent: str | None, changes: dict[str, bytes | None], message: str, identity: tuple[str, str], ref: str) -> str:
        """Create a commit on top of ``parent`` (``None`` = root commit) applying ``changes`` (``path -> bytes`` to write, ``None`` to delete) with ``git fast-import``;
        the commit is stored under ``ref`` (a private ref) and its sha is returned.  Nothing touches a work tree."""
        name, email = (re.sub(r"[<>\n\0]", "", identity[0]).strip() or "olcf", re.sub(r"[<>\n\0 ]", "", identity[1]) or "olcf@localhost.invalid")
        msg = message.encode()
        buf = bytearray()
        buf += f"commit {ref}\n".encode()
        stamp = f"{int(time.time())} +0000"
        buf += f"author {name} <{email}> {stamp}\ncommitter {name} <{email}> {stamp}\ndata {len(msg)}\n".encode() + msg + b"\n"
        if parent:
            buf += f"from {parent}\n".encode()
        else:
            buf += b"deleteall\n"
        for path in sorted(changes):
            if path.startswith("/") or ".." in path.split("/") or "\n" in path or "\0" in path or path.split("/")[0] == ".git":
                raise ValidationFailed(f"unsafe repository path {path[:80]!r}")
            data = changes[path]
            if data is None:
                buf += f"D {_cquote(path)}\n".encode()
            else:
                buf += f"M 100644 inline {_cquote(path)}\ndata {len(data)}\n".encode() + data + b"\n"
        buf += b"done\n"
        self.r.run(["fast-import", "--quiet", "--done", "--force"], stdin=bytes(buf), timeout=max(self.r.timeout, 300))
        res = self.r.run(["rev-parse", "--verify", f"{ref}^{{commit}}"])
        return res.text.strip()

    def push(self, sha: str, branch: str, creds: Credentials | None) -> None:
        try:
            self.r.run(["push", "--porcelain", "--no-verify", "--", self.url, f"{sha}:refs/heads/{check_ref(branch)}"], creds=creds)
        except GitFailure as exc:
            raise remote_error(exc, "push failed") from exc
        self.r.run(["update-ref", f"refs/remotes/origin/{branch}", sha])

    def create_remote_branch(self, sha: str, branch: str, creds: Credentials | None) -> None:
        self.push(sha, branch, creds)
