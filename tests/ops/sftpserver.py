# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""A real (in-process) SFTP server on top of paramiko for testing the SFTP backup target."""

from __future__ import annotations

import base64
import hashlib
import os
import socket
import threading

import paramiko


class _Auth(paramiko.ServerInterface):
    def __init__(self, user: str, password: str) -> None:
        self.user, self.password = user, password

    def check_auth_password(self, username: str, password: str) -> int:
        return paramiko.AUTH_SUCCESSFUL if (username, password) == (self.user, self.password) else paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_channel_request(self, kind: str, chanid: int) -> int:
        return paramiko.OPEN_SUCCEEDED if kind == "session" else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class _Handle(paramiko.SFTPHandle):
    def stat(self):  # noqa: ANN201
        return paramiko.SFTPAttributes.from_stat(os.fstat(self.readfile.fileno() if self.readfile else self.writefile.fileno()))

    def chattr(self, attr):  # noqa: ANN001, ANN201
        return paramiko.SFTP_OK


def _make_sftp(root: str):  # noqa: ANN202
    class Stub(paramiko.SFTPServerInterface):
        def _p(self, path: str) -> str:
            p = os.path.normpath(os.path.join(root, path.lstrip("/")))
            if not (p == root or p.startswith(root + os.sep)):
                raise PermissionError(path)
            return p

        def list_folder(self, path):  # noqa: ANN001, ANN201
            try:
                out = []
                for n in os.listdir(self._p(path)):
                    a = paramiko.SFTPAttributes.from_stat(os.stat(os.path.join(self._p(path), n)))
                    a.filename = n
                    out.append(a)
                return out
            except OSError as e:
                return paramiko.SFTPServer.convert_errno(e.errno)

        def stat(self, path):  # noqa: ANN001, ANN201
            try:
                return paramiko.SFTPAttributes.from_stat(os.stat(self._p(path)))
            except OSError as e:
                return paramiko.SFTPServer.convert_errno(e.errno)

        lstat = stat

        def open(self, path, flags, attr):  # noqa: ANN001, ANN201
            try:
                p = self._p(path)
                binary = getattr(os, "O_BINARY", 0)
                fd = os.open(p, flags | binary, 0o600)
            except OSError as e:
                return paramiko.SFTPServer.convert_errno(e.errno)
            if flags & os.O_WRONLY:
                mode = "ab" if flags & os.O_APPEND else "wb"
            elif flags & os.O_RDWR:
                mode = "r+b"
            else:
                mode = "rb"
            f = os.fdopen(fd, mode)
            h = _Handle(flags)
            h.filename = p
            h.readfile = f if mode in ("rb", "r+b") else None
            h.writefile = f if mode != "rb" else None
            return h

        def remove(self, path):  # noqa: ANN001, ANN201
            try:
                os.remove(self._p(path))
                return paramiko.SFTP_OK
            except OSError as e:
                return paramiko.SFTPServer.convert_errno(e.errno)

        def rename(self, oldpath, newpath):  # noqa: ANN001, ANN201
            try:
                os.replace(self._p(oldpath), self._p(newpath))
                return paramiko.SFTP_OK
            except OSError as e:
                return paramiko.SFTPServer.convert_errno(e.errno)

        posix_rename = rename

        def mkdir(self, path, attr):  # noqa: ANN001, ANN201
            try:
                os.mkdir(self._p(path))
                return paramiko.SFTP_OK
            except OSError as e:
                return paramiko.SFTPServer.convert_errno(e.errno)

    return Stub


class SftpTestServer:
    def __init__(self, root: str, user: str = "bk", password: str = "pw-for-sftp-test") -> None:  # noqa: S107 - fixture credential, not a secret
        self.root, self.user, self.password = os.path.realpath(root), user, password
        self.key = paramiko.ECDSAKey.generate()
        self.fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(self.key.asbytes()).digest()).decode().rstrip("=")
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self._stop = False
        self._t = threading.Thread(target=self._serve, daemon=True)

    def start(self) -> SftpTestServer:
        self._t.start()
        return self

    def stop(self) -> None:
        self._stop = True
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
        except OSError:
            pass
        self.sock.close()

    def _serve(self) -> None:
        while not self._stop:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            if self._stop:
                return
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        tr = paramiko.Transport(conn)
        tr.add_server_key(self.key)
        tr.set_subsystem_handler("sftp", paramiko.SFTPServer, _make_sftp(self.root))
        try:
            tr.start_server(server=_Auth(self.user, self.password))
            tr.join()
        except Exception:  # noqa: BLE001, S110
            pass
