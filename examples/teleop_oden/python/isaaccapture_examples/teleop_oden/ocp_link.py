# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""A framed JSON connection to one of OCP's TCP servers, reconnecting until closed.

Both OCP servers (the Streamer's vehicle port and the Player's operator port) take
one client at a time and frame every message, both ways, as a 4-byte little-endian
length followed by a UTF-8 JSON body.
"""

from __future__ import annotations

import json
import socket
import struct
import threading
import time
from typing import Callable

# OCP rejects bigger frames; ours never come near.
MAX_FRAME_BYTES = 16 * 1024
# A peer frame bigger than this means the stream is not OCP.
_MAX_READ_BYTES = 4 * 1024 * 1024

OnMessage = Callable[[dict], None]


class OcpLink:
    def __init__(
        self,
        host: str,
        port: int,
        on_message: OnMessage,
        log_info: Callable[[str], None],
        log_warn: Callable[[str], None],
        retry_s: float = 1.0,
    ):
        self._address = (host, port)
        self._on_message = on_message
        self._log_info = log_info
        self._log_warn = log_warn
        self._retry_s = retry_s
        self._run = threading.Event()
        self._send_lock = threading.Lock()
        self._sock: socket.socket | None = None
        self._thread = threading.Thread(
            target=self._loop, name=f"ocp:{port}", daemon=True
        )
        self.frames_in = 0
        self.frames_out = 0

    @property
    def connected(self) -> bool:
        return self._sock is not None

    def start(self) -> None:
        self._run.set()
        self._thread.start()

    def close(self) -> None:
        self._run.clear()
        sock = self._sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self._thread.join(timeout=5)

    def send(self, message: dict) -> bool:
        """Send one frame; False when there is no connection or it is too big."""
        body = json.dumps(message, separators=(",", ":")).encode()
        if len(body) > MAX_FRAME_BYTES:
            self._log_warn(
                f"OCP frame of {len(body)} B exceeds {MAX_FRAME_BYTES} B, not sent"
            )
            return False
        with self._send_lock:
            sock = self._sock
            if sock is None:
                return False
            try:
                sock.sendall(struct.pack("<I", len(body)) + body)
            except OSError:
                return False
            self.frames_out += 1
            return True

    def _loop(self) -> None:
        failed_before = False
        while self._run.is_set():
            try:
                sock = socket.create_connection(self._address, timeout=2.0)
            except OSError as e:
                if not failed_before:
                    self._log_warn(
                        f"OCP at {self._address[0]}:{self._address[1]} unreachable ({e}); retrying"
                    )
                    failed_before = True
                time.sleep(self._retry_s)
                continue
            failed_before = False
            sock.settimeout(None)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._log_info(f"connected to OCP at {self._address[0]}:{self._address[1]}")
            with self._send_lock:
                self._sock = sock
            try:
                self._read(sock)
            except (OSError, ValueError) as e:
                if self._run.is_set():
                    self._log_warn(f"OCP connection lost: {e}")
            finally:
                with self._send_lock:
                    self._sock = None
                sock.close()
            if self._run.is_set():
                time.sleep(self._retry_s)

    def _read(self, sock: socket.socket) -> None:
        while self._run.is_set():
            (length,) = struct.unpack("<I", _read_exact(sock, 4))
            if length > _MAX_READ_BYTES:
                raise ValueError(f"OCP frame length {length} is not plausible")
            body = _read_exact(sock, length)
            self.frames_in += 1
            self._on_message(json.loads(body))


def _read_exact(sock: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise OSError("closed by OCP")
        data += chunk
    return bytes(data)
