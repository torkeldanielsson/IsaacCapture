# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""ZMQ streams across the link, as single-frame ``b"<topic> <payload>"`` messages.

The sending bridge subscribes to the producer's PUB socket and keeps only the
newest message; the receiving bridge binds a PUB socket for the consumer, which
connects to it as it would to the producer. Payloads are bytes, so any encoding
crosses.
"""

from __future__ import annotations

import threading
from typing import Callable

from .config import ZmqSpec


class ZmqSource:
    def __init__(self, spec: ZmqSpec, on_payload: Callable[[bytes], None]):
        import zmq

        self._spec = spec
        self._on_payload = on_payload
        self._context = zmq.Context.instance()
        self._run = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, name=f"zmq:{spec.name}", daemon=True
        )

    def start(self) -> None:
        self._run.set()
        self._thread.start()

    def close(self) -> None:
        self._run.clear()
        if self._thread.is_alive():
            self._thread.join(timeout=2)

    def _loop(self) -> None:
        import zmq

        prefix = self._spec.topic.encode() + b" "
        sock = self._context.socket(zmq.SUB)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.CONFLATE, 1)
        sock.setsockopt(zmq.SUBSCRIBE, prefix)
        sock.connect(self._spec.source)
        try:
            while self._run.is_set():
                if not sock.poll(100):
                    continue
                message = sock.recv()
                if message.startswith(prefix):
                    self._on_payload(message[len(prefix) :])
        finally:
            sock.close()


class ZmqSink:
    def __init__(self, spec: ZmqSpec):
        import zmq

        self._prefix = spec.topic.encode() + b" "
        self._lock = threading.Lock()
        self._sock = zmq.Context.instance().socket(zmq.PUB)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.bind(spec.sink)

    def publish(self, payload: bytes) -> None:
        with self._lock:
            self._sock.send(self._prefix + payload)

    def close(self) -> None:
        with self._lock:
            self._sock.close()
