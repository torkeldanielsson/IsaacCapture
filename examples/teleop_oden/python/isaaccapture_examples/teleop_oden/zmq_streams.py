# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""ZMQ streams across the link, as single-frame ``"<topic> <payload>"`` strings.

The sending bridge subscribes to the producer's PUB socket and keeps only the
newest message; the receiving bridge binds a PUB socket for the consumer, which
connects to it as it would to the producer.
"""

from __future__ import annotations

import json
import threading
from typing import Callable

from .clock_sync import SystemClock
from .config import ZmqSpec


def stamp_of(payload: str, field: str | None) -> int | None:
    if not field:
        return None
    try:
        value = json.loads(payload).get(field)
    except (ValueError, AttributeError):
        return None
    return value if isinstance(value, int) and value > 0 else None


def retime(payload: str, field: str | None, time_map: Callable[[int], int]) -> str:
    """Move the JSON ``field`` of ``payload`` through ``time_map``; other payloads pass."""
    if stamp_of(payload, field) is None:
        return payload
    data = json.loads(payload)
    data[field] = time_map(data[field])
    return json.dumps(data, separators=(",", ":"))


class ZmqSource:
    def __init__(
        self,
        spec: ZmqSpec,
        on_payload: Callable[[str, int], None],
        clock: SystemClock,
    ):
        import zmq

        self._spec = spec
        self._on_payload = on_payload
        self._clock = clock
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
        self._thread.join(timeout=2)

    def _loop(self) -> None:
        import zmq

        sock = self._context.socket(zmq.SUB)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.CONFLATE, 1)
        sock.setsockopt_string(zmq.SUBSCRIBE, f"{self._spec.topic} ")
        sock.connect(self._spec.source)
        prefix = len(self._spec.topic) + 1
        try:
            while self._run.is_set():
                if not sock.poll(100):
                    continue
                message = sock.recv().decode("utf-8", errors="replace")
                self._on_payload(message[prefix:], self._clock.realtime_ns())
        finally:
            sock.close()


class ZmqSink:
    def __init__(self, spec: ZmqSpec):
        import zmq

        self._topic = spec.topic
        self._lock = threading.Lock()
        self._sock = zmq.Context.instance().socket(zmq.PUB)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.bind(spec.sink)

    def publish(self, payload: str) -> None:
        with self._lock:
            self._sock.send_string(f"{self._topic} {payload}")

    def close(self) -> None:
        with self._lock:
            self._sock.close()
