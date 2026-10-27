# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import socket
import threading
import time

import pytest

from isaaccapture_examples.teleop_oden.config import ZmqSpec
from isaaccapture_examples.teleop_oden.zmq_streams import ZmqSink, ZmqSource


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_source_keeps_its_topic_and_sink_republishes_bytes():
    zmq = pytest.importorskip("zmq")
    ctx = zmq.Context.instance()
    producer = ctx.socket(zmq.PUB)
    port = producer.bind_to_random_port("tcp://127.0.0.1")
    spec = ZmqSpec(
        name="cmd",
        direction="to_robot",
        topic="vehicle_control",
        source=f"tcp://127.0.0.1:{port}",
        sink=f"tcp://127.0.0.1:{_free_port()}",
    )
    got = []
    seen = threading.Event()

    def on_payload(payload):
        got.append(payload)
        seen.set()

    source = ZmqSource(spec, on_payload)
    source.start()
    sink = ZmqSink(spec)
    consumer = ctx.socket(zmq.SUB)
    consumer.setsockopt(zmq.SUBSCRIBE, b"vehicle_control ")
    consumer.connect(spec.sink)
    try:
        deadline = time.monotonic() + 5
        while not seen.is_set() and time.monotonic() < deadline:
            producer.send(b'vehicle_control {"sequence": 1}')
            producer.send(b"vehicle_controlx not this one")
            producer.send(b'other_topic {"sequence": 2}')
            seen.wait(0.05)
        assert got and set(got) == {b'{"sequence": 1}'}

        received = None
        deadline = time.monotonic() + 5
        while received is None and time.monotonic() < deadline:
            sink.publish(b"\x00\xff binary")
            if consumer.poll(50):
                received = consumer.recv()
        assert received == b"vehicle_control \x00\xff binary"
    finally:
        source.close()
        sink.close()
        consumer.close()
        producer.close()
