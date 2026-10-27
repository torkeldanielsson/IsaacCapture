# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import threading
import time

import pytest

from isaaccapture_examples.teleop_oden.clock_sync import SystemClock
from isaaccapture_examples.teleop_oden.config import ZmqSpec
from isaaccapture_examples.teleop_oden.zmq_streams import (
    ZmqSink,
    ZmqSource,
    retime,
    stamp_of,
)


def test_retime_moves_only_the_timestamp_field():
    payload = json.dumps({"sequence": 3, "timestamp_ns": 1_000, "steer": 0.5})
    out = json.loads(retime(payload, "timestamp_ns", lambda ns: ns + 7))
    assert out == {"sequence": 3, "timestamp_ns": 1_007, "steer": 0.5}
    assert stamp_of(payload, "timestamp_ns") == 1_000


@pytest.mark.parametrize(
    "payload", ["not json", "[1, 2]", '{"timestamp_ns": "x"}', "{}"]
)
def test_retime_passes_other_payloads(payload):
    assert retime(payload, "timestamp_ns", lambda ns: ns + 7) == payload
    assert stamp_of(payload, "timestamp_ns") is None


def test_source_keeps_newest_and_sink_republishes():
    zmq = pytest.importorskip("zmq")
    ctx = zmq.Context.instance()
    producer = ctx.socket(zmq.PUB)
    port = producer.bind_to_random_port("tcp://127.0.0.1")
    sink_port = _free_port()
    spec = ZmqSpec(
        name="cmd",
        direction="to_robot",
        topic="vehicle_control",
        source=f"tcp://127.0.0.1:{port}",
        sink=f"tcp://127.0.0.1:{sink_port}",
    )
    got = []
    seen = threading.Event()

    def on_payload(payload, _now):
        got.append(payload)
        seen.set()

    source = ZmqSource(spec, on_payload, SystemClock())
    source.start()
    sink = ZmqSink(spec)
    consumer = ctx.socket(zmq.SUB)
    consumer.setsockopt_string(zmq.SUBSCRIBE, "vehicle_control ")
    consumer.connect(spec.sink)
    try:
        deadline = time.monotonic() + 5
        while not seen.is_set() and time.monotonic() < deadline:
            producer.send_string('vehicle_control {"sequence": 1}')
            producer.send_string('other_topic {"sequence": 2}')
            seen.wait(0.05)
        assert got and got[-1] == '{"sequence": 1}'

        received = None
        deadline = time.monotonic() + 5
        while received is None and time.monotonic() < deadline:
            sink.publish('{"sequence": 9}')
            if consumer.poll(50):
                received = consumer.recv_string()
        assert received == 'vehicle_control {"sequence": 9}'
    finally:
        source.close()
        sink.close()
        consumer.close()
        producer.close()


def _free_port() -> int:
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
