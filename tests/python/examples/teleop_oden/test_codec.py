# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import math


from isaaccapture_examples.teleop_oden.codec import (
    delta,
    flatten_stream,
    rebuild_stream,
    undelta,
)

NS = 1_000_000_000


def test_delta_round_trip_moves_only_integers():
    base = [10, 2.5, "x", 7]
    values = [13, 2.75, "y", 7]
    wire = delta(values, base)
    assert wire == [3, 2.75, "y", 0]
    assert undelta(wire, base) == values


def test_delta_refuses_differences_beyond_64_bits():
    assert delta([2**64 - 1], [-(2**63)]) is None


def test_raw_stream_is_bytes_in_and_out():
    payload = b"\x00\xffbinary \xc1"
    flat = flatten_stream("cmd", payload)
    assert flat.schema == {"z": "cmd"}
    assert rebuild_stream(flat.schema, flat.values) == payload


def test_json_stream_crosses_as_numbers():
    payload = {"sequence": 41, "timestamp_ns": 1_700_000_000 * NS, "steer": 0.123456}
    flat = flatten_stream("cmd", json.dumps(payload).encode(), 1e-4, json_payload=True)
    assert flat.values == [41, 1_700_000_000 * NS, 1235]
    out = json.loads(rebuild_stream(flat.schema, flat.values))
    assert out == {**payload, "steer": 0.1235}


def test_json_stream_without_precision_is_exact():
    payload = {"a": [0.1, 1e300], "b": [True, False], "c": None, "d": "text"}
    flat = flatten_stream("cmd", json.dumps(payload).encode(), json_payload=True)
    assert json.loads(rebuild_stream(flat.schema, flat.values)) == payload


def test_non_json_stream_falls_back_to_raw():
    flat = flatten_stream("cmd", b"not json", 1e-4, json_payload=True)
    assert flat.schema == {"z": "cmd"}
    assert rebuild_stream(flat.schema, flat.values) == b"not json"


def test_float_the_precision_cannot_carry_goes_plain():
    payload = json.dumps({"x": float("nan"), "y": 0.5}).encode()
    flat = flatten_stream("cmd", payload, 1e-4, json_payload=True)
    assert "p" not in flat.schema
    out = json.loads(rebuild_stream(flat.schema, flat.values))
    assert math.isnan(out["x"]) and out["y"] == 0.5
