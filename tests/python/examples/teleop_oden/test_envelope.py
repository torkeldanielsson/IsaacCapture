# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import random

import pytest

from isaaccapture_examples.teleop_oden.codec import Flat
from isaaccapture_examples.teleop_oden.envelope import (
    Inbox,
    Outbox,
    cbor_size,
    expand,
    json_size,
    squeeze,
)

MS = 1_000_000


def _flat(topic: str, values, names=("a", "b")) -> Flat:
    return Flat({"t": topic, "y": "pkg/msg/T", "k": list(names)}, list(values))


def _wire(env):
    """What reaches the peer: the envelope through JSON, as OCP carries it."""
    return json.loads(json.dumps(env))


def _box(budget=3700, channels=("/a",)):
    out = Outbox(budget, 15000)
    for i, name in enumerate(channels):
        out.add_channel(f"t:{name}", (0, i, 0))
    return out


def _exchange(out: Outbox, inbox: Inbox, t_ns: int = 0):
    got = inbox.accept(_wire(out.build(t_ns, None)))
    out.peer_ack(_wire(inbox.ack()))
    return got


def test_samples_cross_once_and_schemas_until_acknowledged():
    out, inbox = _box(), Inbox()
    out.put("t:/a", _flat("/a", [1, 2.5]), 0)

    first = _wire(out.build(0, None))
    assert "c" in first
    got = inbox.accept(first)
    assert [(r.key, r.values) for r in got] == [("t:/a", [1, 2.5])]

    # OCP repeats a message until the next one; a repeat delivers nothing.
    assert inbox.accept(first) == []
    out.peer_ack(_wire(inbox.ack()))
    # Acknowledged: nothing left to send.
    assert "s" not in out.build(0, None)

    out.put("t:/a", _flat("/a", [3, 2.5]), 0)
    third = _wire(out.build(0, None))
    assert "c" not in third
    assert [r.values for r in inbox.accept(third)] == [[3, 2.5]]


def test_integers_cross_as_differences_to_an_acknowledged_sample():
    out, inbox = _box(), Inbox()
    out.put("t:/a", _flat("/a", [1_000_000, 0.5, 7]), 0)
    _exchange(out, inbox)
    out.put("t:/a", _flat("/a", [1_000_003, 0.75, 7]), 0)
    env = _wire(out.build(0, None))
    _number, _seq, back, _hold, *values = env["s"][0]
    assert back == 1 and values == [3, 0.75, 0]
    assert [r.values for r in inbox.accept(env)] == [[1_000_003, 0.75, 7]]


def test_differences_survive_lost_envelopes_and_acks():
    out, inbox = _box(), Inbox()
    rng = random.Random(3)
    truth = [0]
    out.put("t:/a", _flat("/a", [0, 0]), 0)
    _exchange(out, inbox)
    delivered = []
    for i in range(1, 300):
        truth.append(truth[-1] + rng.randrange(-50, 50))
        out.put("t:/a", _flat("/a", [i, truth[i]]), 0)
        env = _wire(out.build(0, None))
        if rng.random() < 0.3:
            continue  # lost on the way
        delivered += [r.values for r in inbox.accept(env)]
        if rng.random() < 0.3:
            continue  # its acknowledgement lost on the way back
        out.peer_ack(_wire(inbox.ack()))
    delivered += [r.values for r in _exchange(out, inbox)]
    assert all(v == truth[i] for i, v in delivered)
    assert [i for i, _ in delivered] == sorted({i for i, _ in delivered})
    assert delivered[-1] == [299, truth[299]]
    assert sum(inbox.missing_base.values()) == 0


def test_differences_follow_the_ack_across_sequence_wrap():
    out, inbox = _box(), Inbox()
    for i in range(70_000):
        out.put("t:/a", _flat("/a", [i]), 0)
        if i % 1000 == 0 or i > 69_990:
            got = _exchange(out, inbox)
            assert [r.values for r in got] == [[i]]


def test_new_session_resets_the_receiver():
    inbox = Inbox()
    for _ in range(2):
        out = _box()
        out.put("t:/a", _flat("/a", [5]), 0)
        assert len(inbox.accept(_wire(out.build(0, None)))) == 1


def test_receiver_restart_resends_schema_and_absolute_values():
    out, inbox = _box(), Inbox()
    out.put("t:/a", _flat("/a", [5]), 0)
    _exchange(out, inbox)
    fresh = Inbox()
    out.put("t:/a", _flat("/a", [6]), 0)
    # The new receiver has nothing yet: the sample waits for its schema.
    assert fresh.accept(_wire(out.build(0, None))) == []
    out.forget_peer()
    out.peer_ack(_wire(fresh.ack()))
    assert [r.values for r in fresh.accept(_wire(out.build(0, None)))] == [[6]]


def test_sample_without_its_schema_waits():
    out, inbox = _box(), Inbox()
    out.put("t:/a", _flat("/a", [1]), 0)
    env = _wire(out.build(0, None))
    del env["c"]
    assert inbox.accept(env) == []
    assert sum(inbox.missing_schema.values()) == 1


def test_schema_change_is_resent_with_absolute_values():
    out, inbox = _box(), Inbox()
    out.put("t:/a", _flat("/a", [1, 2]), 0)
    _exchange(out, inbox)
    out.put("t:/a", _flat("/a", [1, 2, 3], names=("a", "c")), 0)
    env = _wire(out.build(0, None))
    assert [schema["k"] for _, schema in env["c"]] == [["a", "c"]]
    assert env["s"][0][2] == 0
    got = inbox.accept(env)
    assert got[0].schema["k"] == ["a", "c"] and got[0].values == [1, 2, 3]


def test_hold_is_the_time_waited_on_the_sender():
    out = _box()
    out.put("t:/a", _flat("/a", [1]), 100 * MS)
    env = out.build(112 * MS + 1, None)
    assert env["s"][0][3] == 13


def test_budget_holds_and_earlier_channels_win():
    rng = random.Random(7)
    for _ in range(200):
        budget = rng.randrange(150, 4000)
        topics = [f"/t{i}" for i in range(rng.randrange(1, 8))]
        out = _box(budget, topics)
        for topic in topics:
            values = [
                rng.choice([rng.randrange(-(2**40), 2**40), rng.random(), "xy"])
                for _ in range(rng.randrange(0, 300))
            ]
            out.put(f"t:{topic}", _flat(topic, values), 0)
        env = out.build(0, [123] + [1, 2] * 5)
        assert cbor_size(env) <= budget
        assert out.last_cbor == cbor_size(env)
        sent = [entry[0] for entry in env.get("s", [])]
        assert sent == sorted(sent)
        assert sum(out.dropped.values()) + sum(out.schema_waiting.values()) + len(
            sent
        ) == len(topics)


def test_a_big_schema_behind_full_channels_still_gets_through():
    out, inbox = _box(600, ("/small", "/big")), Inbox()
    delivered = set()
    for t in range(6):
        out.put("t:/small", _flat("/small", [0.1 * i for i in range(40)]), 0)
        out.put(
            "t:/big", _flat("/big", [t], names=tuple("n" * 40 for _ in range(10))), 0
        )
        delivered |= {r.key for r in _exchange(out, inbox)}
    assert delivered == {"t:/small", "t:/big"}


def test_unknown_version_is_refused():
    with pytest.raises(ValueError):
        Inbox().accept({"v": 99, "id": 1})


@pytest.mark.parametrize(
    "value",
    [
        0,
        23,
        24,
        255,
        256,
        65535,
        65536,
        2**32 - 1,
        2**32,
        2**64 - 1,
        -1,
        -24,
        -25,
        -256,
        -257,
        -(2**63),
        0.0,
        -0.0,
        1.0,
        0.5,
        65505.0,
        0.1,
        1e-8,
        3.4e38,
        1e300,
        2.0**-24,
        "",
        "abc",
        "x" * 30,
        "é",
        None,
        True,
        False,
        [],
        [1, [2, 3]],
        {"a": 1, "bb": [0.25, "c"]},
        list(range(30)),
    ],
)
def test_cbor_size_matches_a_cbor_encoder(value):
    cbor2 = pytest.importorskip("cbor2")
    # canonical: shortest integers and the shortest exact float, as ciborium writes.
    assert cbor_size(value) == len(cbor2.dumps(value, canonical=True))


def test_json_size_is_the_compact_encoding():
    assert json_size({"a": [1, 2.5]}) == len('{"a":[1,2.5]}')


def test_max_rate_holds_samples_back_but_the_newest_gets_through():
    out, inbox = Outbox(3700, 15000), Inbox()
    out.add_channel("t:/a", (0, 0, 0), max_rate_hz=10)
    delivered = []
    for i in range(30):  # 100 Hz for 0.3 s
        out.put("t:/a", _flat("/a", [i]), i * 10 * MS)
        delivered += [r.values[0] for r in _exchange(out, inbox, i * 10 * MS)]
    assert delivered == [0, 10, 20]
    # The publisher stops; its last sample still arrives once the interval passes.
    delivered += [r.values[0] for r in _exchange(out, inbox, 400 * MS)]
    assert delivered[-1] == 29


def test_zero_runs_squeeze_and_expand():
    values = [0, 0, 5, 0, 0, 0, 0.0, 0, 0, 0, "s", 0]
    wire = squeeze(values)
    assert wire == [0, 0, 5, None, 3, 0.0, None, 3, "s", 0]
    assert expand(wire) == values


def test_unchanged_values_cost_two_bytes_per_run():
    out, inbox = _box(), Inbox()
    out.put("t:/a", _flat("/a", list(range(1, 201))), 0)
    _exchange(out, inbox)
    out.put("t:/a", _flat("/a", list(range(1, 200)) + [7]), 0)
    env = _wire(out.build(0, None))
    assert env["s"][0][4:] == [None, 199, 7 - 200]
    assert inbox.accept(env)[0].values == list(range(1, 200)) + [7]


def test_switching_back_to_an_earlier_schema_is_acknowledged():
    out, inbox = _box(), Inbox()
    for names in (("a", "b"), ("a", "c"), ("a", "b")):
        for i in range(3):
            out.put("t:/a", _flat("/a", [i], names=names), 0)
            _exchange(out, inbox)
        # Everything is acknowledged: nothing left to send.
        assert "s" not in out.build(0, None) and "c" not in out.build(0, None)
