# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

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


def _pair(budget=3700, channels=("/a",)):
    """A sender and a receiver that has seen its session (a first envelope proves
    nothing: OCP may be forwarding it from a stopped sender)."""
    out, inbox = _box(budget, channels), Inbox()
    assert inbox.accept(_wire(out.build(0, None))) == []
    return out, inbox


def _exchange(out: Outbox, inbox: Inbox, t_ns: int = 0):
    got = inbox.accept(_wire(out.build(t_ns, None)))
    out.peer_ack(_wire(inbox.ack()))
    return got


def test_samples_cross_once_and_schemas_until_acknowledged():
    out, inbox = _pair()
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


def test_every_sample_carries_its_values_in_full():
    out, inbox = _pair()
    out.put("t:/a", _flat("/a", [1_000_000, 0.5, 7]), 0)
    _exchange(out, inbox)
    out.put("t:/a", _flat("/a", [1_000_003, 0.75, 7]), 0)
    env = _wire(out.build(0, None))
    _number, _seq, _hold, *values = env["s"][0]
    assert values == [1_000_003, 0.75, 7]
    assert [r.values for r in inbox.accept(env)] == [[1_000_003, 0.75, 7]]


def test_each_envelope_decodes_with_the_schema_alone():
    out = _box()
    rng = random.Random(3)
    out.put("t:/a", _flat("/a", [0, 0]), 0)
    first = _wire(out.build(0, None))
    envelopes = []
    for i in range(1, 300):
        out.put("t:/a", _flat("/a", [i, rng.randrange(-(2**40), 2**40)]), 0)
        envelopes.append(
            (i, out._channels["t:/a"].current.values, _wire(out.build(0, None)))
        )
    # A receiver that saw only the schema decodes any one later envelope exactly,
    # whatever was lost before it.
    for _i, values, env in envelopes:
        inbox = Inbox()
        inbox.accept(first)
        assert [r.values for r in inbox.accept(env)] == [values]


def test_lost_envelopes_and_acks_never_affect_later_samples():
    out, inbox = _pair()
    rng = random.Random(5)
    truth = {}
    out.put("t:/a", _flat("/a", [0, 0]), 0)
    _exchange(out, inbox)
    delivered = []
    for i in range(1, 300):
        truth[i] = rng.randrange(-1000, 1000)
        out.put("t:/a", _flat("/a", [i, truth[i]]), 0)
        env = _wire(out.build(0, None))
        if rng.random() < 0.3:
            continue  # lost on the way
        got = inbox.accept(env)
        assert len(got) == 1  # every envelope that arrives delivers its sample
        delivered += [r.values for r in got]
        if rng.random() < 0.3:
            continue  # its acknowledgement lost on the way back
        out.peer_ack(_wire(inbox.ack()))
    assert all(v == truth[i] for i, v in delivered)
    assert [i for i, _ in delivered] == sorted({i for i, _ in delivered})


def test_samples_follow_the_ack_across_sequence_wrap():
    out, inbox = _pair()
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
        assert inbox.accept(_wire(out.build(0, None))) == []
        assert len(inbox.accept(_wire(out.build(0, None)))) == 1


def test_a_stopped_sender_s_last_envelope_is_never_delivered():
    out = _box()
    out.put("t:/a", _flat("/a", [5]), 0)
    _ = out.build(0, None)
    last = _wire(out.build(0, None))
    # The sender stops; OCP keeps forwarding its last envelope. A receiver that
    # starts now, or one that already had the session, gets nothing from it.
    fresh = Inbox()
    assert all(fresh.accept(last) == [] for _ in range(100))
    out2, running = _pair()
    out2.put("t:/a", _flat("/a", [6]), 0)
    env = _wire(out2.build(0, None))
    assert [r.values for r in running.accept(env)] == [[6]]
    assert all(running.accept(env) == [] for _ in range(100))


def test_a_released_sample_is_offered_again():
    out, inbox = _pair()
    out.put("t:/a", _flat("/a", [5]), 0)
    assert [r.values for r in _exchange(out, inbox)] == [[5]]
    assert "s" not in out.build(0, None)
    inbox.release("t:/a")
    out.peer_ack(_wire(inbox.ack()))
    assert [r.values for r in inbox.accept(_wire(out.build(0, None)))] == [[5]]


def test_receiver_restart_resends_the_schema():
    out, inbox = _pair()
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
    out, inbox = _pair()
    out.put("t:/a", _flat("/a", [1]), 0)
    env = _wire(out.build(0, None))
    del env["c"]
    assert inbox.accept(env) == []
    assert sum(inbox.missing_schema.values()) == 1


def test_schema_change_is_resent():
    out, inbox = _pair()
    out.put("t:/a", _flat("/a", [1, 2]), 0)
    _exchange(out, inbox)
    out.put("t:/a", _flat("/a", [1, 2, 3], names=("a", "c")), 0)
    env = _wire(out.build(0, None))
    assert [schema["k"] for _, schema in env["c"]] == [["a", "c"]]
    got = inbox.accept(env)
    assert got[0].schema["k"] == ["a", "c"] and got[0].values == [1, 2, 3]


def test_hold_is_the_time_waited_on_the_sender():
    out = _box()
    out.put("t:/a", _flat("/a", [1]), 100 * MS)
    env = out.build(112 * MS + 1, None)
    assert env["s"][0][2] == 13


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
    out, inbox = _pair(600, ("/small", "/big"))
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
    inbox.accept(_wire(out.build(0, None)))
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


def test_a_run_of_zeros_in_a_sample_costs_two_bytes():
    out, inbox = _pair()
    values = [5] + [0] * 199 + [7]
    out.put("t:/a", _flat("/a", values), 0)
    env = _wire(out.build(0, None))
    assert env["s"][0][3:] == [5, None, 199, 7]
    assert inbox.accept(env)[0].values == values


def test_switching_back_to_an_earlier_schema_is_acknowledged():
    out, inbox = _pair()
    for names in (("a", "b"), ("a", "c"), ("a", "b")):
        for i in range(3):
            out.put("t:/a", _flat("/a", [i], names=names), 0)
            _exchange(out, inbox)
        # Everything is acknowledged: nothing left to send.
        assert "s" not in out.build(0, None) and "c" not in out.build(0, None)
