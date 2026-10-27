# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import random
from types import SimpleNamespace

import pytest

from isaaccapture_examples.teleop_oden.envelope import Inbox, Outbox


def _packed(topic: str, values: bytes, names=("a", "b")):
    schema = {"topic": topic, "type": "pkg/msg/T", "f32": True, "tokens": list(names)}
    schema_id = f"{topic}:" + hashlib.sha1(",".join(names).encode()).hexdigest()[:6]
    return SimpleNamespace(
        schema=schema, schema_id=schema_id, values=values, stamp_ns=None
    )


def _size(env) -> int:
    return len(json.dumps(env, separators=(",", ":")))


def test_samples_cross_once_and_schemas_until_acknowledged():
    out, inbox = Outbox(4000), Inbox()
    out.add_channel("/a")
    out.put_ros("/a", _packed("/a", b"\x01\x02"), 111)

    first = out.build(1, {})
    assert "sc" in first
    new, ros, _ = inbox.accept(first)
    assert new and [(s.schema["topic"], s.values, s.stamp_ns) for s in ros] == [
        ("/a", b"\x01\x02", 111)
    ]

    # OCP repeats a message until the next one; a repeat delivers nothing.
    assert inbox.accept(first) == (False, [], [])
    # A new envelope still carrying the same sample delivers nothing either.
    new, ros, _ = inbox.accept(out.build(2, {}))
    assert new and ros == []

    out.peer_ack(inbox.ack())
    out.put_ros("/a", _packed("/a", b"\x03"), 222)
    third = out.build(3, {})
    assert "sc" not in third
    assert [s.values for s in inbox.accept(third)[1]] == [b"\x03"]


def test_new_session_resets_the_receiver():
    inbox = Inbox()
    for _ in range(2):
        out = Outbox(4000)
        out.add_channel("/a")
        out.put_ros("/a", _packed("/a", b"x"), 5)
        assert len(inbox.accept(out.build(1, {}))[1]) == 1


def test_sample_without_its_schema_waits():
    sender, inbox = Outbox(4000), Inbox()
    sender.add_channel("/a")
    sender.put_ros("/a", _packed("/a", b"x"), 5)
    env = sender.build(1, {})
    del env["sc"]
    assert inbox.accept(env)[1] == []
    assert sum(inbox.missing_schema.values()) == 1


def test_schema_change_is_resent():
    out, inbox = Outbox(4000), Inbox()
    out.add_channel("/a")
    out.put_ros("/a", _packed("/a", b"x"), 5)
    inbox.accept(out.build(1, {}))
    out.peer_ack(inbox.ack())
    out.put_ros("/a", _packed("/a", b"y", names=("a", "c")), 6)
    env = out.build(2, {})
    assert [schema["tokens"] for schema in env["sc"].values()] == [["a", "c"]]
    assert inbox.accept(env)[1][0].schema["tokens"] == ["a", "c"]


def test_budget_holds_and_earlier_channels_win():
    rng = random.Random(7)
    for _ in range(200):
        budget = rng.randrange(300, 4000)
        out = Outbox(budget)
        topics = [f"/t{i}" for i in range(rng.randrange(1, 8))]
        for topic in topics:
            out.add_channel(topic)
            out.put_ros(
                topic, _packed(topic, rng.randbytes(rng.randrange(0, 900))), 10**18
            )
        out.add_channel("cmd")
        out.put_zmq("cmd", "x" * rng.randrange(0, 300), 10**18)
        env = out.build(
            10**18, {"ack": [123, ["abc"] * 5], "sync": [10**18, 10**18, 10**7]}
        )
        assert _size(env) <= budget
        sent = [entry[0].split(":")[0] for entry in env.get("s", [])]
        assert sent == [t for t in topics if t in sent]
        assert (
            out.dropped.total() + len(sent) + len(env.get("z", {})) == len(topics) + 1
        )


def test_zmq_streams_dedup_by_sequence():
    out, inbox = Outbox(4000), Inbox()
    out.add_channel("cmd")
    out.put_zmq("cmd", '{"timestamp_ns": 5}', 5)
    assert [z.payload for z in inbox.accept(out.build(1, {}))[2]] == [
        '{"timestamp_ns": 5}'
    ]
    assert inbox.accept(out.build(2, {}))[2] == []


def test_unknown_version_is_refused():
    with pytest.raises(ValueError):
        Inbox().accept({"v": 99, "sid": 1, "seq": 1, "t": 0})


def test_a_big_schema_behind_full_channels_still_gets_through():
    out, inbox = Outbox(1200), Inbox()
    out.add_channel("/small")
    out.add_channel("/big")
    out.put_ros("/small", _packed("/small", b"s" * 300), 1)
    out.put_ros(
        "/big", _packed("/big", b"b" * 200, names=tuple("n" * 40 for _ in range(12))), 1
    )
    delivered = set()
    for t in range(1, 6):
        _, ros, _ = inbox.accept(out.build(t, {}))
        delivered |= {s.schema["topic"] for s in ros}
        out.peer_ack(inbox.ack())
        out.put_ros("/small", _packed("/small", b"s" * 300), t + 1)
        out.put_ros(
            "/big",
            _packed("/big", b"b" * 200, names=tuple("n" * 40 for _ in range(12))),
            t + 1,
        )
    assert delivered == {"/small", "/big"}


def test_acknowledged_samples_are_not_resent():
    out, inbox = Outbox(4000), Inbox()
    out.add_channel("/a")
    out.add_channel("cmd")
    out.put_ros("/a", _packed("/a", b"x" * 100), 5)
    out.put_zmq("cmd", "{}", 5)
    inbox.accept(out.build(1, {}))
    # Until the receiver's ack comes back, the newest samples keep crossing.
    assert "s" in out.build(2, {}) and "z" in out.build(3, {})
    out.peer_ack(inbox.ack())
    held = out.build(4, {})
    assert "s" not in held and "z" not in held
    out.put_ros("/a", _packed("/a", b"y" * 100), 6)
    assert [s.values for s in inbox.accept(out.build(5, {}))[1]] == [b"y" * 100]
