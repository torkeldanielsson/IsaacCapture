# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""The envelope each bridge puts in OCP user data, and its sending and receiving ends.

OCP keeps only the newest user data and sends it once per Oden frame, so every
envelope carries the newest sample of every channel until the receiver
acknowledges it; the receiver keeps a sample only when its per-channel sequence
number has moved on. Fields::

    v    format version            sid  sender session id (new on every start)
    seq  envelope number           t    sender realtime ns when built
    s    [[schema id, sample seq, stamp ns, base64 values], ...]   ROS samples
    z    {stream: [sample seq, stamp ns, payload]}                  ZMQ samples
    sc   {schema id: schema}       schemas the receiver has not acknowledged
    ack  [peer sid, [schema ids], {"s": {topic: seq}, "z": {stream: seq}}]
         what this side holds from the peer

plus the direction-specific keys the bridges add (clock sync, latency echoes).
"""

from __future__ import annotations

import base64
import json
import random
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .flatten import Packed

KEY = "oden_teleop"
VERSION = 1
# Schema ids acknowledged per envelope, and held per peer session.
MAX_ACKS = 64
MAX_SCHEMAS = 256


def _size(value) -> int:
    return len(json.dumps(value, separators=(",", ":")))


@dataclass(frozen=True)
class RosSample:
    schema: dict
    seq: int
    stamp_ns: int
    values: bytes


@dataclass(frozen=True)
class ZmqSample:
    name: str
    seq: int
    stamp_ns: int
    payload: str


class Outbox:
    """Newest sample per channel; ``build`` packs as many as fit, in channel order."""

    def __init__(self, budget_bytes: int):
        self.session_id = random.randrange(1, 2**31)
        self.budget_bytes = budget_bytes
        # Samples left out for lack of room, and schemas still waiting for room.
        self.dropped: Counter[str] = Counter()
        self.schema_waiting: Counter[str] = Counter()
        self.last_size = 0
        self._channels: list[str] = []
        # channel -> (wire entry, its JSON size, schema id, schema, schema JSON size)
        self._ros: dict[str, tuple[list, int, str, dict, int]] = {}
        self._zmq: dict[str, tuple[list, int]] = {}
        self._seq_by_channel: Counter[str] = Counter()
        self._peer_schemas: set[str] = set()
        self._peer_ros_seq: dict[str, int] = {}
        self._peer_zmq_seq: dict[str, int] = {}
        self._seq = 0

    def add_channel(self, name: str) -> None:
        """Register a channel; earlier channels win when the budget is short."""
        self._channels.append(name)

    def put_ros(self, topic: str, packed: Packed, stamp_ns: int) -> None:
        self._seq_by_channel[topic] += 1
        entry = [
            packed.schema_id,
            self._seq_by_channel[topic],
            stamp_ns,
            base64.b64encode(packed.values).decode(),
        ]
        schema_cost = _size(packed.schema_id) + _size(packed.schema) + 2
        self._ros[topic] = (
            entry,
            _size(entry) + 1,
            packed.schema_id,
            packed.schema,
            schema_cost,
        )

    def put_zmq(self, name: str, payload: str, stamp_ns: int) -> None:
        self._seq_by_channel[name] += 1
        entry = [self._seq_by_channel[name], stamp_ns, payload]
        self._zmq[name] = (entry, _size(name) + _size(entry) + 2)

    def forget_peer(self) -> None:
        """The peer restarted: it holds none of our schemas or samples."""
        self._peer_schemas = set()
        self._peer_ros_seq = {}
        self._peer_zmq_seq = {}

    def peer_ack(self, ack) -> None:
        """Take the peer's ``ack`` field: which of our schemas and samples it holds."""
        if ack and ack[0] == self.session_id:
            self._peer_schemas = set(ack[1])
            if len(ack) > 2:
                self._peer_ros_seq = ack[2].get("s", {})
                self._peer_zmq_seq = ack[2].get("z", {})

    def build(self, now_ns: int, extra: dict) -> dict:
        self._seq += 1
        env = {
            "v": VERSION,
            "sid": self.session_id,
            "seq": self._seq,
            "t": now_ns,
            **extra,
        }
        s: list = []
        z: dict = {}
        sc: dict = {}
        # Keys, brackets and separators of s, z and sc. Every cost below counts a
        # separator per entry, so ``used`` bounds the final size from above.
        used = _size(env) + 24
        # Schemas the peer lacks go first: they cross only until acknowledged, and a
        # sample whose schema never fits beside the others would never be usable.
        for name in self._channels:
            if name not in self._ros:
                continue
            entry, _, schema_id, schema, schema_cost = self._ros[name]
            if entry[1] <= self._peer_ros_seq.get(name, 0):
                continue
            if schema_id in self._peer_schemas or schema_id in sc:
                continue
            if used + schema_cost > self.budget_bytes:
                self.schema_waiting[name] += 1
                continue
            used += schema_cost
            sc[schema_id] = schema
        for name in self._channels:
            if name in self._ros:
                entry, cost, _, _, _ = self._ros[name]
                if entry[1] <= self._peer_ros_seq.get(name, 0):
                    continue
                if used + cost > self.budget_bytes:
                    self.dropped[name] += 1
                    continue
                used += cost
                s.append(entry)
            elif name in self._zmq:
                entry, cost = self._zmq[name]
                if entry[0] <= self._peer_zmq_seq.get(name, 0):
                    continue
                if used + cost > self.budget_bytes:
                    self.dropped[name] += 1
                    continue
                used += cost
                z[name] = entry
        if s:
            env["s"] = s
        if z:
            env["z"] = z
        if sc:
            env["sc"] = sc
        self.last_size = _size(env)
        return env


class Inbox:
    """Schemas and per-channel sequence numbers of the peer's current session."""

    def __init__(self):
        self.peer_sid: int | None = None
        self.missing_schema: Counter[str] = Counter()
        self._schemas: dict[str, dict] = {}
        self._ros_seq: dict[str, int] = {}
        self._zmq_seq: dict[str, int] = {}
        self._env_seq = 0

    def ack(self) -> list | None:
        if self.peer_sid is None:
            return None
        seqs = {"s": dict(self._ros_seq), "z": dict(self._zmq_seq)}
        return [self.peer_sid, list(self._schemas)[-MAX_ACKS:], seqs]

    def accept(self, env: dict) -> tuple[bool, list[RosSample], list[ZmqSample]]:
        """Return (whether the envelope is new, its new ROS samples, its new ZMQ samples).

        OCP repeats its newest message until the next one, so most envelopes are
        repeats; a repeat yields nothing.
        """
        if env.get("v") != VERSION:
            raise ValueError(f"unsupported envelope version {env.get('v')!r}")
        if env["sid"] != self.peer_sid:
            self.peer_sid = env["sid"]
            self._schemas.clear()
            self._ros_seq.clear()
            self._zmq_seq.clear()
            self._env_seq = 0
        if env["seq"] <= self._env_seq:
            return False, [], []
        self._env_seq = env["seq"]
        for schema_id, schema in env.get("sc", {}).items():
            self._schemas.pop(schema_id, None)
            self._schemas[schema_id] = schema
        while len(self._schemas) > MAX_SCHEMAS:
            del self._schemas[next(iter(self._schemas))]
        ros = []
        for schema_id, seq, stamp, values in env.get("s", ()):
            schema = self._schemas.get(schema_id)
            if schema is None:
                self.missing_schema[schema_id] += 1
                continue
            topic = schema["topic"]
            if seq <= self._ros_seq.get(topic, 0):
                continue
            self._ros_seq[topic] = seq
            ros.append(RosSample(schema, seq, stamp, base64.b64decode(values)))
        zmq = []
        for name, (seq, stamp, payload) in env.get("z", {}).items():
            if seq <= self._zmq_seq.get(name, 0):
                continue
            self._zmq_seq[name] = seq
            zmq.append(ZmqSample(name, seq, stamp, payload))
        return True, ros, zmq
