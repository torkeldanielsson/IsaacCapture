# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Bridge configuration: which ROS 2 topics and ZMQ streams cross the Oden link."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

TO_ROBOT = "to_robot"
TO_OPERATOR = "to_operator"


@dataclass(frozen=True)
class TopicSpec:
    topic: str
    type: str
    # Pack float64 fields as float32 on the wire.
    float32: bool = False
    # "msgpack": carry a std_msgs/ByteMultiArray as its decoded msgpack payload.
    payload: str | None = None
    # msgpack keys holding sender-clock nanoseconds, moved into the receiver's clock.
    time_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class ZmqSpec:
    name: str
    direction: str
    topic: str
    # The sending side connects a SUB socket here (the producer's PUB).
    source: str
    # The receiving side binds a PUB socket here for the consumer.
    sink: str
    # JSON field holding sender-clock nanoseconds, moved into the receiver's clock.
    timestamp_field: str | None = None


@dataclass(frozen=True)
class BridgeConfig:
    to_robot: tuple[TopicSpec, ...]
    to_operator: tuple[TopicSpec, ...]
    zmq: tuple[ZmqSpec, ...]

    def zmq_streams(self, direction: str) -> tuple[ZmqSpec, ...]:
        return tuple(z for z in self.zmq if z.direction == direction)


_TOPIC_KEYS = {"topic", "type", "float32", "payload", "time_keys"}
_ZMQ_KEYS = {"name", "direction", "topic", "source", "sink", "timestamp_field"}


def _topic(entry: dict, where: str) -> TopicSpec:
    unknown = set(entry) - _TOPIC_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)}")
    if entry.get("payload") not in (None, "msgpack"):
        raise ValueError(f"{where}: payload must be 'msgpack' if set")
    if entry.get("payload") and entry["type"] != "std_msgs/msg/ByteMultiArray":
        raise ValueError(
            f"{where}: payload 'msgpack' needs std_msgs/msg/ByteMultiArray"
        )
    return TopicSpec(
        topic=entry["topic"],
        type=entry["type"],
        float32=bool(entry.get("float32", False)),
        payload=entry.get("payload"),
        time_keys=tuple(entry.get("time_keys", ())),
    )


def _zmq(entry: dict, where: str) -> ZmqSpec:
    unknown = set(entry) - _ZMQ_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)}")
    if entry["direction"] not in (TO_ROBOT, TO_OPERATOR):
        raise ValueError(f"{where}: direction must be {TO_ROBOT} or {TO_OPERATOR}")
    return ZmqSpec(
        name=entry["name"],
        direction=entry["direction"],
        topic=entry["topic"],
        source=entry["source"],
        sink=entry["sink"],
        timestamp_field=entry.get("timestamp_field"),
    )


def load_config(path: str | Path) -> BridgeConfig:
    data = yaml.safe_load(Path(path).read_text()) or {}
    unknown = set(data) - {TO_ROBOT, TO_OPERATOR, "zmq"}
    if unknown:
        raise ValueError(f"{path}: unknown sections {sorted(unknown)}")
    config = BridgeConfig(
        to_robot=tuple(
            _topic(e, f"{path}: {TO_ROBOT}[{i}]")
            for i, e in enumerate(data.get(TO_ROBOT) or [])
        ),
        to_operator=tuple(
            _topic(e, f"{path}: {TO_OPERATOR}[{i}]")
            for i, e in enumerate(data.get(TO_OPERATOR) or [])
        ),
        zmq=tuple(
            _zmq(e, f"{path}: zmq[{i}]") for i, e in enumerate(data.get("zmq") or [])
        ),
    )
    names = [t.topic for t in config.to_robot + config.to_operator]
    names += [f"zmq:{z.name}" for z in config.zmq]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"{path}: listed more than once: {duplicates}")
    return config
