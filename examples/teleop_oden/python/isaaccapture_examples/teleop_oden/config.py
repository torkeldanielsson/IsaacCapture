# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

"""Bridge configuration: which ROS 2 topics and ZMQ streams cross the Oden link."""

from __future__ import annotations

import math
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

import yaml

TO_ROBOT = "to_robot"
TO_OPERATOR = "to_operator"
BYTE_MULTI_ARRAY = "std_msgs/msg/ByteMultiArray"

_GLOB = set("*?[")


@dataclass(frozen=True)
class TopicSpec:
    # A topic name, or a glob such as "/xr_teleop/*" matching discovered topics.
    topic: str
    # Message type; when left out it is taken from the ROS graph.
    type: str | None = None
    # Floats travel as whole multiples of this step (0.0001: a tenth of a millimetre
    # for positions in metres). Left out, floats travel exactly.
    precision: float | None = None
    # "msgpack": carry a std_msgs/ByteMultiArray as its decoded msgpack payload.
    payload: str | None = None
    # Transient-local QoS at both ends, as for /tf_static.
    latched: bool = False
    # Send at most this often; the newest sample still always gets through.
    max_rate_hz: float | None = None

    @property
    def is_pattern(self) -> bool:
        return bool(_GLOB & set(self.topic))

    def matches(self, topic: str, type_name: str) -> bool:
        if self.type is not None and type_name != self.type:
            return False
        return (
            fnmatchcase(topic, self.topic) if self.is_pattern else topic == self.topic
        )


@dataclass(frozen=True)
class ZmqSpec:
    name: str
    direction: str
    topic: str
    # The sending side connects a SUB socket here (the producer's PUB).
    source: str
    # The receiving side binds a PUB socket here for the consumer.
    sink: str
    # "raw": the payload bytes as they are; "json": a JSON payload as numbers.
    payload: str = "raw"
    precision: float | None = None
    max_rate_hz: float | None = None


@dataclass(frozen=True)
class BridgeConfig:
    to_robot: tuple[TopicSpec, ...]
    to_operator: tuple[TopicSpec, ...]
    zmq: tuple[ZmqSpec, ...]

    def topics(self, direction: str) -> tuple[TopicSpec, ...]:
        return self.to_robot if direction == TO_ROBOT else self.to_operator

    def zmq_streams(self, direction: str) -> tuple[ZmqSpec, ...]:
        return tuple(z for z in self.zmq if z.direction == direction)


_TOPIC_KEYS = {"topic", "type", "precision", "payload", "latched", "max_rate_hz"}
_ZMQ_KEYS = {
    "name",
    "direction",
    "topic",
    "source",
    "sink",
    "payload",
    "precision",
    "max_rate_hz",
}


def _positive(entry: dict, key: str, where: str) -> float | None:
    value = entry.get(key)
    if value is None:
        return None
    if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{where}: {key} must be a positive number")
    return float(value)


def _topic(entry: dict, where: str) -> TopicSpec:
    unknown = set(entry) - _TOPIC_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)}")
    if not entry.get("topic"):
        raise ValueError(f"{where}: topic is required")
    payload = entry.get("payload")
    if payload not in (None, "msgpack"):
        raise ValueError(f"{where}: payload must be 'msgpack' if set")
    if payload and entry.get("type", BYTE_MULTI_ARRAY) != BYTE_MULTI_ARRAY:
        raise ValueError(f"{where}: payload 'msgpack' needs {BYTE_MULTI_ARRAY}")
    return TopicSpec(
        topic=entry["topic"],
        type=entry.get("type"),
        precision=_positive(entry, "precision", where),
        payload=payload,
        latched=bool(entry.get("latched", False)),
        max_rate_hz=_positive(entry, "max_rate_hz", where),
    )


def _zmq(entry: dict, where: str) -> ZmqSpec:
    unknown = set(entry) - _ZMQ_KEYS
    if unknown:
        raise ValueError(f"{where}: unknown keys {sorted(unknown)}")
    if entry["direction"] not in (TO_ROBOT, TO_OPERATOR):
        raise ValueError(f"{where}: direction must be {TO_ROBOT} or {TO_OPERATOR}")
    if entry.get("payload", "raw") not in ("raw", "json"):
        raise ValueError(f"{where}: payload must be 'raw' or 'json'")
    return ZmqSpec(
        name=entry["name"],
        direction=entry["direction"],
        topic=entry["topic"],
        source=entry["source"],
        sink=entry["sink"],
        payload=entry.get("payload", "raw"),
        precision=_positive(entry, "precision", where),
        max_rate_hz=_positive(entry, "max_rate_hz", where),
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
