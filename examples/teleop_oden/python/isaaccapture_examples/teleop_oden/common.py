# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pieces both bridge nodes use: latency statistics, /tf handling and status output."""

from __future__ import annotations

import json
from collections import defaultdict

TF_TYPE = "tf2_msgs/msg/TFMessage"
STATUS_TOPIC = "oden_teleop/status"


def _ms(ns: float) -> float:
    return round(ns / 1e6, 2)


class PeriodStats:
    """Latencies per key since the last ``take``, as count / p50 / p95 / max in ms."""

    def __init__(self):
        self._values: dict[str, list[int]] = defaultdict(list)

    def add(self, key: str, value_ns: int) -> None:
        self._values[key].append(value_ns)

    def take(self) -> dict[str, dict]:
        out = {}
        for key, values in sorted(self._values.items()):
            values.sort()
            n = len(values)
            out[key] = {
                "n": n,
                "p50_ms": _ms(values[n // 2]),
                "p95_ms": _ms(values[min(n - 1, (n * 95) // 100)]),
                "max_ms": _ms(values[-1]),
            }
        self._values.clear()
        return out


class Rates:
    """Per-second rates of monotonically growing counters between ``take`` calls."""

    def __init__(self, clock):
        self._clock = clock
        self._last: dict[str, int] = {}
        self._t = clock.monotonic_ns()

    def take(self, **counters: int) -> dict[str, float]:
        now = self._clock.monotonic_ns()
        dt = max(now - self._t, 1) / 1e9
        out = {
            k: round((v - self._last.get(k, v)) / dt, 1) for k, v in counters.items()
        }
        self._last, self._t = dict(counters), now
        return out


class TfMerger:
    """Operator side: /tf carries one frame per message, OCP keeps one message per
    topic, so send the newest transform of every child frame together."""

    def __init__(self):
        self._latest: dict[str, object] = {}

    def merge(self, msg):
        for transform in msg.transforms:
            self._latest[transform.child_frame_id] = transform
        merged = type(msg)()
        merged.transforms = [self._latest[k] for k in sorted(self._latest)]
        return merged


class TfRepeatFilter:
    """Robot side: drop transforms already published with the same stamp (tf2 warns
    about repeated data)."""

    def __init__(self):
        self._stamps: dict[str, tuple[int, int]] = {}

    def fresh(self, msg):
        keep = []
        for transform in msg.transforms:
            stamp = (transform.header.stamp.sec, transform.header.stamp.nanosec)
            if self._stamps.get(transform.child_frame_id) != stamp:
                self._stamps[transform.child_frame_id] = stamp
                keep.append(transform)
        if not keep:
            return None
        msg.transforms = keep
        return msg


def status_message(node_name: str, level_ok: bool, summary: str, values: dict, stamp):
    """A diagnostic_msgs/DiagnosticArray with ``values`` flattened to key/value strings."""
    from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue

    status = DiagnosticStatus()
    status.name = node_name
    status.hardware_id = "oden"
    status.level = DiagnosticStatus.OK if level_ok else DiagnosticStatus.WARN
    status.message = summary
    for key, value in values.items():
        text = (
            value
            if isinstance(value, str)
            else json.dumps(value, separators=(",", ":"))
        )
        status.values.append(KeyValue(key=key, value=text))
    array = DiagnosticArray()
    array.header.stamp = stamp
    array.status = [status]
    return array
