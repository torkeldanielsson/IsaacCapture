# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Offset between the operator's and the robot's realtime clocks, measured through OCP.

One sample is one trip robot -> operator -> robot: the robot stamps ``t1`` on its
OCP response, the operator bridge stamps ``t2`` when that response reaches it and
``t3`` when it next writes to OCP, and the robot stamps ``t4`` on arrival. As in
NTP, ``offset = ((t2 - t1) + (t3 - t4)) / 2`` and the round trip excludes the
operator's hold time. Round trips are measured on monotonic clocks so a realtime
step cannot make a sample look short. The estimate is the shortest-round-trip
sample in the window; its error is at most half that round trip.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass


class SystemClock:
    """The clocks the bridges read; tests substitute one with a skewed realtime."""

    def realtime_ns(self) -> int:
        return time.time_ns()

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()


@dataclass(frozen=True)
class ClockEstimate:
    # Operator realtime minus robot realtime.
    offset_ns: int
    # Round trip of the sample the offset came from; the error is at most half of it.
    rtt_ns: int
    # Robot monotonic time of that sample.
    measured_mono_ns: int

    def to_robot(self, operator_ns: int) -> int:
        return operator_ns - self.offset_ns

    def to_operator(self, robot_ns: int) -> int:
        return robot_ns + self.offset_ns


class ClockSync:
    def __init__(
        self, window_ns: int = 10_000_000_000, max_rtt_ns: int = 2_000_000_000
    ):
        self._window_ns = window_ns
        self._max_rtt_ns = max_rtt_ns
        # Sliding-window minimum: round trips increase from front to back.
        self._candidates: deque[ClockEstimate] = deque()
        self._last: ClockEstimate | None = None
        self.accepted = 0
        self.rejected = 0

    def add(
        self, t1: int, t2: int, t3: int, t4: int, rtt_ns: int, now_mono_ns: int
    ) -> None:
        """Add one round trip; ``rtt_ns`` is robot monotonic t4 - t1 minus the operator hold."""
        if rtt_ns < 0 or rtt_ns > self._max_rtt_ns:
            self.rejected += 1
            return
        self.accepted += 1
        sample = ClockEstimate(((t2 - t1) + (t3 - t4)) // 2, rtt_ns, now_mono_ns)
        while self._candidates and self._candidates[-1].rtt_ns >= rtt_ns:
            self._candidates.pop()
        self._candidates.append(sample)
        self.estimate(now_mono_ns)

    def estimate(self, now_mono_ns: int) -> ClockEstimate | None:
        """Best sample in the window; with none left, the last one (clocks drift slowly)."""
        while (
            self._candidates
            and now_mono_ns - self._candidates[0].measured_mono_ns > self._window_ns
        ):
            self._candidates.popleft()
        if self._candidates:
            self._last = self._candidates[0]
        return self._last
