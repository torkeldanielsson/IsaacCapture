# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import random

from isaaccapture_examples.teleop_oden.clock_sync import ClockSync

MS = 1_000_000
S = 1_000_000_000


def _trip(sync, now_mono, offset, up_ns, down_ns, hold_ns, base=10**18):
    """One robot -> operator -> robot trip; ``offset`` is operator minus robot."""
    t1 = base + now_mono
    t2 = t1 + up_ns + offset
    t3 = t2 + hold_ns
    t4 = t1 + up_ns + hold_ns + down_ns
    sync.add(t1, t2, t3, t4, up_ns + down_ns, now_mono + up_ns + hold_ns + down_ns)


def test_symmetric_trip_gives_exact_offset():
    sync = ClockSync()
    _trip(sync, 0, offset=-2_500 * MS, up_ns=20 * MS, down_ns=20 * MS, hold_ns=7 * MS)
    estimate = sync.estimate(1 * S)
    assert estimate.offset_ns == -2_500 * MS
    assert estimate.rtt_ns == 40 * MS
    assert estimate.to_robot(5 * S) == 7_500 * MS
    assert estimate.to_operator(7_500 * MS) == 5 * S


def test_error_stays_within_half_the_round_trip():
    rng = random.Random(1)
    sync = ClockSync()
    offset = 123_456_789
    for i in range(300):
        _trip(
            sync,
            i * 33 * MS,
            offset,
            up_ns=rng.randrange(5, 80) * MS,
            down_ns=rng.randrange(5, 80) * MS,
            hold_ns=rng.randrange(0, 33) * MS,
        )
        estimate = sync.estimate(i * 33 * MS + 200 * MS)
        assert abs(estimate.offset_ns - offset) <= estimate.rtt_ns // 2 + 1


def test_shortest_round_trip_wins_until_it_leaves_the_window():
    sync = ClockSync(window_ns=10 * S)
    _trip(sync, 0, offset=0, up_ns=5 * MS, down_ns=5 * MS, hold_ns=0)
    _trip(sync, 1 * S, offset=0, up_ns=50 * MS, down_ns=10 * MS, hold_ns=0)
    assert sync.estimate(2 * S).rtt_ns == 10 * MS
    assert sync.estimate(10_500 * MS).rtt_ns == 60 * MS


def test_last_estimate_survives_an_empty_window():
    sync = ClockSync(window_ns=1 * S)
    _trip(sync, 0, offset=4 * MS, up_ns=10 * MS, down_ns=10 * MS, hold_ns=0)
    assert sync.estimate(100 * S).offset_ns == 4 * MS


def test_impossible_round_trips_are_rejected():
    sync = ClockSync(max_rtt_ns=1 * S)
    sync.add(0, 0, 0, 0, -1, 0)
    sync.add(0, 0, 0, 0, 2 * S, 0)
    assert sync.estimate(0) is None
    assert sync.rejected == 2
