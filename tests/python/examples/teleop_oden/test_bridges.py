# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Both bridges against a stand-in for OCP, with the robot's clock 2.5 s off.

The stand-in follows OCP's timing: the Player forwards the operator's newest
data once per frame, the Streamer hands the newest control state to the robot
every 10 ms and returns the robot's newest answer once per frame, and the robot
answer's latency token comes back in the next video frame.
"""

import heapq
import itertools
import json
import os
import random
import socket
import struct
import threading
import time

import pytest

rclpy = pytest.importorskip("rclpy")

from geometry_msgs.msg import PoseStamped  # noqa: E402
from rclpy.executors import SingleThreadedExecutor  # noqa: E402
from rclpy.parameter import Parameter  # noqa: E402
from sensor_msgs.msg import JointState  # noqa: E402

from isaaccapture_examples.teleop_oden.clock_sync import SystemClock  # noqa: E402
from isaaccapture_examples.teleop_oden.operator_bridge import OperatorBridge  # noqa: E402
from isaaccapture_examples.teleop_oden.robot_bridge import RobotBridge  # noqa: E402

NS = 1_000_000_000
ROBOT_SKEW_NS = 2_500_000_000


class SkewedClock(SystemClock):
    def realtime_ns(self) -> int:
        return time.time_ns() + ROBOT_SKEW_NS


class _Peer:
    """A listening socket taking one framed-JSON client at a time."""

    def __init__(self, on_frame):
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._on_frame = on_frame
        self._conn = None
        self._lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                self._conn = conn
            try:
                while True:
                    (n,) = struct.unpack("<I", _exact(conn, 4))
                    self._on_frame(json.loads(_exact(conn, n)))
            except OSError:
                pass
            with self._lock:
                self._conn = None

    def send(self, obj):
        body = json.dumps(obj).encode()
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.sendall(struct.pack("<I", len(body)) + body)
                except OSError:
                    pass

    def close(self):
        self._listener.close()
        with self._lock:
            if self._conn is not None:
                self._conn.close()


def _exact(conn, n):
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise OSError("closed")
        data += chunk
    return data


class FakeOcp:
    def __init__(self, up_s: float, down_s: float, frame_s: float = 1 / 30):
        self.up_s, self.down_s, self.frame_s = up_s, down_s, frame_s
        self.fault = False
        self._lock = threading.Lock()
        self._client_data = None
        self._at_robot = None
        self._response = None
        self._at_player = None
        self._pending = []
        self._order = itertools.count()
        self._start = time.monotonic()
        self.operator = _Peer(self._from_operator)
        self.robot = _Peer(self._from_robot)
        self._run = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def close(self):
        self._run = False
        self._thread.join()
        self.operator.close()
        self.robot.close()

    def _from_operator(self, msg):
        with self._lock:
            self._client_data = msg

    def _from_robot(self, msg):
        with self._lock:
            self._response = msg

    def _later(self, delay, kind, payload):
        heapq.heappush(
            self._pending, (time.monotonic() + delay, next(self._order), kind, payload)
        )

    def _loop(self):
        player_next = streamer_next = tick_next = time.monotonic()
        streamer_next += self.frame_s / 2
        while self._run:
            now = time.monotonic()
            with self._lock:
                while self._pending and self._pending[0][0] <= now:
                    _, _, kind, payload = heapq.heappop(self._pending)
                    if kind == "up":
                        self._at_robot = payload
                    else:
                        self._at_player = payload
                if now >= player_next:
                    player_next += self.frame_s
                    if self._client_data:
                        data = self._client_data["client_user_data"]["robot"]
                        self._later(self.up_s, "up", data)
                    feedback = {
                        "ping_latency_ms": int((self.up_s + self.down_s) * 1000),
                        "message_latency_roundtrip": None,
                        "ms_since_last_vehicle_data": 0,
                        "missing_cameras": None,
                        "sender_fault": [],
                        "receiver_fault": [],
                        "vehicle_data": self._at_player
                        and self._at_player["vehicle_data"],
                        "last_remote_input": None,
                        "ack_time": self._at_player["token"] if self._at_player else 0,
                        "ack_time_mac": 0,
                        "is_monitor": False,
                    }
                    self.operator.send(
                        {"vehicle_feedback": {"robot": feedback}, "last_input": None}
                    )
                if now >= streamer_next:
                    streamer_next += self.frame_s
                    if self._response:
                        self._later(
                            self.down_s,
                            "down",
                            {
                                "vehicle_data": self._response["vehicle_user_data"],
                                "token": self._response["ack_time"],
                            },
                        )
                if now >= tick_next:
                    tick_next += 0.01
                    token = int((now - self._start) * 1000)
                    at = self._at_robot or {}
                    self.robot.send(
                        {
                            "controller": None,
                            "telemetry": {
                                "latency": token - (at.get("ack_time_returned") or 0),
                                "fault": self.fault,
                                "fault_reason": "Test Fault" if self.fault else "",
                                "missing_cameras": [],
                            },
                            "ack_time": token,
                            "ack_time_mac": 0,
                            "client_user_data": at.get("user_data"),
                            "roundtrip": at.get("roundtrip"),
                        }
                    )
            time.sleep(0.0005)


class _Side:
    """One ROS graph (its own domain), its bridge and an executor thread."""

    def __init__(self, domain: int, bridge_cls, clock, params):
        self.context = rclpy.Context()
        rclpy.init(context=self.context, domain_id=domain)
        self.bridge = bridge_cls(
            clock=clock,
            context=self.context,
            parameter_overrides=[Parameter(k, value=v) for k, v in params.items()],
        )
        self.node = rclpy.create_node(f"test_{domain}", context=self.context)
        self.executor = SingleThreadedExecutor(context=self.context)
        self.executor.add_node(self.bridge)
        self.executor.add_node(self.node)
        self._thread = threading.Thread(target=self.executor.spin, daemon=True)
        self._thread.start()

    def close(self):
        self.executor.shutdown()
        self.bridge.destroy_node()
        self.node.destroy_node()
        rclpy.shutdown(context=self.context)


@pytest.fixture
def rig(tmp_path, request):
    os.environ["ROS_AUTOMATIC_DISCOVERY_RANGE"] = "LOCALHOST"
    up_s, down_s, max_age_ms = getattr(request, "param", (0.02, 0.06, 500.0))
    config = tmp_path / "bridge.yaml"
    config.write_text(
        "to_robot:\n"
        "  - {topic: /cmd, type: geometry_msgs/msg/PoseStamped, float32: true}\n"
        "to_operator:\n"
        "  - {topic: /joint_states, type: sensor_msgs/msg/JointState}\n"
    )
    ocp = FakeOcp(up_s, down_s)
    domain = random.randrange(100, 200)
    common = {"config": str(config), "status_period_s": 0.25}
    operator = _Side(
        domain, OperatorBridge, SystemClock(), {**common, "ocp_port": ocp.operator.port}
    )
    robot = _Side(
        domain + 1,
        RobotBridge,
        SkewedClock(),
        {**common, "ocp_port": ocp.robot.port, "max_age_ms": max_age_ms},
    )
    yield ocp, operator, robot
    operator.close()
    robot.close()
    ocp.close()


def _collect(side, msg_type, topic):
    got = []
    side.node.create_subscription(
        msg_type, topic, lambda m: got.append((time.time_ns(), m)), 10
    )
    return got


def _drive(side, topic, make, seconds, rate_hz=50):
    pub = side.node.create_publisher(type(make(0)), topic, 10)
    sent = []
    end = time.monotonic() + seconds
    i = 0
    while time.monotonic() < end:
        msg = make(i)
        sent.append(msg)
        pub.publish(msg)
        i += 1
        time.sleep(1 / rate_hz)
    return sent


def _stamped_pose(i):
    msg = PoseStamped()
    msg.header.stamp.sec, msg.header.stamp.nanosec = divmod(time.time_ns(), NS)
    msg.header.frame_id = "world"
    msg.pose.position.x = float(i)
    return msg


def _ns(stamp):
    return stamp.sec * NS + stamp.nanosec


def test_commands_reach_the_robot_in_the_robot_clock(rig):
    _ocp, operator, robot = rig
    got = _collect(robot, PoseStamped, "/cmd")
    sent = _drive(operator, "/cmd", _stamped_pose, seconds=4)
    time.sleep(0.5)

    # The clock sync and OCP's first latency echo take a few frames.
    assert len(got) > 40, len(got)
    by_index = {int(m.pose.position.x): m for _, m in got}
    xs = sorted(by_index)
    assert xs == sorted(set(xs))
    estimate = robot.bridge._sync.estimate(time.monotonic_ns())
    bound = estimate.rtt_ns // 2
    for x in xs:
        expected = _ns(sent[x].header.stamp) + ROBOT_SKEW_NS
        assert abs(_ns(by_index[x].header.stamp) - expected) <= bound + 2_000_000
    # Robot-side ages: one way plus waits, nowhere near the 2.5 s skew.
    ages = [(arrived + ROBOT_SKEW_NS) - _ns(m.header.stamp) for arrived, m in got]
    assert 0 < sorted(ages)[len(ages) // 2] < 200_000_000


def test_robot_state_reaches_the_operator_in_its_clock(rig):
    _ocp, operator, robot = rig
    got = _collect(operator, JointState, "/joint_states")

    def joint_state(i):
        msg = JointState()
        msg.header.stamp.sec, msg.header.stamp.nanosec = divmod(
            time.time_ns() + ROBOT_SKEW_NS, NS
        )
        msg.name = ["j0", "j1"]
        msg.position = [float(i), 0.5]
        return msg

    sent = _drive(robot, "/joint_states", joint_state, seconds=3)
    time.sleep(0.5)
    assert len(got) > 20, len(got)
    bound = robot.bridge._sync.estimate(time.monotonic_ns()).rtt_ns // 2
    for _, m in got:
        original = sent[int(m.position[0])]
        expected = _ns(original.header.stamp) - ROBOT_SKEW_NS
        assert abs(_ns(m.header.stamp) - expected) <= bound + 2_000_000
        assert list(m.name) == ["j0", "j1"]


def test_ocp_fault_stops_commands(rig):
    ocp, operator, robot = rig
    got = _collect(robot, PoseStamped, "/cmd")
    _drive(operator, "/cmd", _stamped_pose, seconds=2)
    assert got
    ocp.fault = True
    time.sleep(0.3)
    before = len(got)
    _drive(operator, "/cmd", _stamped_pose, seconds=1)
    assert len(got) == before
    assert robot.bridge._counts["/cmd:dropped_ocp_fault"] > 0
    ocp.fault = False
    _drive(operator, "/cmd", _stamped_pose, seconds=1)
    assert len(got) > before


@pytest.mark.parametrize("rig", [(0.08, 0.02, 40.0)], indirect=True)
def test_samples_older_than_max_age_are_dropped(rig):
    _ocp, operator, robot = rig
    got = _collect(robot, PoseStamped, "/cmd")
    _drive(operator, "/cmd", _stamped_pose, seconds=2)
    time.sleep(0.3)
    assert got == []
    assert robot.bridge._counts["/cmd:dropped_too_old"] > 0
