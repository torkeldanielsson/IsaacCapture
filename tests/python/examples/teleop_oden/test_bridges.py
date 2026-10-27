# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Both bridges against a stand-in for OCP.

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
from types import SimpleNamespace

import pytest

rclpy = pytest.importorskip("rclpy")

from geometry_msgs.msg import PoseStamped  # noqa: E402
from rclpy.executors import SingleThreadedExecutor  # noqa: E402
from rclpy.parameter import Parameter  # noqa: E402
from sensor_msgs.msg import JointState  # noqa: E402
from std_msgs.msg import String  # noqa: E402

from isaaccapture_examples.teleop_oden.operator_bridge import OperatorBridge  # noqa: E402
from isaaccapture_examples.teleop_oden.robot_bridge import RobotBridge  # noqa: E402

NS = 1_000_000_000


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
                        }
                    )
            time.sleep(0.0005)


class _Side:
    """One ROS graph (its own domain), its bridge and an executor thread."""

    def __init__(self, domain: int, bridge_cls, params):
        self.context = rclpy.Context()
        rclpy.init(context=self.context, domain_id=domain)
        self.bridge = bridge_cls(
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


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def rig(tmp_path, request):
    os.environ["ROS_AUTOMATIC_DISCOVERY_RANGE"] = "LOCALHOST"
    up_s, down_s, max_age_ms = getattr(request, "param", (0.02, 0.06, 500.0))
    zmq_source, zmq_sink = _free_port(), _free_port()
    config = tmp_path / "bridge.yaml"
    config.write_text(
        "to_robot:\n"
        "  - {topic: /cmd, type: geometry_msgs/msg/PoseStamped, precision: 0.0001}\n"
        "  - {topic: '/disc/*'}\n"
        "to_operator:\n"
        "  - {topic: /joint_states, type: sensor_msgs/msg/JointState}\n"
        "zmq:\n"
        "  - {name: steer, direction: to_robot, topic: vehicle_control,\n"
        f"     source: 'tcp://127.0.0.1:{zmq_source}', sink: 'tcp://127.0.0.1:{zmq_sink}',\n"
        "     payload: json, precision: 0.0001}\n"
    )
    ocp = FakeOcp(up_s, down_s)
    domain = random.randrange(100, 200)
    common = {"config": str(config), "status_period_s": 0.25, "discovery_period_s": 0.2}
    operator = _Side(domain, OperatorBridge, {**common, "ocp_port": ocp.operator.port})
    robot = _Side(
        domain + 1,
        RobotBridge,
        {**common, "ocp_port": ocp.robot.port, "max_age_ms": max_age_ms},
    )
    rig = SimpleNamespace(
        ocp=ocp,
        operator=operator,
        robot=robot,
        zmq_source=zmq_source,
        zmq_sink=zmq_sink,
    )
    yield rig
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
    msg.pose.position.y = 0.123456
    return msg


def test_commands_reach_the_robot_with_stamps_as_sent(rig):
    got = _collect(rig.robot, PoseStamped, "/cmd")
    sent = _drive(rig.operator, "/cmd", _stamped_pose, seconds=3)
    time.sleep(0.5)

    assert len(got) > 40, len(got)
    by_index = {int(m.pose.position.x): m for _, m in got}
    for x, m in by_index.items():
        assert m.header == sent[x].header
        assert m.pose.position.y == 0.1235
    counts = rig.robot.bridge._counts
    assert counts["t:/cmd:published"] == len(got)
    assert not any(k.endswith("dropped_too_old") for k in counts)


def test_robot_state_reaches_the_operator(rig):
    got = _collect(rig.operator, JointState, "/joint_states")

    def joint_state(i):
        msg = JointState()
        msg.header.stamp.sec, msg.header.stamp.nanosec = divmod(time.time_ns(), NS)
        msg.name = ["j0", "j1"]
        msg.position = [float(i), 0.5]
        return msg

    sent = _drive(rig.robot, "/joint_states", joint_state, seconds=3)
    time.sleep(0.5)
    assert len(got) > 20, len(got)
    for _, m in got:
        original = sent[int(m.position[0])]
        assert m.header == original.header
        assert list(m.name) == ["j0", "j1"] and m.position[1] == 0.5


def test_ocp_fault_stops_commands(rig):
    got = _collect(rig.robot, PoseStamped, "/cmd")
    _drive(rig.operator, "/cmd", _stamped_pose, seconds=2)
    assert got
    rig.ocp.fault = True
    time.sleep(0.3)
    before = len(got)
    _drive(rig.operator, "/cmd", _stamped_pose, seconds=1)
    assert len(got) == before
    assert rig.robot.bridge._counts["t:/cmd:dropped_ocp_fault"] > 0
    rig.ocp.fault = False
    _drive(rig.operator, "/cmd", _stamped_pose, seconds=1)
    assert len(got) > before


@pytest.mark.parametrize("rig", [(0.08, 0.02, 40.0)], indirect=True)
def test_samples_older_than_max_age_are_dropped(rig):
    got = _collect(rig.robot, PoseStamped, "/cmd")
    _drive(rig.operator, "/cmd", _stamped_pose, seconds=2)
    time.sleep(0.3)
    assert got == []
    assert rig.robot.bridge._counts["t:/cmd:dropped_too_old"] > 0


def test_topics_matching_a_pattern_are_found_with_their_types(rig):
    got_text = _collect(rig.robot, String, "/disc/text")
    got_joints = _collect(rig.robot, JointState, "/disc/joints")
    text = rig.operator.node.create_publisher(String, "/disc/text", 10)
    joints = rig.operator.node.create_publisher(JointState, "/disc/joints", 10)
    deadline = time.monotonic() + 8
    while (not got_text or not got_joints) and time.monotonic() < deadline:
        text.publish(String(data="hello"))
        msg = JointState()
        msg.name, msg.position = ["a"], [0.5]
        joints.publish(msg)
        time.sleep(0.05)
    assert got_text and got_text[-1][1].data == "hello"
    assert got_joints and list(got_joints[-1][1].position) == [0.5]
    assert "/disc/text" in rig.operator.bridge._subscribed


def test_json_zmq_stream_crosses_as_numbers(rig):
    zmq = pytest.importorskip("zmq")
    ctx = zmq.Context.instance()
    producer = ctx.socket(zmq.PUB)
    producer.bind(f"tcp://127.0.0.1:{rig.zmq_source}")
    consumer = ctx.socket(zmq.SUB)
    consumer.setsockopt(zmq.SUBSCRIBE, b"vehicle_control ")
    consumer.connect(f"tcp://127.0.0.1:{rig.zmq_sink}")
    try:
        received = None
        deadline = time.monotonic() + 8
        sequence = 0
        while received is None and time.monotonic() < deadline:
            sequence += 1
            payload = {
                "sequence": sequence,
                "timestamp_ns": time.time_ns(),
                "steer": 0.123456,
            }
            producer.send(b"vehicle_control " + json.dumps(payload).encode())
            if consumer.poll(20):
                received = consumer.recv()
        assert received is not None
        body = json.loads(received.split(b" ", 1)[1])
        assert body["steer"] == 0.1235 and isinstance(body["timestamp_ns"], int)
    finally:
        consumer.close()
        producer.close()
