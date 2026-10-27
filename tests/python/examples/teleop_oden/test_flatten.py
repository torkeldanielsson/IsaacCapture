# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import math
import struct

import pytest

pytest.importorskip("rclpy")
msgpack = pytest.importorskip("msgpack")

from geometry_msgs.msg import PoseStamped, TransformStamped  # noqa: E402
from sensor_msgs.msg import JointState  # noqa: E402
from std_msgs.msg import ByteMultiArray  # noqa: E402
from tf2_msgs.msg import TFMessage  # noqa: E402

from isaaccapture_examples.teleop_oden.config import TopicSpec  # noqa: E402
from isaaccapture_examples.teleop_oden.flatten import pack, unpack  # noqa: E402

NS = 1_000_000_000


def _f32(x: float) -> float:
    return struct.unpack("<f", struct.pack("<f", x))[0]


def _pose(stamp_ns=1_700_000_000_123_456_789, frame="world"):
    msg = PoseStamped()
    msg.header.stamp.sec, msg.header.stamp.nanosec = divmod(stamp_ns, NS)
    msg.header.frame_id = frame
    msg.pose.position.x, msg.pose.position.y, msg.pose.position.z = 0.1, -2.25, 1.0 / 3
    msg.pose.orientation.w = 0.7071067811865476
    msg.pose.orientation.z = 0.7071067811865476
    return msg


def _roundtrip(spec, msg, time_map=lambda ns: ns):
    packed = pack(spec, msg)
    return packed, unpack(packed.schema, packed.values, time_map)


def test_float64_round_trip_is_exact():
    msg = _pose()
    packed, out = _roundtrip(TopicSpec("/p", "geometry_msgs/msg/PoseStamped"), msg)
    assert out == msg
    assert packed.stamp_ns == 1_700_000_000_123_456_789
    assert packed.schema["tokens"] == ["world"]


def test_float32_halves_the_numbers_and_rounds_to_float32():
    msg = _pose()
    p64 = pack(TopicSpec("/p", "geometry_msgs/msg/PoseStamped"), msg)
    packed, out = _roundtrip(
        TopicSpec("/p", "geometry_msgs/msg/PoseStamped", float32=True), msg
    )
    assert len(p64.values) - len(packed.values) == 7 * 4
    assert out.pose.position.z == _f32(1.0 / 3)
    assert out.header == msg.header


def test_every_stamp_goes_through_the_time_map_and_zero_stays_zero():
    msg = TFMessage()
    for child, stamp in (("left_wrist", 5 * NS + 7), ("head", 0)):
        t = TransformStamped()
        t.header.stamp.sec, t.header.stamp.nanosec = divmod(stamp, NS)
        t.header.frame_id = "world"
        t.child_frame_id = child
        t.transform.rotation.w = 1.0
        msg.transforms.append(t)
    packed, out = _roundtrip(
        TopicSpec("/tf", "tf2_msgs/msg/TFMessage"),
        msg,
        time_map=lambda ns: ns + 2_500_000_000,
    )
    assert packed.stamp_ns == 5 * NS + 7
    assert (
        out.transforms[0].header.stamp.sec,
        out.transforms[0].header.stamp.nanosec,
    ) == (7, 500_000_007)
    assert (
        out.transforms[1].header.stamp.sec,
        out.transforms[1].header.stamp.nanosec,
    ) == (0, 0)
    assert [t.child_frame_id for t in out.transforms] == ["left_wrist", "head"]


def test_joint_state_with_empty_sequences():
    msg = JointState()
    msg.name = [f"left_thumb_j{i}" for i in range(20)]
    msg.position = [i * 0.01 for i in range(20)]
    _packed, out = _roundtrip(
        TopicSpec("/j", "sensor_msgs/msg/JointState", float32=True), msg
    )
    assert list(out.name) == list(msg.name)
    assert list(out.position) == [_f32(v) for v in msg.position]
    assert len(out.velocity) == 0 and len(out.effort) == 0


def test_schema_id_follows_names_not_numbers():
    spec = TopicSpec("/p", "geometry_msgs/msg/PoseStamped")
    a = pack(spec, _pose())
    b = pack(spec, _pose(stamp_ns=9 * NS))
    c = pack(spec, _pose(frame="head"))
    assert a.schema_id == b.schema_id != c.schema_id


def test_isaac_hand_message_stays_small():
    interfaces = pytest.importorskip("teleop_ros2_interfaces.msg")
    from geometry_msgs.msg import Pose

    msg = interfaces.NamedPoseArray()
    msg.header.frame_id = "world"
    for side in ("left", "right"):
        for j in range(26):
            msg.name.append(f"{side}_joint_{j}")
            pose = Pose()
            pose.position.x = j * 0.01
            pose.orientation.w = 1.0
            msg.pose.append(pose)
            msg.is_valid.append(j % 3 != 0)
    packed, out = _roundtrip(
        TopicSpec(
            "/xr_teleop/hand", "teleop_ros2_interfaces/msg/NamedPoseArray", float32=True
        ),
        msg,
    )
    assert list(out.name) == list(msg.name)
    assert list(out.is_valid) == list(msg.is_valid)
    # stamp, 52 float32 poses, then the flags with their length; names are schema.
    assert len(packed.values) == 8 + 52 * 7 * 4 + 4 + 52


def _msgpack_msg(payload) -> ByteMultiArray:
    msg = ByteMultiArray()
    msg.data = [bytes((b,)) for b in msgpack.packb(payload)]
    return msg


def test_msgpack_payload_crosses_as_numbers_with_its_timestamp_moved():
    payload = {
        "timestamp": 1_700_000_000_000_000_000,
        "left_thumbstick": [0.25, -1.0],
        "right_trigger_value": 0.1,
        "buttons": {"a": True, "b": False},
        "joint_valid": [True, False, True],
        "mixed": [1, 2.5, "x"],
        "label": "right",
        "nothing": None,
    }
    spec = TopicSpec(
        "/xr_teleop/controller_data",
        "std_msgs/msg/ByteMultiArray",
        float32=True,
        payload="msgpack",
        time_keys=("timestamp",),
    )
    packed, out = _roundtrip(spec, _msgpack_msg(payload), lambda ns: ns - 5)
    assert "msgpack" in packed.schema
    assert packed.stamp_ns == payload["timestamp"]
    decoded = msgpack.unpackb(b"".join(out.data))
    assert decoded["timestamp"] == payload["timestamp"] - 5
    assert decoded["right_trigger_value"] == _f32(0.1)
    assert decoded["left_thumbstick"] == [0.25, -1.0]
    assert decoded["buttons"] == {"a": True, "b": False}
    assert decoded["joint_valid"] == [True, False, True]
    assert decoded["mixed"] == [1, 2.5, "x"]
    assert decoded["label"] == "right" and decoded["nothing"] is None
    assert len(packed.values) < len(msgpack.packb(payload)) / 2


def test_non_msgpack_bytes_fall_back_to_the_plain_message():
    msg = ByteMultiArray()
    msg.data = [bytes((b,)) for b in b"\xc1\xc1 not msgpack"]
    spec = TopicSpec("/raw", "std_msgs/msg/ByteMultiArray", payload="msgpack")
    packed, out = _roundtrip(spec, msg)
    assert "tokens" in packed.schema
    assert list(out.data) == list(msg.data)


def test_values_float32_cannot_hold_go_as_float64():
    msg = _pose()
    msg.pose.position.x = 1e300
    packed, out = _roundtrip(
        TopicSpec("/p", "geometry_msgs/msg/PoseStamped", float32=True), msg
    )
    assert packed.schema["f32"] is False
    assert out.pose.position.x == 1e300
    assert not math.isinf(out.pose.position.x)
