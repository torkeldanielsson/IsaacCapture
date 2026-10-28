# SPDX-FileCopyrightText: Copyright (c) 2026 Voysys AB
# SPDX-License-Identifier: MIT

import json
import math

import pytest

pytest.importorskip("rclpy")

from geometry_msgs.msg import PoseStamped, TransformStamped  # noqa: E402
from sensor_msgs.msg import Image, JointState  # noqa: E402
from std_msgs.msg import ByteMultiArray  # noqa: E402
from tf2_msgs.msg import TFMessage  # noqa: E402

from isaaccapture_examples.teleop_oden.codec import (  # noqa: E402
    flatten_topic,
    rebuild_topic,
)

NS = 1_000_000_000


def _pose(stamp_ns=1_700_000_000_123_456_789, frame="world"):
    msg = PoseStamped()
    msg.header.stamp.sec, msg.header.stamp.nanosec = divmod(stamp_ns, NS)
    msg.header.frame_id = frame
    msg.pose.position.x, msg.pose.position.y, msg.pose.position.z = 0.1, -2.25, 1.0 / 3
    msg.pose.orientation.w = 0.7071067811865476
    msg.pose.orientation.z = 0.7071067811865476
    return msg


def _roundtrip(topic, type_name, msg, precision=None, msgpack_payload=False):
    flat = flatten_topic(topic, type_name, msg, precision, msgpack_payload)
    # The values cross as JSON.
    values = json.loads(json.dumps(flat.values))
    return flat, rebuild_topic(flat.schema, values)


def test_message_round_trip_is_exact_without_precision():
    msg = _pose()
    flat, out = _roundtrip("/p", "geometry_msgs/msg/PoseStamped", msg)
    assert out == msg
    assert flat.schema["k"] == ["world"]
    # sec, nanosec, then the seven pose numbers.
    assert flat.values[:2] == [1_700_000_000, 123_456_789]
    assert len(flat.values) == 9


def test_precision_rounds_floats_to_whole_steps():
    msg = _pose()
    flat, out = _roundtrip("/p", "geometry_msgs/msg/PoseStamped", msg, 1e-4)
    assert flat.values[2:5] == [1000, -22500, 3333]
    assert out.pose.position.z == 0.3333
    assert out.header == msg.header


def test_stamps_pass_unchanged():
    msg = TFMessage()
    for child, stamp in (("left_wrist", 5 * NS + 7), ("head", 0)):
        t = TransformStamped()
        t.header.stamp.sec, t.header.stamp.nanosec = divmod(stamp, NS)
        t.header.frame_id = "world"
        t.child_frame_id = child
        t.transform.rotation.w = 1.0
        msg.transforms.append(t)
    _flat, out = _roundtrip("/tf", "tf2_msgs/msg/TFMessage", msg, 1e-4)
    assert [(t.header.stamp.sec, t.header.stamp.nanosec) for t in out.transforms] == [
        (5, 7),
        (0, 0),
    ]
    assert [t.child_frame_id for t in out.transforms] == ["left_wrist", "head"]


def test_sequence_lengths_are_schema():
    msg = JointState()
    msg.name = [f"j{i}" for i in range(3)]
    msg.position = [0.0, 0.5, 1.0]
    a = flatten_topic("/j", "sensor_msgs/msg/JointState", msg, 1e-4)
    msg.position = [0.25, 0.5, 1.0]
    b = flatten_topic("/j", "sensor_msgs/msg/JointState", msg, 1e-4)
    msg.velocity = [0.0, 0.0, 0.0]
    c = flatten_topic("/j", "sensor_msgs/msg/JointState", msg, 1e-4)
    assert a.schema == b.schema != c.schema
    assert len(a.values) == len(b.values) == 2 + 3


def test_byte_arrays_cross_as_base64():
    msg = Image()
    msg.height, msg.width, msg.encoding, msg.step = 2, 3, "mono8", 3
    msg.data = bytes(range(6))
    flat, out = _roundtrip("/img", "sensor_msgs/msg/Image", msg)
    assert isinstance(flat.values[-1], str)
    assert bytes(out.data) == bytes(range(6)) and out.encoding == "mono8"


def test_non_finite_floats_cross():
    msg = _pose()
    msg.pose.position.x = float("inf")
    msg.pose.position.y = float("nan")
    flat, out = _roundtrip("/p", "geometry_msgs/msg/PoseStamped", msg, 1e-4)
    assert "p" not in flat.schema
    assert out.pose.position.x == float("inf") and math.isnan(out.pose.position.y)


def test_isaac_hand_message_is_mostly_small_numbers():
    interfaces = pytest.importorskip("teleop_ros2_interfaces.msg")
    from geometry_msgs.msg import Pose

    msg = interfaces.NamedPoseArray()
    msg.header.frame_id = "world"
    for side in ("left", "right"):
        for j in range(25):
            msg.name.append(f"{side}_joint_{j}")
            pose = Pose()
            pose.position.x = j * 0.01
            pose.orientation.w = 1.0
            msg.pose.append(pose)
            msg.is_valid.append(j % 3 != 0)
    flat, out = _roundtrip(
        "/xr_teleop/hand", "teleop_ros2_interfaces/msg/NamedPoseArray", msg, 1e-4
    )
    assert list(out.name) == list(msg.name)
    assert list(out.is_valid) == list(msg.is_valid)
    # stamp, 50 poses, 50 flags; names are schema.
    assert len(flat.values) == 2 + 50 * 7 + 50


msgpack = pytest.importorskip("msgpack")


def _msgpack_msg(payload) -> ByteMultiArray:
    msg = ByteMultiArray()
    msg.data = [bytes((b,)) for b in msgpack.packb(payload)]
    return msg


def test_msgpack_payload_crosses_as_numbers():
    payload = {
        "timestamp": 1_700_000_000_000_000_000,
        "left_thumbstick": [0.25, -1.0],
        "right_trigger_value": 0.1,
        "buttons": {"a": True, "b": False},
        "joint_valid": [True, False, True],
        "counts": [1, 2, 3],
        "mixed": [1, 2.5, "x"],
        "label": "right",
        "blob": b"\x00\x01",
        "nothing": None,
    }
    flat, out = _roundtrip(
        "/xr_teleop/controller_data",
        "std_msgs/msg/ByteMultiArray",
        _msgpack_msg(payload),
        1e-4,
        msgpack_payload=True,
    )
    assert "m" in flat.schema
    decoded = msgpack.unpackb(b"".join(out.data))
    assert decoded == {**payload, "right_trigger_value": 0.1}


def test_non_msgpack_bytes_fall_back_to_the_plain_message():
    msg = ByteMultiArray()
    msg.data = [bytes((b,)) for b in b"\xc1\xc1 not msgpack"]
    flat, out = _roundtrip(
        "/raw", "std_msgs/msg/ByteMultiArray", msg, msgpack_payload=True
    )
    assert "k" in flat.schema
    assert list(out.data) == list(msg.data)
